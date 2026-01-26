import asyncio
import logging
import re
import time
from asyncio import PriorityQueue, Task
from typing import Any, Callable, Optional

from pydcm1.listener import SourceChangeListener

# Command priority levels (lower number = higher priority)
# Design: WRITE commands have higher priority than READ queries
# Rationale: In a control system, state-changing operations (writes) must execute
# immediately in response to user actions. Read queries (status polls, confirmation
# checks) can execute later with lower priority. This ensures:
# - User commands execute without delay
# - Confirmation queries trail behind, reading the latest state
# - Only writes are tracked for recovery on reconnection
PRIORITY_WRITE = 10   # Write commands that change device state (SET volume, SET source)
PRIORITY_READ = 20    # Read commands that query device state (all queries, confirmations)

# DCM1 uses XML-style commands and responses
# Response when a zone's source is queried with <Z1.MU,SQ/>:
# <z1.mu,s=7/> means zone 1 is using source 7 (0 means no source)
# DCM1 has 8 zones and 8 line sources (L1-L8)
ZONE_SOURCE_RESPONSE = re.compile(
    r"<z(\d+)\.mu,s=(\d+).*/>.*", re.IGNORECASE
)

# Response for system info
# <sy,vq sw=2.02 hw=1.05/>
SYSTEM_INFO = re.compile(r"<sy,vq\s+sw=([\d.]+)\s+hw=([\d.]+)/>")

# Zone label response: <z1,lqMain Bar/>
ZONE_LABEL_RESPONSE = re.compile(r"<z(\d+),lq(.*)/>")

# Source label response: <l7,lqMusic/>
SOURCE_LABEL_RESPONSE = re.compile(r"<l(\d+),lq(.*)/>", re.IGNORECASE)

# Zone volume level response: <z1.mu,l=20/> or <z1.mu,l=mute/>
ZONE_VOLUME_LEVEL_RESPONSE = re.compile(r"<z(\d+)\.mu,l=([^/]+)/>", re.IGNORECASE)

# Line input enable response: <z1.l1,q=e, pri = off/> where q=e is enabled, q=d is disabled
LINE_INPUT_ENABLE_RESPONSE = re.compile(r"<z(\d+)\.l(\d+),q=([ed]),", re.IGNORECASE)

# Group label response: <g1,lqMainBar+Snug/>
GROUP_LABEL_RESPONSE = re.compile(r"<g(\d+),lq(.*)/>", re.IGNORECASE)

# Group enable status response: <g1,q=1,3d/> or <g1,q=empty/> or <g1,q=2d/>
# Format: q=<zone_list><d|e> where d=disabled, e=enabled
# "empty" means no zones assigned and disabled
# "1,3d" means zones 1 and 3 are assigned but group is disabled
# "1,3" or "1,3e" means zones 1 and 3 are assigned and group is enabled
GROUP_ENABLE_RESPONSE = re.compile(r"<g(\d+),q=([^/]+)/>", re.IGNORECASE)

# Group line input enable response: <g1.l1,q=e, pri = off/> where q=e is enabled, q=d is disabled
GROUP_LINE_INPUT_ENABLE_RESPONSE = re.compile(r"<g(\d+)\.l(\d+),q=([ed]),", re.IGNORECASE)

# Group volume level response: <g1.mu,l=20/> or <g1.mu,l=mute/>
GROUP_VOLUME_LEVEL_RESPONSE = re.compile(r"<g(\d+)\.mu,l=([^/]+)/>", re.IGNORECASE)
# Group source response: <g1.mu,s=7/> where 7 is the source ID
GROUP_SOURCE_RESPONSE = re.compile(r"<g(\d+)\.mu,s=(\d+)/>", re.IGNORECASE)

class MixerProtocol(asyncio.Protocol):
    _received_message: str
    _heartbeat_task: Optional[Task[Any]]
    _connected: bool
    _zone_to_source_map: dict[int, int]
    _source_change_callback: SourceChangeListener

    def __init__(
        self,
        hostname,
        port,
        callback: SourceChangeListener,
        heartbeat_time=10,
        reconnect_time=10,
        enable_heartbeat=True,
        command_confirmation=True,
    ):
        self._logger = logging.getLogger(__name__)
        self._heartbeat_time = heartbeat_time
        self._reconnect_time = reconnect_time
        self._hostname = hostname
        self._port = port
        self._source_change_callback = callback
        self._loop = asyncio.get_event_loop()
        self._enable_heartbeat = enable_heartbeat
        self._command_confirmation = command_confirmation

        self._connected = False
        self._reconnect = True
        self._transport = None
        self.peer_name = None
        self._received_message = ""
        self._zone_to_source_map = {}
        self._zone_to_volume_map = {}  # Maps zone_id to volume level (int or "mute")
        self._zone_line_inputs_map = {}  # Maps zone_id to dict of line_id: enabled_bool
        self._group_to_source_map = {}  # Maps group_id to source_id
        self._group_line_inputs_map = {}  # Maps group_id to dict of line_id: enabled_bool
        self._group_volume_map = {}  # Maps group_id to volume level (int or "mute")
        # Track pending heartbeat queries to prevent duplicate polling
        # Maps query command string -> True if already queued, prevents redundant reads
        self._pending_heartbeat_queries = set()
        # DCM1 has 8 zones and 8 line sources (hardcoded)
        self._zone_count: int = 8
        self._source_count: int = 8
        self._heartbeat_task = None
        # Priority queue to serialize commands (user commands jump ahead of heartbeat)
        self._command_queue: PriorityQueue = PriorityQueue()
        self._command_counter: int = 0  # Counter for FIFO order within same priority
        self._command_worker_task: Optional[Task[Any]] = None
        self._connection_watchdog_task: Optional[Task[Any]] = None  # Monitor connection health
        # Track last send time to avoid clashes between commands
        self._last_send_time: float = 0.0
        # Track last received data time to detect silent/stalled connections
        self._last_received_time: float = time.time()
        # Minimum delay between sends to avoid MCU serial port clashes (in seconds)
        self._min_send_delay: float = 0.1
        # Volume command debouncing: maps zone_id -> (pending_level, debounce_task)
        # This coalesces rapid volume changes into a single final command
        self._zone_volume_debounce: dict[int, tuple[Any, Task[Any]]] = {}
        # Group volume debouncing: maps group_id -> (pending_level, debounce_task)
        self._group_volume_debounce: dict[int, tuple[Any, Task[Any]]] = {}
        self._volume_debounce_delay: float = 0.5  # Wait 500ms after last request before sending
        # Track confirmation tasks to cancel them when new requests arrive
        self._zone_volume_confirm_task: dict[int, Task[Any]] = {}
        self._group_volume_confirm_task: dict[int, Task[Any]] = {}
        # Source command debouncing: maps zone_id -> (pending_source_id, debounce_task)
        self._zone_source_debounce: dict[int, tuple[int, Task[Any]]] = {}
        # Group source debouncing: maps group_id -> (pending_source_id, debounce_task)
        self._group_source_debounce: dict[int, tuple[int, Task[Any]]] = {}
        # Track source confirmation tasks to cancel them when new requests arrive
        self._zone_source_confirm_task: dict[int, Task[Any]] = {}
        self._group_source_confirm_task: dict[int, Task[Any]] = {}
        # Track user commands that have been sent but not yet confirmed (inflight)
        # Format: counter -> (priority, message)
        # Used for recovery on reconnection - only user commands, not heartbeat queries
        self._inflight_sends: dict[int, tuple[int, str]] = {}
        # If connection is down for longer than this, clear inflight sends on reconnect
        # because users may have used the front panel and our commands are now stale
        self._clear_queue_after_reconnection_delay_seconds: float = 60.0
        # Track when connection was lost to detect long downtime
        self._connection_lost_time: Optional[float] = None
        # Retry inflight commands on confirmation failures due to shared serial line interference
        self._retry_on_confirmation_timeout: bool = True  # Retry if no response from device
        self._retry_on_confirmation_mismatch: bool = True  # TODO: Retry if response has wrong value
        # Track retry attempts per command. Retry quota is PER-CONNECTION:
        # - While connected: limited to _max_retries to prevent infinite loops on device misbehavior
        # - On reconnection: quota is reset (fresh connection, potentially different device state)
        # This design treats timeout/mismatch during a connection as evidence of device issues,
        # but connection failure itself as transport-level (not device-level), so reconnection
        # gets a fresh quota. A command that failed once then reconnected is fair to retry fully again.
        self._inflight_retry_count: dict[int, int] = {}  # Track retry attempts per command
        self._max_retries: int = 1  # Max retry attempts per command

    async def async_connect(self):
        transport, protocol = await self._loop.create_connection(
            lambda: self, host=self._hostname, port=self._port
        )

    def close(self):
        self._reconnect = False
        if self._transport:
            self._transport.close()

    def connection_made(self, transport):
        """Method from asyncio.Protocol"""
        self._connected = True
        self._transport = transport
        self.peer_name = transport.get_extra_info("peername")
        self._logger.info(f"Connection Made: {self.peer_name}")
        self._logger.info("Requesting current status")
        self._source_change_callback.connected()
        
        # Cancel any existing tasks before creating new ones
        if self._command_worker_task is not None and not self._command_worker_task.done():
            self._command_worker_task.cancel()
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        
        # Check if connection was down for a long time
        # If so, inflight commands are likely stale (user may have used front panel)
        should_queue_inflight = True
        if self._connection_lost_time is not None:
            downtime = time.time() - self._connection_lost_time
            if downtime > self._clear_queue_after_reconnection_delay_seconds:
                self._logger.warning(
                    f"Connection was down for {downtime:.1f}s (threshold: {self._clear_queue_after_reconnection_delay_seconds}s), "
                    f"clearing {len(self._inflight_sends)} stale inflight sends and debounce/confirmation tasks"
                )
                # Clear inflight sends tracking
                self._inflight_sends.clear()
                
                # Cancel and clear all mid-flight debounce tasks (both volume and source, both zones and groups)
                for task in self._zone_volume_debounce.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._zone_volume_debounce.clear()
                
                for task in self._group_volume_debounce.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._group_volume_debounce.clear()
                
                for task in self._zone_source_debounce.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._zone_source_debounce.clear()
                
                for task in self._group_source_debounce.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._group_source_debounce.clear()
                
                # Cancel and clear all mid-flight confirmation tasks (both volume and source, both zones and groups)
                for task in self._zone_volume_confirm_task.values():
                    if task and not task.done():
                        task.cancel()
                self._zone_volume_confirm_task.clear()
                
                for task in self._group_volume_confirm_task.values():
                    if task and not task.done():
                        task.cancel()
                self._group_volume_confirm_task.clear()
                
                for task in self._zone_source_confirm_task.values():
                    if task and not task.done():
                        task.cancel()
                self._zone_source_confirm_task.clear()
                
                for task in self._group_source_confirm_task.values():
                    if task and not task.done():
                        task.cancel()
                self._group_source_confirm_task.clear()
                
                should_queue_inflight = False
            self._connection_lost_time = None
            
            # Clear pending heartbeat queries since the device won't respond to stale requests
            self._pending_heartbeat_queries.clear()
        
        # Re-queue any inflight user commands that were sent but not confirmed before disconnect
        if should_queue_inflight and self._inflight_sends:
            self._logger.warning(f"Re-queueing {len(self._inflight_sends)} inflight user commands after reconnection")
            # Clear retry counts for fresh quota on new connection. Rationale: if a command timed out
            # before disconnect, that timeout may have been due to the bad connection about to fail,
            # not device misbehavior. The reconnection represents a fresh transport with potentially
            # different device state, so it's fair to grant full retry quota. Commands that failed
            # once on connection A and reconnected to connection B are treated fresh.
            self._inflight_retry_count.clear()
            # Sort by counter to maintain FIFO order within inflight sends
            for counter in sorted(self._inflight_sends.keys()):
                priority, message = self._inflight_sends[counter]
                self._logger.info(f"Re-queueing pending command #{counter}: {message.strip()}")
                try:
                    self._command_queue.put_nowait((priority, counter, (message, None)))
                except asyncio.QueueFull:
                    self._logger.error(f"Queue full, could not re-queue pending command #{counter}")
        
        # Start command worker and heartbeat tasks
        self._command_worker_task = self._loop.create_task(self._command_worker())
        if self._enable_heartbeat:
            self._heartbeat_task = self._loop.create_task(self._heartbeat())
        
        # Start connection watchdog to detect silent disconnections
        self._connection_watchdog_task = self._loop.create_task(self._connection_watchdog())
        
        # Query zone/source labels, volume levels, and current sources
        self.send_zone_label_query_messages()
        self.send_source_label_query_messages()
        self.send_volume_level_query_messages()
        self.send_zone_source_query_messages()

    async def _command_worker(self):
        """Worker task that processes all outgoing commands from the priority queue."""
        while True:
            try:
                # Priority queue returns (priority, counter, (message, response_validator))
                priority, counter, (message, response_validator) = await self._command_queue.get()
                self._logger.info(f"[WORKER] Processing command #{counter} (priority={priority}): {message.strip()}")
                try:
                    # Track write commands for recovery on reconnection BEFORE sending
                    # This ensures commands are tracked even if send fails due to disconnection
                    if priority == PRIORITY_WRITE:
                        self._inflight_sends[counter] = (priority, message)
                        self._logger.info(f"[WORKER] Tracking inflight send: Command #{counter}")
                        # Remove any older inflight commands for the same zone/group to avoid resending superseded commands
                        self._debounce_inflight_sends(counter, message)
                    
                    # Enforce minimum delay between commands to avoid MCU serial port clashes
                    time_since_last_send = time.time() - self._last_send_time
                    if time_since_last_send < self._min_send_delay:
                        wait_time = self._min_send_delay - time_since_last_send
                        self._logger.debug(f"Waiting {wait_time:.2f}s before sending command")
                        await asyncio.sleep(wait_time)
                    
                    if self._transport and self._connected:
                        self._logger.info(f"SEND: Command #{counter} (priority={priority}): {message.encode()}")
                        self._transport.write(message.encode())
                        self._logger.info(f"SEND: Command #{counter} written to transport successfully")
                    else:
                        self._logger.error(f"SEND FAILED: Command #{counter} - not connected (transport={self._transport is not None}, connected={self._connected})")
                        # Connection is definitely down - trigger immediate reconnection
                        # Don't wait for watchdog or connection_lost() callback
                        if self._connected:  # Only if we haven't already marked it as disconnected
                            self._handle_connection_broken()
                    
                    self._last_send_time = time.time()
                except Exception as e:
                    self._logger.error(f"Error sending command: {e}", exc_info=True)
                finally:
                    self._command_queue.task_done()
            except asyncio.CancelledError:
                self._logger.debug("Command worker cancelled")
                break
            except Exception as e:
                self._logger.error(f"Unexpected error in command worker loop: {e}", exc_info=True)
                break  # Exit worker on unexpected error

    async def _heartbeat(self):
        """Periodically query zone and group status (volume and source) to sync with physical panel changes.
        
        Deduplicates queries: if a query is already pending in the queue, skip it to avoid
        redundant polling. Since protocol responses are unsolicited status messages, one 
        query answers all waiting consumers—there's no temporal significance to duplicate queries.
        """
        while True:
            await asyncio.sleep(self._heartbeat_time)
            
            self._logger.debug(f"heartbeat - polling status for {self._zone_count} zones and 4 groups")
            
            # Query volume and source for all zones
            # This keeps HA in sync when staff use physical volume knobs or buttons
            # Use low priority so user commands can jump ahead
            for zone_id in range(1, self._zone_count + 1):
                # Query volume level with deduplication
                volume_query = f"<Z{zone_id}.MU,LQ/>\r"
                if volume_query not in self._pending_heartbeat_queries:
                    self._command_counter += 1
                    self._pending_heartbeat_queries.add(volume_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_counter, (volume_query, None)))
                
                # Query source with deduplication
                source_query = f"<Z{zone_id}.MU,SQ/>\r"
                if source_query not in self._pending_heartbeat_queries:
                    self._command_counter += 1
                    self._pending_heartbeat_queries.add(source_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_counter, (source_query, None)))
            
            # Query volume and source for all groups (4 groups on DCM1)
            # This keeps groups in sync with physical panel changes
            for group_id in range(1, 5):
                # Query group volume level with deduplication
                group_volume_query = f"<G{group_id}.MU,LQ/>\r"
                if group_volume_query not in self._pending_heartbeat_queries:
                    self._command_counter += 1
                    self._pending_heartbeat_queries.add(group_volume_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_counter, (group_volume_query, None)))
                
                # Query group source with deduplication
                group_source_query = f"<G{group_id}.MU,SQ/>\r"
                if group_source_query not in self._pending_heartbeat_queries:
                    self._command_counter += 1
                    self._pending_heartbeat_queries.add(group_source_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_counter, (group_source_query, None)))

    async def _connection_watchdog(self):
        """Monitor connection health and trigger reconnection if connection dies silently.
        
        asyncio's connection_lost() callback is not always reliable for detecting all
        connection breaks. This watchdog detects if no data has been received for a
        long time and manually triggers reconnection.
        """
        idle_timeout = 30.0  # Consider connection dead if no data for 30 seconds
        check_interval = 5.0  # Check every 5 seconds
        
        while self._reconnect:
            try:
                await asyncio.sleep(check_interval)
                
                if self._connected:
                    # Check if connection appears to be silent (no data received)
                    time_since_last_data = time.time() - self._last_received_time
                    
                    if time_since_last_data > idle_timeout:
                        # Connection appears to be dead - the device isn't responding
                        # Manually trigger reconnection since connection_lost() may not be called
                        # NOTE: This is a FALLBACK detection. The send failure handler should have
                        # detected disconnection immediately on first failed command (~milliseconds),
                        # and heartbeat queries should have detected it within ~10 seconds.
                        # If watchdog is triggering, primary detection mechanisms failed or are disabled.
                        self._logger.error(
                            f"[WATCHDOG FALLBACK] Connection appears dead: no data received for {time_since_last_data:.1f}s "
                            f"(send failure handler or heartbeat should have detected this earlier), reconnecting..."
                        )
                        self._connected = False
                        if self._transport:
                            self._transport.close()
                        # Schedule reconnection
                        await self._wait_to_reconnect()
                        return  # Exit watchdog, it will be restarted on next connection
            except asyncio.CancelledError:
                self._logger.debug("Connection watchdog cancelled")
                break
            except Exception as e:
                self._logger.error(f"Error in connection watchdog: {e}")

    async def _wait_to_reconnect(self):
        """Attempt to reconnect after connection loss."""
        while not self._connected and self._reconnect:
            await asyncio.sleep(self._reconnect_time)
            try:
                await self.async_connect()
            except Exception as e:
                self._logger.warning(f"Reconnect attempt failed: {e}")

    def connection_lost(self, exc):
        """Method from asyncio.Protocol"""
        self._handle_connection_broken()

    def _handle_connection_broken(self):
        """Handle connection loss - called either by asyncio callback or by send failure detection.
        
        This centralizes disconnection logic so it triggers immediately on detection,
        not delayed by timeouts or unreliable callbacks.
        """
        self._connected = False
        # Record when the connection was lost (for detecting long downtimes)
        self._connection_lost_time = time.time()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._command_worker_task is not None:
            self._command_worker_task.cancel()
        # Cancel confirmation tasks so they don't burn retry budget while offline
        for task in self._zone_volume_confirm_task.values():
            if task and not task.done():
                task.cancel()
        for task in self._group_volume_confirm_task.values():
            if task and not task.done():
                task.cancel()
        for task in self._zone_source_confirm_task.values():
            if task and not task.done():
                task.cancel()
        for task in self._group_source_confirm_task.values():
            if task and not task.done():
                task.cancel()
        if hasattr(self, '_connection_watchdog_task') and self._connection_watchdog_task is not None:
            self._connection_watchdog_task.cancel()
        disconnected_message = f"Disconnected from {self._hostname}"
        if self._reconnect:
            disconnected_message = (
                disconnected_message
                + " will try to reconnect in {self._reconnect_time} seconds"
            )
            self._logger.error(disconnected_message)
        else:
            disconnected_message = disconnected_message + " not reconnecting"
            # Only info in here as close has been called.
            self._logger.info(disconnected_message)
        
        # Notify callback, but don't let callback exceptions prevent reconnection
        try:
            self._source_change_callback.disconnected()
        except Exception as e:
            self._logger.error(f"Exception in disconnected() callback: {e}")
        
        # Always attempt reconnection if enabled, even if callback failed
        if self._reconnect:
            self._loop.create_task(self._wait_to_reconnect())

    def data_received(self, data):
        """Method from asyncio.Protocol"""
        self._logger.debug(f"data_received client: {data}")
        self._last_received_time = time.time()

        # DCM1 sometimes sends multiple complete responses in one packet
        # Each response is of the format <.../>
        # We need to split them by /> and process each one
        decoded = data.decode('ascii', errors='ignore')
        
        # Split by /> to separate multiple responses, but keep the />
        messages = []
        current = ""
        for i, char in enumerate(decoded):
            current += char
            if char == '>' and i > 0 and decoded[i-1] == '/':
                # Found end of a message
                message = current.strip()
                if message:
                    messages.append(message)
                current = ""
        
        # Process each message
        for message in messages:
            if message:
                self._logger.debug(f"Whole message: {message}")
                self._process_received_packet(message)

    def _data_send_persistent(self, message, priority: int = PRIORITY_WRITE):
        """Queue a command to be sent via the persistent connection.
        
        Args:
            message: The message to send
            priority: Command priority (lower = higher priority, default=write commands)
        """
        # Safety: make sure the command worker is alive; if it died we silently stop sending
        if self._command_worker_task is None or self._command_worker_task.done():
            self._logger.warning("Command worker was not running; restarting it")
            self._command_worker_task = self._loop.create_task(self._command_worker())
        try:
            self._command_counter += 1
            self._logger.info(f"QUEUE: Adding command #{self._command_counter} (priority={priority}): {message.strip()}")
            self._command_queue.put_nowait((priority, self._command_counter, (message, None)))
            self._logger.info(f"QUEUE: Command #{self._command_counter} queued successfully, queue size ~= {self._command_queue.qsize()}")
        except asyncio.QueueFull:
            self._logger.error("Command queue is full, dropping command")

    # noinspection DuplicatedCode
    def _process_received_packet(self, message):
        # DCM1 responses are XML-style
        # Zone source response: <z1.mu,s=7/> means zone 1 is using source 7 (0 = no source)
        zone_source_match = ZONE_SOURCE_RESPONSE.match(message)
        if zone_source_match:
            self._logger.info(f"RECV: Zone source response: {message}")
            zone_id = int(zone_source_match.group(1))
            source_id = int(zone_source_match.group(2))
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<Z{zone_id}.MU,SQ/>\r")
            # Source ID 0 means no source is active, don't process it
            if source_id > 0:
                self._process_source_changed(source_id, zone_id)
            else:
                self._logger.debug(f"Zone {zone_id} has no active source")
            return

        # Zone label response: <z1,lqMain Bar/>
        zone_label_match = ZONE_LABEL_RESPONSE.match(message)
        if zone_label_match:
            self._logger.debug(f"Zone label response received: {message}")
            zone_id = int(zone_label_match.group(1))
            label = zone_label_match.group(2)
            self._source_change_callback.zone_label_changed(zone_id, label)
            return

        # Source label response: <l7,lqMusic/>
        source_label_match = SOURCE_LABEL_RESPONSE.match(message)
        if source_label_match:
            self._logger.debug(f"Source label response received: {message}")
            source_id = int(source_label_match.group(1))
            label = source_label_match.group(2)
            self._source_change_callback.source_label_changed(source_id, label)
            return

        # Zone volume level response: <z1.mu,l=20/> or <z1.mu,l=mute/>
        volume_level_match = ZONE_VOLUME_LEVEL_RESPONSE.match(message)
        if volume_level_match:
            self._logger.info(f"RECV: Volume level response: {message}")
            zone_id = int(volume_level_match.group(1))
            level_str = volume_level_match.group(2)
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<Z{zone_id}.MU,LQ/>\r")
            # Ignore heartbeat/old responses while a volume command is still debouncing
            # This prevents stale reads from clobbering pending UI changes before we send
            if zone_id in self._zone_volume_debounce:
                self._logger.debug(
                    "Ignoring volume response for zone %s while command is debounced",
                    zone_id,
                )
                return
            # Parse level - can be numeric or "mute"
            if level_str.lower() == "mute":
                level = "mute"
            else:
                try:
                    level = int(level_str)
                except ValueError:
                    self._logger.warning(f"Invalid volume level: {level_str}")
                    return
            self._zone_to_volume_map[zone_id] = level
            self._source_change_callback.volume_level_changed(zone_id, level)
            return

        # Line input enable response: <z1.l1,q=e, pri = off/>
        line_input_match = LINE_INPUT_ENABLE_RESPONSE.match(message)
        if line_input_match:
            self._logger.debug(f"Line input enable response received: {message}")
            zone_id = int(line_input_match.group(1))
            line_id = int(line_input_match.group(2))
            enabled = line_input_match.group(3).lower() == 'e'
            
            if zone_id not in self._zone_line_inputs_map:
                self._zone_line_inputs_map[zone_id] = {}
            self._zone_line_inputs_map[zone_id][line_id] = enabled
            
            # Check if we've received all 8 line inputs for this zone
            if len(self._zone_line_inputs_map[zone_id]) == 8:
                # Notify listener with complete set
                self._source_change_callback.line_inputs_changed(
                    zone_id, self._zone_line_inputs_map[zone_id].copy()
                )
            return

        # System info response
        system_info_match = SYSTEM_INFO.match(message)
        if system_info_match:
            self._logger.debug(f"System info received: {message}")
            # Could store firmware/hardware versions if needed
            return

        # Group label response: <g1,lqMainBar+Snug/>
        group_label_match = GROUP_LABEL_RESPONSE.match(message)
        if group_label_match:
            self._logger.debug(f"Group label response received: {message}")
            group_id = int(group_label_match.group(1))
            label = group_label_match.group(2)
            self._source_change_callback.group_label_changed(group_id, label)
            return

        # Group enable status response: <g1,q=1,3d/> or <g1,q=empty/>
        group_enable_match = GROUP_ENABLE_RESPONSE.match(message)
        if group_enable_match:
            self._logger.debug(f"Group enable status response received: {message}")
            group_id = int(group_enable_match.group(1))
            status_str = group_enable_match.group(2).strip()
            # Parse status - "empty" means disabled, zone list can end with 'd' or 'e' or neither
            enabled = False
            zones = []
            if status_str.lower() != "empty":
                # Check if ends with 'd' or 'e'
                if status_str.endswith('d'):
                    enabled = False
                    zone_list = status_str[:-1]  # Remove 'd'
                elif status_str.endswith('e'):
                    enabled = True
                    zone_list = status_str[:-1]  # Remove 'e'
                else:
                    # No explicit enable/disable marker, assume enabled if has zones
                    enabled = True
                    zone_list = status_str
                
                # Parse zone list (e.g., "1,3" -> [1, 3])
                if zone_list:
                    try:
                        zones = [int(z.strip()) for z in zone_list.split(',') if z.strip()]
                    except ValueError:
                        self._logger.warning(f"Invalid zone list in group status: {status_str}")
            
            self._source_change_callback.group_status_changed(group_id, enabled, zones)
            return

        # Group line input enable response: <g1.l1,q=e, pri = off/>
        group_line_input_match = GROUP_LINE_INPUT_ENABLE_RESPONSE.match(message)
        if group_line_input_match:
            self._logger.debug(f"Group line input enable response received: {message}")
            group_id = int(group_line_input_match.group(1))
            line_id = int(group_line_input_match.group(2))
            enabled = group_line_input_match.group(3).lower() == 'e'
            
            if group_id not in self._group_line_inputs_map:
                self._group_line_inputs_map[group_id] = {}
            self._group_line_inputs_map[group_id][line_id] = enabled
            
            # Check if we've received all 8 line inputs for this group
            if len(self._group_line_inputs_map[group_id]) == 8:
                # Notify listener with complete set
                self._source_change_callback.group_line_inputs_changed(
                    group_id, self._group_line_inputs_map[group_id].copy()
                )
            return

        # Group volume level response
        match = GROUP_VOLUME_LEVEL_RESPONSE.match(message)
        if match:
            group_id = int(match.group(1))
            level_str = match.group(2)
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<G{group_id}.MU,LQ/>\r")
            # Ignore heartbeat/old responses while a volume command is still debouncing
            if group_id in self._group_volume_debounce:
                self._logger.debug(
                    "Ignoring group volume response for group %s while command is debounced",
                    group_id,
                )
                return
            
            if level_str.lower() == "mute":
                level = "mute"
            else:
                level = int(level_str)
            
            self._group_volume_map[group_id] = level
            self._logger.info(f"Group {group_id} volume: {level}")
            self._source_change_callback.group_volume_changed(group_id, level)
            return
        
        # Group source response: <g1.mu,s=7/>
        group_source_match = GROUP_SOURCE_RESPONSE.match(message)
        if group_source_match:
            self._logger.debug(f"Group source response received: {message}")
            group_id = int(group_source_match.group(1))
            source_id = int(group_source_match.group(2))
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<G{group_id}.MU,SQ/>\r")
            self._group_to_source_map[group_id] = source_id
            self._logger.info(f"Group {group_id} source: {source_id}")
            # Notify callback
            self._source_change_callback.group_source_changed(group_id, source_id)
            return

        self._logger.debug(f"Unhandled message received: {message}")

    def _process_source_changed(self, source_id, zone_id):
        self._logger.debug(f"Source ID [{source_id}] Zone id [{zone_id}]")
        source_id_int = int(source_id)
        zone_id_int = int(zone_id)
        self._zone_to_source_map[zone_id_int] = source_id_int
        self._source_change_callback.source_changed(zone_id_int, source_id_int)

    def send_change_source(self, source_id: int, zone_id: int):
        """Set source for a zone with debounce to prevent command collisions.
        
        Rapid consecutive source changes are coalesced: only the final command is sent.
        
        Args:
            zone_id: Zone ID (1-8)
            source_id: Source ID (1-8)
        """
        self._logger.info(
            f"Source request - Zone: {zone_id} to source: {source_id} (debounced)"
        )
        # Validate inputs early
        if not (1 <= source_id <= self._source_count):
            self._logger.error(f"Invalid source_id {source_id}, must be 1-{self._source_count}")
            return
        if not (1 <= zone_id <= self._zone_count):
            self._logger.error(f"Invalid zone_id {zone_id}, must be 1-{self._zone_count}")
            return
        
        # Cancel any pending debounce timer for this zone
        if zone_id in self._zone_source_debounce:
            old_source, old_task = self._zone_source_debounce[zone_id]
            if old_task and not old_task.done():
                old_task.cancel()
                self._logger.debug(f"Cancelled pending source command for zone {zone_id} (was set to {old_source})")
        
        # Cancel any pending confirmation task for this zone (prevents race conditions)
        if zone_id in self._zone_source_confirm_task:
            old_confirm_task = self._zone_source_confirm_task[zone_id]
            if old_confirm_task and not old_confirm_task.done():
                old_confirm_task.cancel()
                self._logger.debug(f"Cancelled pending source confirmation for zone {zone_id}")
        
        # Create a new debounce task that will send the command after a delay
        debounce_task = self._loop.create_task(
            self._debounce_source_command(zone_id, source_id)
        )
        self._zone_source_debounce[zone_id] = (source_id, debounce_task)

    def send_volume_level(self, zone_id: int, level):
        """Set volume level for a zone with debounce to prevent command collisions.
        
        Rapid consecutive volume commands are coalesced: only the final command is sent.
        This prevents the issue where two volume changes queued back-to-back cause
        the second command to override the first before it completes.
        
        Args:
            zone_id: Zone ID (1-8)
            level: Volume level - int (0-61 where 20 = -20dB, 62 for mute) or "mute"
        """
        # Validate inputs early
        if not (1 <= zone_id <= self._zone_count):
            self._logger.error(f"Invalid zone_id {zone_id}, must be 1-{self._zone_count}")
            return
        
        if isinstance(level, str):
            if level.lower() != "mute":
                self._logger.error(f"Invalid level string '{level}', must be 'mute' or integer 0-62")
                return
            level_str = "62"  # Mute is level 62
            expected_level = 62
        elif isinstance(level, int):
            if not (0 <= level <= 62):
                self._logger.error(f"Invalid level {level}, must be 0-62 (62=mute)")
                return
            level_str = str(level)
            expected_level = level
        else:
            self._logger.error(f"Invalid level type {type(level)}, must be int or 'mute'")
            return
        
        self._logger.info(f"Volume request - Zone: {zone_id} to level: {level} (debounced)")
        
        # Cancel any pending debounce timer for this zone
        if zone_id in self._zone_volume_debounce:
            old_level, old_task = self._zone_volume_debounce[zone_id]
            if old_task and not old_task.done():
                old_task.cancel()
                self._logger.debug(f"Cancelled pending volume command for zone {zone_id} (was set to {old_level})")
        
        # Cancel any pending confirmation task for this zone (prevents race conditions)
        if zone_id in self._zone_volume_confirm_task:
            old_confirm_task = self._zone_volume_confirm_task[zone_id]
            if old_confirm_task and not old_confirm_task.done():
                old_confirm_task.cancel()
                self._logger.debug(f"Cancelled pending volume confirmation for zone {zone_id}")
        
        # Create a new debounce task that will send the command after a delay
        debounce_task = self._loop.create_task(
            self._debounce_volume_command(zone_id, level_str, expected_level)
        )
        self._zone_volume_debounce[zone_id] = (level, debounce_task)

    async def _debounce_volume_command(self, zone_id: int, level_str: str, expected_level: int):
        """Wait for debounce delay, then send volume command if not superseded.
        
        Args:
            zone_id: Zone ID (1-8)
            level_str: Level as string (e.g. "47" or "62")
            expected_level: Expected level after command (for confirmation)
        """
        try:
            # Wait for debounce window to pass (no more rapid requests)
            await asyncio.sleep(self._volume_debounce_delay)
            
            # Check if this task was cancelled (superseded by newer request)
            if zone_id in self._zone_volume_debounce:
                _, current_task = self._zone_volume_debounce[zone_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Volume command for zone {zone_id} level {level_str} was superseded")
                    return
            
            # Send the actual command
            command = f"<Z{zone_id}.MU,L{level_str}/>\r"
            self._logger.info(f"Queueing debounced volume command: {command.encode()}")
            self._data_send_persistent(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_volume(zone_id, expected_level))
                self._zone_volume_confirm_task[zone_id] = confirm_task
            
            # Clean up debounce tracker
            if zone_id in self._zone_volume_debounce:
                del self._zone_volume_debounce[zone_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for zone {zone_id} was cancelled")
            if zone_id in self._zone_volume_debounce:
                del self._zone_volume_debounce[zone_id]

    async def _debounce_group_volume_command(self, group_id: int, level_str: str, expected_level: int):
        """Wait for debounce delay, then send group volume command if not superseded.
        
        Args:
            group_id: Group ID (1-4)
            level_str: Level as string (e.g. "47" or "62")
            expected_level: Expected level after command (for confirmation)
        """
        try:
            # Wait for debounce window to pass (no more rapid requests)
            await asyncio.sleep(self._volume_debounce_delay)
            
            # Check if this task was cancelled (superseded by newer request)
            if group_id in self._group_volume_debounce:
                _, current_task = self._group_volume_debounce[group_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Volume command for group {group_id} level {level_str} was superseded")
                    return
            
            # Send the actual command
            command = f"<G{group_id}.MU,L{level_str}/>\r"
            self._logger.info(f"Queueing debounced volume command: {command.encode()}")
            self._data_send_persistent(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_group_volume(group_id, expected_level))
                self._group_volume_confirm_task[group_id] = confirm_task
            
            # Clean up debounce tracker
            if group_id in self._group_volume_debounce:
                del self._group_volume_debounce[group_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for group {group_id} was cancelled")
            if group_id in self._group_volume_debounce:
                del self._group_volume_debounce[group_id]

    def send_group_source(self, source_id: int, group_id: int):
        """Set a group to use a specific source with debounce to prevent command collisions.
        
        Rapid consecutive source changes are coalesced: only the final command is sent.
        
        Args:
            source_id: Source ID (1-8)
            group_id: Group ID (1-4)
        """
        self._logger.info(
            f"Source request - Group: {group_id} to source: {source_id} (debounced)"
        )
        # Validate inputs early
        if not (1 <= source_id <= self._source_count):
            self._logger.error(f"Invalid source_id {source_id}, must be 1-{self._source_count}")
            return
        if not (1 <= group_id <= 4):  # DCM1 has 4 groups
            self._logger.error(f"Invalid group_id {group_id}, must be 1-4")
            return
        
        # Cancel any pending debounce timer for this group
        if group_id in self._group_source_debounce:
            old_source, old_task = self._group_source_debounce[group_id]
            if old_task and not old_task.done():
                old_task.cancel()
                self._logger.debug(f"Cancelled pending source command for group {group_id} (was set to {old_source})")
        
        # Cancel any pending confirmation task for this group (prevents race conditions)
        if group_id in self._group_source_confirm_task:
            old_confirm_task = self._group_source_confirm_task[group_id]
            if old_confirm_task and not old_confirm_task.done():
                old_confirm_task.cancel()
                self._logger.debug(f"Cancelled pending source confirmation for group {group_id}")
        
        # Create a new debounce task that will send the command after a delay
        debounce_task = self._loop.create_task(
            self._debounce_group_source_command(group_id, source_id)
        )
        self._group_source_debounce[group_id] = (source_id, debounce_task)

    def send_group_volume_level(self, group_id: int, level):
        """Set volume level for a group with debounce to prevent command collisions.
        
        Rapid consecutive volume commands are coalesced: only the final command is sent.
        This prevents the issue where two volume changes queued back-to-back cause
        the second command to override the first before it completes.
        
        Args:
            group_id: Group ID (1-4)
            level: Volume level - int (0-61 where 20 = -20dB, 62 for mute) or "mute"
        """
        # Validate inputs early
        if not (1 <= group_id <= 4):  # DCM1 has 4 groups
            self._logger.error(f"Invalid group_id {group_id}, must be 1-4")
            return
        
        if isinstance(level, str):
            if level.lower() != "mute":
                self._logger.error(f"Invalid level string '{level}', must be 'mute' or integer 0-62")
                return
            level_str = "62"  # Mute is level 62
            expected_level = 62
        elif isinstance(level, int):
            if not (0 <= level <= 62):
                self._logger.error(f"Invalid level {level}, must be 0-62 (62=mute)")
                return
            level_str = str(level)
            expected_level = level
        else:
            self._logger.error(f"Invalid level type {type(level)}, must be int or 'mute'")
            return
        
        self._logger.info(f"Volume request - Group: {group_id} to level: {level} (debounced)")
        
        # Cancel any pending debounce timer for this group
        if group_id in self._group_volume_debounce:
            old_level, old_task = self._group_volume_debounce[group_id]
            if old_task and not old_task.done():
                old_task.cancel()
                self._logger.debug(f"Cancelled pending volume command for group {group_id} (was set to {old_level})")
        
        # Cancel any pending confirmation task for this group (prevents race conditions)
        if group_id in self._group_volume_confirm_task:
            old_confirm_task = self._group_volume_confirm_task[group_id]
            if old_confirm_task and not old_confirm_task.done():
                old_confirm_task.cancel()
                self._logger.debug(f"Cancelled pending volume confirmation for group {group_id}")
        
        # Create a new debounce task that will send the command after a delay
        debounce_task = self._loop.create_task(
            self._debounce_group_volume_command(group_id, level_str, expected_level)
        )
        self._group_volume_debounce[group_id] = (level, debounce_task)

    async def _debounce_source_command(self, zone_id: int, source_id: int):
        """Wait for debounce delay, then send source command if not superseded.
        
        Args:
            zone_id: Zone ID (1-8)
            source_id: Source ID (1-8)
        """
        try:
            # Wait for debounce window to pass (no more rapid requests)
            await asyncio.sleep(self._volume_debounce_delay)
            
            # Check if this task was cancelled (superseded by newer request)
            if zone_id in self._zone_source_debounce:
                _, current_task = self._zone_source_debounce[zone_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Source command for zone {zone_id} to source {source_id} was superseded")
                    return
            
            # Send the actual command
            command = f"<Z{zone_id}.MU,S{source_id}/>\r"
            self._logger.info(f"Queueing debounced source command: {command.encode()}")
            self._data_send_persistent(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_source(zone_id, source_id))
                self._zone_source_confirm_task[zone_id] = confirm_task
            
            # Clean up debounce tracker
            if zone_id in self._zone_source_debounce:
                del self._zone_source_debounce[zone_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for zone {zone_id} source change was cancelled")
            if zone_id in self._zone_source_debounce:
                del self._zone_source_debounce[zone_id]

    async def _debounce_group_source_command(self, group_id: int, source_id: int):
        """Wait for debounce delay, then send group source command if not superseded.
        
        Args:
            group_id: Group ID (1-4)
            source_id: Source ID (1-8)
        """
        try:
            # Wait for debounce window to pass (no more rapid requests)
            await asyncio.sleep(self._volume_debounce_delay)
            
            # Check if this task was cancelled (superseded by newer request)
            if group_id in self._group_source_debounce:
                _, current_task = self._group_source_debounce[group_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Source command for group {group_id} to source {source_id} was superseded")
                    return
            
            # Send the actual command
            command = f"<G{group_id}.MU,S{source_id}/>\r"
            self._logger.info(f"Queueing debounced source command: {command.encode()}")
            self._data_send_persistent(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_group_source(group_id, source_id))
                self._group_source_confirm_task[group_id] = confirm_task
            
            # Clean up debounce tracker
            if group_id in self._group_source_debounce:
                del self._group_source_debounce[group_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for group {group_id} source change was cancelled")
            if group_id in self._group_source_debounce:
                del self._group_source_debounce[group_id]

    def send_zone_source_query_messages(self):
        self._logger.info(f"Sending status query messages for all zones")
        # DCM1 uses <ZX.MU,SQ/> to query which source is active on zone X
        # Response: <zX.mu,s=N/> where N is the source ID (0 = no source)
        for zone_id in range(1, self._zone_count + 1):
            # Use persistent connection for queries
            self._data_send_persistent(f"<Z{zone_id}.MU,SQ/>\r", PRIORITY_READ)

    def send_zone_label_query_messages(self):
        self._logger.info(f"Querying zone labels")
        # DCM1 uses <ZX,LQ/> to query the label/name of zone X
        # Response: <zX,lq[label]/> where [label] is the zone name
        for zone_id in range(1, self._zone_count + 1):
            self._data_send_persistent(f"<Z{zone_id},LQ/>\r", PRIORITY_READ)

    def send_source_label_query_messages(self):
        self._logger.info(f"Querying source labels")
        # DCM1 uses <LX,LQ/> to query the label/name of line source X
        # Response: <lX,lq[label]/> where [label] is the source name
        for source_id in range(1, self._source_count + 1):
            self._data_send_persistent(f"<L{source_id},LQ/>\r", PRIORITY_READ)

    def send_volume_level_query_messages(self):
        self._logger.info(f"Querying zone volume levels")
        # DCM1 uses <ZX.MU,LQ/> to query the volume level of zone X
        # Response: <zX.mu,l=N/> where N is the level (0-61) or "mute"
        for zone_id in range(1, self._zone_count + 1):
            self._data_send_persistent(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)

    def send_line_input_enable_query_messages(self, zone_id: int):
        """Query which line inputs are enabled for a specific zone.
        
        Args:
            zone_id: Zone ID (1-8)
        """
        self._logger.info(f"Querying line input enables for zone {zone_id}")
        # DCM1 uses <ZX.LY,Q/> to query if line input Y is enabled for zone X
        # Response: <zX.lY,q=e, pri = off/> where q=e means enabled, q=d means disabled
        for line_id in range(1, self._source_count + 1):
            self._data_send_persistent(f"<Z{zone_id}.L{line_id},Q/>\r", PRIORITY_READ)

    def send_group_line_input_enable_query_messages(self, group_id: int):
        """Query which line inputs are enabled for a specific group.
        
        Args:
            group_id: Group ID (1-4)
        """
        self._logger.info(f"Querying line input enables for group {group_id}")
        # DCM1 uses <GX.LY,Q/> to query if line input Y is enabled for group X
        # Response: <gX.lY,q=e, pri = off/> where q=e means enabled, q=d means disabled
        for line_id in range(1, self._source_count + 1):
            self._data_send_persistent(f"<G{group_id}.L{line_id},Q/>\r", PRIORITY_READ)
    def send_group_volume_query_messages(self, group_id: int):
        """Send queries for a group's volume level."""
        self._logger.info(f"Querying volume level for group {group_id}")
        self._data_send_persistent(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
    def send_all_group_queries(self):
        """Query labels, status, volume, source, and line inputs for all 4 groups.
        
        Sends 12 commands per group (label + status + volume + source + 8 line inputs) = 48 total commands.
        """
        self._logger.info("Querying all group labels, statuses, volumes, sources, and line inputs")
        for group_id in range(1, 5):  # DCM1 has 4 groups (G1-G4)
            # Query group label: <G1,LQ/>
            self._data_send_persistent(f"<G{group_id},LQ/>\r", PRIORITY_READ)
            # Query group status: <G1,Q/>
            self._data_send_persistent(f"<G{group_id},Q/>\r", PRIORITY_READ)
            # Query group volume: <G1.MU,LQ/>
            self._data_send_persistent(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
            # Query group source: <G1.MU,SQ/>
            self._data_send_persistent(f"<G{group_id}.MU,SQ/>\r", PRIORITY_READ)
            # Query group line inputs
            for line_id in range(1, self._source_count + 1):
                self._data_send_persistent(f"<G{group_id}.L{line_id},Q/>\r", PRIORITY_READ)

    def get_status_of_zone(self, zone_id: int) -> Optional[int]:
        return self._zone_to_source_map.get(zone_id, None)

    def get_volume_level(self, zone_id: int):
        """Get volume level for a zone. Returns int (0-61) or 'mute'."""
        return self._zone_to_volume_map.get(zone_id, None)

    def get_enabled_line_inputs(self, zone_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a zone.
        
        Returns:
            Dictionary mapping line input ID (1-8) to enabled status (True/False)
        """
        return self._zone_line_inputs_map.get(zone_id, {}).copy()

    def get_enabled_group_line_inputs(self, group_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a group.
        
        Returns:
            Dictionary mapping line input ID (1-8) to enabled status (True/False)
        """
        return self._group_line_inputs_map.get(group_id, {}).copy()

    def get_group_volume_level(self, group_id: int):
        """Get the volume level for a group.
        
        Returns:
            Volume level (int 0-61) or "mute" or None if not known
        """
        return self._group_volume_map.get(group_id)
    
    def get_group_source(self, group_id: int) -> Optional[int]:
        """Get the current source ID for a group.
        
        Returns:
            Source ID (1-8) or None if not known
        """
        return self._group_to_source_map.get(group_id, None)

    def get_status_of_all_zones(self) -> list[tuple[int, Optional[int]]]:
        return_list: list[tuple[int, int | None]] = []
        for zone_id in self._zone_to_source_map:
            source_id = self._zone_to_source_map.get(zone_id, None)
            return_list.append((zone_id, source_id))
        return return_list

    def send_volume_up(self, zone_id: int):
        """Send volume up command to zone."""
        self._logger.info(f"Sending volume up to zone {zone_id}")
        # TODO: Implement DCM1 volume control
        # Likely something like <Z1.MU,LU/> or increment the level value
        self._logger.warning("Volume control not yet implemented for DCM1")

    def send_volume_down(self, zone_id: int):
        """Send volume down command to zone."""
        self._logger.info(f"Sending volume down to zone {zone_id}")
        # TODO: Implement DCM1 volume control
        self._logger.warning("Volume control not yet implemented for DCM1")

    def send_mute(self, zone_id: int):
        """Send mute toggle command to zone."""
        self._logger.info(f"Sending mute toggle to zone {zone_id}")
        # TODO: Implement DCM1 mute control
        # Based on log: <Z1.MU,MQ/> queries mute status
        # Might be <Z1.MU,M/> to mute?
        self._logger.warning("Mute control not yet implemented for DCM1")

    def _clear_inflight_send(self, counter: int):
        """Clear an inflight send from tracking after successful confirmation.
        
        Args:
            counter: Command counter to clear from inflight sends
        """
        if counter in self._inflight_sends:
            del self._inflight_sends[counter]
            self._logger.debug(f"Cleared inflight send: Command #{counter}")
        # Clean up retry count when command is confirmed
        if counter in self._inflight_retry_count:
            del self._inflight_retry_count[counter]
    
    def _retry_inflight_send(self, counter: int) -> bool:
        """Attempt to retry an inflight send that failed confirmation.
        
        Handles shared serial line interference by re-queuing failed commands.
        Only retries if max retries not exceeded.
        
        Args:
            counter: Command counter to retry
            
        Returns:
            True if retry was attempted, False if max retries exceeded or command not found
        """
        if counter not in self._inflight_sends:
            self._logger.warning(f"Cannot retry: command #{counter} not in inflight_sends")
            return False
        # If we're offline, don't consume retry budget; reconnection logic will requeue
        if not self._connected or self._transport is None:
            self._logger.debug(f"Skip retry for command #{counter}: transport unavailable (offline)")
            return False
        
        retry_count = self._inflight_retry_count.get(counter, 0)
        if retry_count >= self._max_retries:
            self._logger.error(f"Max retries ({self._max_retries}) exceeded for command #{counter}, giving up")
            return False
        
        priority, message = self._inflight_sends[counter]
        self._logger.warning(f"Retrying failed command #{counter} (attempt {retry_count + 1}/{self._max_retries}): {message.strip()}")
        self._inflight_retry_count[counter] = retry_count + 1
        
        try:
            self._command_queue.put_nowait((priority, counter, (message, None)))
            return True
        except asyncio.QueueFull:
            self._logger.error(f"Queue full, could not retry command #{counter}")
            return False
    
    def _clear_all_inflight_sends_by_pattern(self, pattern: str):
        """Clear inflight sends matching a command pattern (e.g., volume change for zone 1).
        
        Used when a command is confirmed - removes matching inflight sends from recovery tracking.
        This assumes most recent matching command was the one that was confirmed.
        
        Args:
            pattern: Part of the command string to match (e.g., "<Z1.MU,L" for zone 1 volume)
        """
        # Find and remove the most recent inflight send matching this pattern
        to_remove = []
        for counter in sorted(self._inflight_sends.keys(), reverse=True):
            _, message = self._inflight_sends[counter]
            if pattern in message:
                to_remove.append(counter)
                self._logger.debug(f"Clearing confirmed inflight send: Command #{counter} matching {pattern}")
                break  # Only clear the most recent one (most likely to be the confirmed command)
        
        for counter in to_remove:
            del self._inflight_sends[counter]

    def _debounce_inflight_sends(self, counter: int, message: str):
        """Remove older inflight commands for the same zone/group AND command type.
        
        Keeps latest command for each (zone, command_type) or (group, command_type) pair.
        Volume and source changes are independent: sending Vol 47 then Vol 42 to zone 1
        keeps only #2, but sending Vol 42 then Source 5 to zone 1 keeps both.
        
        Args:
            counter: Command counter being added to inflight sends
            message: Command message (e.g., "<Z1.MU,L42/>\r" or "<Z1.MU,S5/>\r")
        """
        import re
        # Extract zone/group and command type
        # Format: <Z1.MU,L42/> (volume) or <Z1.MU,S5/> (source) or <G1.MU,L.../> or <G1.MU,S.../>
        zone_vol_match = re.search(r'<Z(\d+)\.MU,L', message)
        zone_src_match = re.search(r'<Z(\d+)\.MU,S', message)
        group_vol_match = re.search(r'<G(\d+)\.MU,L', message)
        group_src_match = re.search(r'<G(\d+)\.MU,S', message)
        
        if zone_vol_match:
            zone_id = zone_vol_match.group(1)
            pattern = f"<Z{zone_id}.MU,L"  # Only volume commands for this zone
        elif zone_src_match:
            zone_id = zone_src_match.group(1)
            pattern = f"<Z{zone_id}.MU,S"  # Only source commands for this zone
        elif group_vol_match:
            group_id = group_vol_match.group(1)
            pattern = f"<G{group_id}.MU,L"  # Only volume commands for this group
        elif group_src_match:
            group_id = group_src_match.group(1)
            pattern = f"<G{group_id}.MU,S"  # Only source commands for this group
        else:
            return  # Unknown command format, don't debounce
        
        # Remove all older inflight sends matching the EXACT zone/group AND command type
        to_remove = []
        for old_counter in sorted(self._inflight_sends.keys()):
            if old_counter >= counter:
                continue  # Skip this command and newer ones
            _, old_message = self._inflight_sends[old_counter]
            if pattern in old_message:
                # Same zone/group AND same command type (both volume or both source)
                to_remove.append(old_counter)
                self._logger.debug(f"Removing superseded inflight send: Command #{old_counter} (newer #{counter} replaces it for {pattern})")
        
        for old_counter in to_remove:
            del self._inflight_sends[old_counter]

    async def _confirm_volume(self, zone_id: int, expected_level: int):
        """Query volume after setting to confirm and broadcast to all listeners.
        
        Args:
            zone_id: Zone ID (1-8)
            expected_level: Expected volume level (0-62, where 62=mute)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between sends is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming volume for zone {zone_id}, expected: {expected_level}")
        
        # Query the volume level - this will broadcast the response to all listeners
        self._data_send_persistent(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command counter for potential retry
        # Find the matching inflight command for this zone volume change
        matching_counter = None
        for counter in sorted(self._inflight_sends.keys(), reverse=True):
            _, message = self._inflight_sends[counter]
            if f"<Z{zone_id}.MU,L" in message:
                matching_counter = counter
                break
        
        # Check if the volume matches expected
        actual_level = self.get_volume_level(zone_id)
        if actual_level is None:
            # No response - timeout
            self._logger.warning(f"Volume confirmation: no response received for zone {zone_id}")
            if self._retry_on_confirmation_timeout and matching_counter is not None:
                self._retry_inflight_send(matching_counter)
            return

        # Handle mute cases first
        if actual_level == "mute":
            if expected_level == 62:
                self._logger.info(f"Volume confirmation: zone {zone_id} muted successfully")
                self._clear_all_inflight_sends_by_pattern(f"<Z{zone_id}.MU,L")
                return
            # Retry once if we expected a number but read mute (device can lag)
            self._logger.debug(
                f"Volume mismatch (got mute), retrying query once: zone {zone_id} expected {expected_level}"
            )
            self._data_send_persistent(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_volume_level(zone_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: zone {zone_id} set to {retry_level} after retry")
                self._clear_all_inflight_sends_by_pattern(f"<Z{zone_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: zone {zone_id} set to {expected_level}, got {retry_level if retry_level is not None else 'no response'}"
                )
                # TODO: Implement retry_on_confirmation_mismatch - re-send the command if flag is enabled
            return

        # Numeric levels
        if isinstance(actual_level, int) and actual_level != expected_level:
            # Retry once before giving up
            self._logger.debug(
                f"Volume mismatch (first read {actual_level}), retrying query once: zone {zone_id} expected {expected_level}"
            )
            self._data_send_persistent(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_volume_level(zone_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: zone {zone_id} set to {retry_level} after retry")
                self._clear_all_inflight_sends_by_pattern(f"<Z{zone_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: zone {zone_id} set to {expected_level}, got {retry_level if retry_level is not None else actual_level}"
                )
                # TODO: Implement retry_on_confirmation_mismatch - re-send the command if flag is enabled
        else:
            self._logger.info(f"Volume confirmation: zone {zone_id} set to {actual_level} successfully")
            self._clear_all_inflight_sends_by_pattern(f"<Z{zone_id}.MU,L")

    async def _confirm_source(self, zone_id: int, expected_source: int):
        """Query source after setting to confirm and broadcast to all listeners.
        
        Args:
            zone_id: Zone ID (1-8)
            expected_source: Expected source ID (1-8)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between sends is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming source for zone {zone_id}, expected: {expected_source}")
        
        # Query the source - this will broadcast the response to all listeners
        self._data_send_persistent(f"<Z{zone_id}.MU,SQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command counter for potential retry
        matching_counter = None
        for counter in sorted(self._inflight_sends.keys(), reverse=True):
            _, message = self._inflight_sends[counter]
            if f"<Z{zone_id}.MU,S" in message:
                matching_counter = counter
                break
        
        # Check if the source matches expected
        actual_source = self.get_status_of_zone(zone_id)
        if actual_source is None:
            # No response - timeout
            self._logger.warning(f"Source confirmation: no response received for zone {zone_id}")
            if self._retry_on_confirmation_timeout and matching_counter is not None:
                self._retry_inflight_send(matching_counter)
        elif actual_source != expected_source:
            # Value mismatch
            self._logger.warning(f"Source mismatch: zone {zone_id} set to {expected_source}, got {actual_source}")
            # TODO: Implement retry_on_confirmation_mismatch - re-send the command if flag is enabled
        else:
            self._logger.info(f"Source confirmation: zone {zone_id} set to source {actual_source} successfully")
            self._clear_all_inflight_sends_by_pattern(f"<Z{zone_id}.MU,S")

    async def _confirm_group_volume(self, group_id: int, expected_level: int):
        """Query group volume after setting to confirm and broadcast to all listeners.
        
        Args:
            group_id: Group ID (1-4)
            expected_level: Expected volume level (0-62, where 62=mute)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between sends is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming volume for group {group_id}, expected: {expected_level}")
        
        # Query the volume level - this will broadcast the response to all listeners
        self._data_send_persistent(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command counter for potential retry
        matching_counter = None
        for counter in sorted(self._inflight_sends.keys(), reverse=True):
            _, message = self._inflight_sends[counter]
            if f"<G{group_id}.MU,L" in message:
                matching_counter = counter
                break
        
        # Check if the volume matches expected
        actual_level = self.get_group_volume_level(group_id)
        if actual_level is None:
            # No response - timeout
            self._logger.warning(f"Volume confirmation: no response received for group {group_id}")
            if self._retry_on_confirmation_timeout and matching_counter is not None:
                self._retry_inflight_send(matching_counter)
            return

        # Handle mute cases first
        if actual_level == "mute":
            if expected_level == 62:
                self._logger.info(f"Volume confirmation: group {group_id} muted successfully")
                self._clear_all_inflight_sends_by_pattern(f"<G{group_id}.MU,L")
                return
            # Retry once if we expected a number but read mute (device can lag)
            self._logger.debug(
                f"Volume mismatch (got mute), retrying query once: group {group_id} expected {expected_level}"
            )
            self._data_send_persistent(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_group_volume_level(group_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: group {group_id} set to {retry_level} after retry")
                self._clear_all_inflight_sends_by_pattern(f"<G{group_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: group {group_id} set to {expected_level}, got {retry_level if retry_level is not None else 'no response'}"
                )
                # TODO: Implement retry_on_confirmation_mismatch - re-send the command if flag is enabled
            return

        # Numeric levels
        if isinstance(actual_level, int) and actual_level != expected_level:
            # Retry once before giving up
            self._logger.debug(
                f"Volume mismatch (first read {actual_level}), retrying query once: group {group_id} expected {expected_level}"
            )
            self._data_send_persistent(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_group_volume_level(group_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: group {group_id} set to {retry_level} after retry")
                self._clear_all_inflight_sends_by_pattern(f"<G{group_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: group {group_id} set to {expected_level}, got {retry_level if retry_level is not None else actual_level}"
                )
                # TODO: Implement retry_on_confirmation_mismatch - re-send the command if flag is enabled
        else:
            self._logger.info(f"Volume confirmation: group {group_id} set to {actual_level} successfully")
            self._clear_all_inflight_sends_by_pattern(f"<G{group_id}.MU,L")

    async def _confirm_group_source(self, group_id: int, expected_source: int):
        """Query group source after setting to confirm and broadcast to all listeners.
        
        Args:
            group_id: Group ID (1-4)
            expected_source: Expected source ID (1-8)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between sends is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming source for group {group_id}, expected: {expected_source}")
        
        # Query the source - this will broadcast the response to all listeners
        self._data_send_persistent(f"<G{group_id}.MU,SQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command counter for potential retry
        matching_counter = None
        for counter in sorted(self._inflight_sends.keys(), reverse=True):
            _, message = self._inflight_sends[counter]
            if f"<G{group_id}.MU,S" in message:
                matching_counter = counter
                break
        
        # Check if the source matches expected
        actual_source = self.get_group_source(group_id)
        if actual_source is None:
            # No response - timeout
            self._logger.warning(f"Source confirmation: no response received for group {group_id}")
            if self._retry_on_confirmation_timeout and matching_counter is not None:
                self._retry_inflight_send(matching_counter)
        elif actual_source != expected_source:
            # Value mismatch
            self._logger.warning(f"Source mismatch: group {group_id} set to {expected_source}, got {actual_source}")
            # TODO: Implement retry_on_confirmation_mismatch - re-send the command if flag is enabled
        else:
            self._logger.info(f"Source confirmation: group {group_id} set to source {actual_source} successfully")
            self._clear_all_inflight_sends_by_pattern(f"<G{group_id}.MU,S")
