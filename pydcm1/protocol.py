import asyncio
import logging
import re
import time
from asyncio import PriorityQueue, Task
from typing import Any, Callable, Optional

from pydcm1.listener import MixerResponseListener

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

# Response for system info
# <sy,vq sw=2.02 hw=1.05/>
SYSTEM_INFO = re.compile(r"<sy,vq\s+sw=([\d.]+)\s+hw=([\d.]+)/>")

# Source label response: <l7,lqMusic/>
SOURCE_LABEL_RESPONSE = re.compile(r"<l(\d+),lq(.*)/>", re.IGNORECASE)

# Zone label response: <z1,lqMain Bar/>
ZONE_LABEL_RESPONSE = re.compile(r"<z(\d+),lq(.*)/>")

# Line input enable response: <z1.l1,q=e, pri = off/> where q=e is enabled, q=d is disabled
ZONE_LINE_INPUT_ENABLE_RESPONSE = re.compile(r"<z(\d+)\.l(\d+),q=([ed]),", re.IGNORECASE)

# Response when a zone's source is queried with <Z1.MU,SQ/>:
# <z1.mu,s=7/> means zone 1 is using source 7 (0 means no source)
# DCM1 has 8 zones and 8 line sources (L1-L8)
ZONE_SOURCE_RESPONSE = re.compile(r"<z(\d+)\.mu,s=(\d+).*/>.*", re.IGNORECASE)

# Zone volume level response: <z1.mu,l=20/> or <z1.mu,l=mute/>
ZONE_VOLUME_LEVEL_RESPONSE = re.compile(r"<z(\d+)\.mu,l=([^/]+)/>", re.IGNORECASE)

# Group enable status response: <g1,q=1,3d/> or <g1,q=empty/> or <g1,q=2d/>
# Format: q=<zone_list><d|e> where d=disabled, e=enabled
# "empty" means no zones assigned and disabled
# "1,3d" means zones 1 and 3 are assigned but group is disabled
# "1,3" or "1,3e" means zones 1 and 3 are assigned and group is enabled
GROUP_ENABLE_RESPONSE = re.compile(r"<g(\d+),q=([^/]+)/>", re.IGNORECASE)

# Group label response: <g1,lqMainBar+Snug/>
GROUP_LABEL_RESPONSE = re.compile(r"<g(\d+),lq(.*)/>", re.IGNORECASE)

# Group line input enable response: <g1.l1,q=e, pri = off/> where q=e is enabled, q=d is disabled
GROUP_LINE_INPUT_ENABLE_RESPONSE = re.compile(r"<g(\d+)\.l(\d+),q=([ed]),", re.IGNORECASE)

# Group source response: <g1.mu,s=7/> where 7 is the source ID
GROUP_SOURCE_RESPONSE = re.compile(r"<g(\d+)\.mu,s=(\d+)/>", re.IGNORECASE)

# Group volume level response: <g1.mu,l=20/> or <g1.mu,l=mute/>
GROUP_VOLUME_LEVEL_RESPONSE = re.compile(r"<g(\d+)\.mu,l=([^/]+)/>", re.IGNORECASE)

class MixerProtocol(asyncio.Protocol):
    _received_message: str
    _heartbeat_task: Optional[Task[Any]]
    _connected: bool
    _source_change_callback: MixerResponseListener

    def __init__(
        self,
        hostname,
        port,
        callback: MixerResponseListener,
        heartbeat_time=10,
        reconnect_time=10,
        enable_heartbeat=True,
        command_confirmation=True,
    ):
        # DCM1 has 8 zones, 8 line sources, and 4 groups (hardcoded)
        self._zone_count: int = 8
        self._source_count: int = 8
        self._group_count: int = 4

        # Retry inflight commands on confirmation failures due to shared serial line interference
        self._retry_on_confirmation_timeout: bool = True  # Retry if no response from device
        self._retry_on_confirmation_mismatch: bool = True  # TODO: Retry if response has wrong value
        # If connection is down for longer than this, clear inflight commands on reconnect
        # because users may have used the front panel and our commands are now stale
        self._clear_queue_after_reconnection_delay_seconds: float = 60.0

        self._max_retries: int = 1  # Max retry attempts per command

        # Minimum delay between commands to avoid MCU serial port clashes (in seconds)
        self._min_send_delay_seconds: float = 0.1

        self._volume_debounce_delay: float = 0.5  # Wait 500ms after last request before sending

        self._hostname = hostname
        self._port = port
        self._enable_heartbeat = enable_heartbeat
        self._command_confirmation = command_confirmation
        self._source_change_callback = callback

        self._logger = logging.getLogger(__name__)
        self._heartbeat_time = heartbeat_time
        self._reconnect_time = reconnect_time
        self._loop = asyncio.get_event_loop()

        self._connected = False
        self._reconnect = True
        self._transport = None
        self.peer_name = None
        self._received_message = ""
        self._zone_line_inputs_map = {}  # Maps zone_id to dict of line_id: enabled_bool
        self._zone_to_source_map = {}  # Maps zone_id to source_id
        self._zone_to_volume_map = {}  # Maps zone_id to volume level (int or "mute")
        self._group_line_inputs_map = {}  # Maps group_id to dict of line_id: enabled_bool
        self._group_to_source_map = {}  # Maps group_id to source_id
        self._group_to_volume_map = {}  # Maps group_id to volume level (int or "mute")
        # Track pending heartbeat queries to prevent duplicate polling
        # Maps query command string -> True if already queued, prevents redundant reads
        self._pending_heartbeat_queries = set()
        self._heartbeat_task = None
        # Priority queue to serialize commands (user commands jump ahead of heartbeat)
        self._command_queue: PriorityQueue = PriorityQueue()
        self._queued_commands_set: set[str] = set()  # Deduplication set for queued commands
        self._command_sequence_number: int = 0  # Sequence number for FIFO order within same priority
        self._command_worker_task: Optional[Task[Any]] = None
        self._connection_watchdog_task: Optional[Task[Any]] = None  # Monitor connection health

        # Track last send time to avoid clashes between commands
        self._last_send_timestamp: float = 0.0
        # Track last received data time to detect silent/stalled connections
        self._last_receive_timestamp: float = time.time()

        # Track when connection was lost to detect long downtime
        self._connection_lost_timestamp: Optional[float] = None

        # Source command debouncing: maps zone_id -> (pending_source_id, debounce_task)
        self._zone_source_debounce_tasks: dict[int, tuple[int, Task[Any]]] = {}
        # Volume command debouncing: maps zone_id -> (pending_level, debounce_task)
        # This coalesces rapid volume changes into a single final command
        self._zone_volume_debounce_tasks: dict[int, tuple[Any, Task[Any]]] = {}
        # Track confirmation tasks to cancel them when new requests arrive
        self._zone_source_confirm_task: dict[int, Task[Any]] = {}
        self._zone_volume_confirm_task: dict[int, Task[Any]] = {}

        # Group source debouncing: maps group_id -> (pending_source_id, debounce_task)
        self._group_source_debounce_tasks: dict[int, tuple[int, Task[Any]]] = {}
        # Group volume debouncing: maps group_id -> (pending_level, debounce_task)
        self._group_volume_debounce_tasks: dict[int, tuple[Any, Task[Any]]] = {}
        # Track source confirmation tasks to cancel them when new requests arrive
        self._group_source_confirm_task: dict[int, Task[Any]] = {}
        self._group_volume_confirm_task: dict[int, Task[Any]] = {}

        # Track user commands that have been sent but not yet confirmed (inflight)
        # Format: command_sequence_number -> (priority, message)
        # Used for recovery on reconnection - only user commands, not heartbeat queries
        self._inflight_commands: dict[int, tuple[int, str]] = {}
        # Track retry attempts per command. Retry quota is PER-CONNECTION:
        # - While connected: limited to _max_retries to prevent infinite loops on device misbehavior
        # - On reconnection: quota is reset (fresh connection, potentially different device state)
        # This design treats timeout/mismatch during a connection as evidence of device issues,
        # but connection failure itself as transport-level (not device-level), so reconnection
        # gets a fresh quota. A command that failed once then reconnected is fair to retry fully again.
        self._inflight_command_reenqueue_counts: dict[int, int] = {}  # Track retry attempts per command

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
        reenqueue_inflight_commands = True
        if self._connection_lost_timestamp is not None:
            # Clear pending heartbeat queries since the device won't respond to stale requests
            self._pending_heartbeat_queries.clear()

            downtime_duration_seconds = time.time() - self._connection_lost_timestamp
            if downtime_duration_seconds > self._clear_queue_after_reconnection_delay_seconds:
                reenqueue_inflight_commands = False

                self._logger.warning(
                    f"Connection was down for {downtime_duration_seconds:.1f}s (threshold: {self._clear_queue_after_reconnection_delay_seconds}s), "
                    f"clearing {len(self._inflight_commands)} stale inflight commands and debounce/confirmation tasks"
                )
                # Clear inflight commands tracking
                self._inflight_commands.clear()
                
                # Cancel and clear all mid-flight debounce tasks (both volume and source, both zones and groups)
                for task in self._zone_source_debounce_tasks.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._zone_source_debounce_tasks.clear()

                for task in self._zone_volume_debounce_tasks.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._zone_volume_debounce_tasks.clear()
                
                for task in self._group_source_debounce_tasks.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._group_source_debounce_tasks.clear()

                for task in self._group_volume_debounce_tasks.values():
                    _, debounce_task = task
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                self._group_volume_debounce_tasks.clear()
                                
            # Clear here in case it gets used again above.                    
            self._connection_lost_timestamp = None
        
        # Re-queue any inflight user commands that were sent but not confirmed before disconnect
        if reenqueue_inflight_commands and self._inflight_commands:
            self._logger.warning(f"Re-queueing {len(self._inflight_commands)} inflight user commands after reconnection")
            # Clear retry counts for fresh quota on new connection. Rationale: if a command timed out
            # before disconnect, that timeout may have been due to the bad connection about to fail,
            # not device misbehavior. The reconnection represents a fresh transport with potentially
            # different device state, so it's fair to grant full retry quota. Commands that failed
            # once on connection A and reconnected to connection B are treated fresh.
            self._inflight_command_reenqueue_counts.clear()
            # Sort by sequence_number to maintain FIFO order within inflight commands
            for sequence_number in sorted(self._inflight_commands.keys()):
                priority, message = self._inflight_commands[sequence_number]
                self._logger.info(f"Re-queueing pending command #{sequence_number}: {message.strip()}")
                try:
                    self._command_queue.put_nowait((priority, sequence_number, (message, None)))
                except asyncio.QueueFull:
                    self._logger.error(f"Queue full, could not re-queue pending command #{sequence_number}")
        
        # Start command worker and heartbeat tasks
        self._command_worker_task = self._loop.create_task(self._command_worker())
        if self._enable_heartbeat:
            self._heartbeat_task = self._loop.create_task(self._heartbeat())
        
        # Start connection watchdog to detect silent disconnections
        self._connection_watchdog_task = self._loop.create_task(self._connection_watchdog())
        
        # Query configuration
        self.enqueue_source_label_query_commands()

        self.enqueue_zone_label_query_commands()
        self.enqueue_zone_line_input_enable_query_commands()

        self.enqueue_group_status_query_commands()
        self.enqueue_group_label_query_commands()
        self.enqueue_group_line_input_enable_query_commands()

        # Query operational status
        self.enqueue_zone_source_query_commands()
        self.enqueue_zone_volume_level_query_commands()

        self.enqueue_group_source_query_commands()
        self.enqueue_group_volume_level_query_commands()

    async def _command_worker(self):
        """Worker task that processes all outgoing commands from the priority queue."""
        while True:
            try:
                # Priority queue returns (priority, sequence_number, (message, response_validator))
                priority, sequence_number, (message, response_validator) = await self._command_queue.get()
                msg_key = message.strip()
                self._logger.info(f"[WORKER] Processing command #{sequence_number} (priority={priority}): {msg_key}")
                try:
                    # Track write commands for recovery on reconnection BEFORE sending
                    # This ensures commands are tracked even if send fails due to disconnection
                    if priority == PRIORITY_WRITE:
                        self._inflight_commands[sequence_number] = (priority, message)
                        self._logger.info(f"[WORKER] Tracking inflight command: Command #{sequence_number}")
                        # Remove any older inflight commands for the same zone/group to avoid resending superseded commands
                        self._debounce_inflight_commands(sequence_number, message)
                    
                    # Enforce minimum delay between commands to avoid MCU serial port clashes
                    time_since_last_send = time.time() - self._last_send_timestamp
                    if time_since_last_send < self._min_send_delay_seconds:
                        wait_time = self._min_send_delay_seconds - time_since_last_send
                        self._logger.debug(f"Waiting {wait_time:.2f}s before sending command")
                        await asyncio.sleep(wait_time)
                    
                    if self._transport and self._connected:
                        self._logger.info(f"SEND: Command #{sequence_number} (priority={priority}): {message.encode()}")
                        self._transport.write(message.encode())
                        self._logger.info(f"SEND: Command #{sequence_number} written to transport successfully")
                    else:
                        self._logger.error(f"SEND FAILED: Command #{sequence_number} - not connected (transport={self._transport is not None}, connected={self._connected})")
                        # Connection is definitely down - trigger immediate reconnection
                        # Don't wait for watchdog or connection_lost() callback
                        if self._connected:  # Only if we haven't already marked it as disconnected
                            self._handle_connection_broken()
                    
                    self._last_send_timestamp = time.time()
                except Exception as e:
                    self._logger.error(f"Error sending command: {e}", exc_info=True)
                finally:
                    # Remove from deduplication set so it can be re-queued in the future
                    if msg_key in self._queued_commands_set:
                        self._queued_commands_set.remove(msg_key)
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
            self._logger.debug(f"heartbeat - polling status for {self._zone_count} zones and {self._group_count} groups")
            
            # Query source and volume for all zones
            # This keeps HA in sync when staff use physical volume knobs or buttons
            # Use low priority so user commands can jump ahead
            for zone_id in range(1, self._zone_count + 1):
                # Query source with deduplication
                zone_source_query = f"<Z{zone_id}.MU,SQ/>\r"
                if zone_source_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(zone_source_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (zone_source_query, None)))

                # Query volume level with deduplication
                zone_volume_query = f"<Z{zone_id}.MU,LQ/>\r"
                if zone_volume_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(zone_volume_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (zone_volume_query, None)))
                            
            # Query source and volume for all groups (4 groups on DCM1)
            # This keeps groups in sync with physical panel changes
            # Use low priority so user commands can jump ahead
            for group_id in range(1, self._group_count + 1):                
                # Query group source with deduplication
                group_source_query = f"<G{group_id}.MU,SQ/>\r"
                if group_source_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(group_source_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (group_source_query, None)))

                # Query group volume level with deduplication
                group_volume_query = f"<G{group_id}.MU,LQ/>\r"
                if group_volume_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(group_volume_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (group_volume_query, None)))

    async def _connection_watchdog(self):
        """Monitor connection health and trigger reconnection if connection dies silently.
        
        asyncio's connection_lost() callback is not always reliable for detecting all
        connection breaks. This watchdog detects if no data has been received for a
        long time and manually triggers reconnection.
        """
        idle_timeout_seconds = 30.0  # Consider connection dead if no data for 30 seconds
        check_interval_seconds = 5.0  # Check every 5 seconds
        
        while self._reconnect:
            try:
                await asyncio.sleep(check_interval_seconds)
                
                if self._connected:
                    # Check if connection appears to be silent (no data received)
                    elapsed_seconds_since_data_last_received = time.time() - self._last_receive_timestamp
                    
                    if elapsed_seconds_since_data_last_received > idle_timeout_seconds:
                        # Connection appears to be dead - the device isn't responding
                        # Manually trigger reconnection since connection_lost() may not be called
                        # NOTE: This is a FALLBACK detection. The send failure handler should have
                        # detected disconnection immediately on first failed command (~milliseconds),
                        # and heartbeat queries should have detected it within ~10 seconds.
                        # If watchdog is triggering, primary detection mechanisms failed or are disabled.
                        self._logger.error(
                            f"[WATCHDOG FALLBACK] Connection appears dead: no data received for {elapsed_seconds_since_data_last_received:.1f}s "
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
        self._connection_lost_timestamp = time.time()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._command_worker_task is not None:
            self._command_worker_task.cancel()
        # Cancel confirmation tasks so they don't burn retry budget while offline
        for task in self._zone_source_confirm_task.values():
            if task and not task.done():
                task.cancel()
        for task in self._zone_volume_confirm_task.values():
            if task and not task.done():
                task.cancel()
        for task in self._group_source_confirm_task.values():
            if task and not task.done():
                task.cancel()
        for task in self._group_volume_confirm_task.values():
            if task and not task.done():
                task.cancel()
        if hasattr(self, '_connection_watchdog_task') and self._connection_watchdog_task is not None:
            self._connection_watchdog_task.cancel()
        
        disconnected_message = f"Disconnected from {self._hostname}"
        if self._reconnect:
            disconnected_message = disconnected_message + f"will try to reconnect in {self._reconnect_time} seconds"
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
        self._last_receive_timestamp = time.time()

        # DCM1 sometimes sends multiple complete responses in one packet
        # Each response is of the format <.../>
        # We need to split them by /> and process each one
        decoded = data.decode('ascii', errors='ignore')
        
        # Split by /> to separate multiple responses, but keep the />
        received_messages = []
        current_message_builder = ""
        for i, char in enumerate(decoded):
            current_message_builder += char
            if char == '>' and i > 0 and decoded[i-1] == '/':
                # Found end of a message
                message = current_message_builder.strip()
                if message:
                    received_messages.append(message)
                current_message_builder = ""
        
        # Process each message
        for message in received_messages:
            if message:
                self._logger.debug(f"Whole message: {message}")
                self._process_received_message(message)

    def _enqueue_command(self, message, priority: int = PRIORITY_WRITE):
        """Queue a command to be sent via the persistent connection.
        
        Args:
            message: The message to send
            priority: Command psend_group_sourceriority (lower = higher priority, default=write commands)
        """
        # Deduplication: Only queue if not already present
        msg_key = message.strip()
        if msg_key in self._queued_commands_set:
            self._logger.info(f"QUEUE: Duplicate command not queued: {msg_key}")
            return
        self._queued_commands_set.add(msg_key)
        # Safety: make sure the command worker is alive; if it died we silently stop sending
        if self._command_worker_task is None or self._command_worker_task.done():
            self._logger.warning("Command worker was not running; restarting it")
            self._command_worker_task = self._loop.create_task(self._command_worker())
        try:
            self._command_sequence_number += 1
            self._logger.info(f"QUEUE: Adding command #{self._command_sequence_number} (priority={priority}): {msg_key}")
            self._command_queue.put_nowait((priority, self._command_sequence_number, (message, None)))
            self._logger.info(f"QUEUE: Command #{self._command_sequence_number} queued successfully, queue size ~= {self._command_queue.qsize()}")
        except asyncio.QueueFull:
            self._logger.error("Command queue is full, dropping command")

    def _process_received_message(self, message):
        # Process the most expected messages first which will
        # be responses to our polling requests.

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
                self._process_zone_source_received(source_id, zone_id)
            else:
                self._logger.debug(f"Zone {zone_id} has no active source")
            return

        # Zone volume level response: <z1.mu,l=20/> or <z1.mu,l=mute/>
        zone_volume_level_match = ZONE_VOLUME_LEVEL_RESPONSE.match(message)
        if zone_volume_level_match:
            self._logger.info(f"RECV: Volume level response: {message}")
            zone_id = int(zone_volume_level_match.group(1))
            level_str = zone_volume_level_match.group(2)
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<Z{zone_id}.MU,LQ/>\r")
            # Ignore heartbeat/old responses while a volume command is still debouncing
            # This prevents sending UI state updates that are about to be superseded by the pending command.
            # 
            # NOTE: This does NOT fully mitigate race conditions for listeners consuming these responses
            # as general events. After debounce completes (~0.5s) and the command is sent, debounce 
            # tracking is cleared, creating a ~0.8s window until confirmation completes where stale
            # heartbeat responses could still arrive and trigger UI updates. The media_player layer
            # handles this race by storing _pending_raw_volume_level and ignoring non-matching responses.
            # 
            # Edge case: Physical knob changes during the pending window (~1s total) are temporarily
            # "lost" until next heartbeat (~10s). Acceptable for single-staff operation.
            if zone_id in self._zone_volume_debounce_tasks:
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
                    self._logger.warning(f"Invalid zone volume level: {level_str}")
                    return
            self._zone_to_volume_map[zone_id] = level
            self._source_change_callback.zone_volume_level_received(zone_id, level)
            return
        
        # Group source response: <g1.mu,s=7/>
        group_source_match = GROUP_SOURCE_RESPONSE.match(message)
        if group_source_match:
            self._logger.info(f"RECV: Group source response: {message}")
            group_id = int(group_source_match.group(1))
            source_id = int(group_source_match.group(2))
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<G{group_id}.MU,SQ/>\r")
            # Source ID 0 means no source is active, don't process it
            if source_id > 0:
                self._process_group_source_received(source_id, group_id)
            else:
                self._logger.debug(f"Group {group_id} has no active source")
            return

        # Group volume level response
        group_volume_level_match = GROUP_VOLUME_LEVEL_RESPONSE.match(message)
        if group_volume_level_match:
            self._logger.info(f"RECV: Group volume level response: {message}")
            group_id = int(group_volume_level_match.group(1))
            level_str = group_volume_level_match.group(2)
            # Remove from pending heartbeat queries when response received
            self._pending_heartbeat_queries.discard(f"<G{group_id}.MU,LQ/>\r")
            # Ignore heartbeat/old responses while a volume command is still debouncing
            # This prevents sending UI state updates that are about to be superseded by the pending command.
            # 
            # NOTE: This does NOT fully mitigate race conditions for listeners consuming these responses
            # as general events. After debounce completes (~0.5s) and the command is sent, debounce 
            # tracking is cleared, creating a ~0.8s window until confirmation completes where stale
            # heartbeat responses could still arrive and trigger UI updates. The media_player layer
            # handles this race by storing _pending_raw_volume_level and ignoring non-matching responses.
            # 
            # Edge case: Physical knob changes during the pending window (~1s total) are temporarily
            # "lost" until next heartbeat (~10s). Acceptable for single-staff operation.
            if group_id in self._group_volume_debounce_tasks:
                self._logger.debug(
                    "Ignoring group volume response for group %s while command is debounced",
                    group_id,
                )
                return
            
            if level_str.lower() == "mute":
                level = "mute"
            else:
                try:
                    level = int(level_str)
                except ValueError:
                    self._logger.warning(f"Invalid group volume level: {level_str}")
                    return
            
            self._group_to_volume_map[group_id] = level
            self._logger.info(f"Group {group_id} volume: {level}")
            self._source_change_callback.group_volume_level_received(group_id, level)
            return        

        # Source label response: <l7,lqMusic/>
        source_label_match = SOURCE_LABEL_RESPONSE.match(message)
        if source_label_match:
            self._logger.debug(f"Source label response received: {message}")
            source_id = int(source_label_match.group(1))
            label = source_label_match.group(2)
            self._source_change_callback.source_label_received(source_id, label)
            return

        # Zone label response: <z1,lqMain Bar/>
        zone_label_match = ZONE_LABEL_RESPONSE.match(message)
        if zone_label_match:
            self._logger.debug(f"Zone label response received: {message}")
            zone_id = int(zone_label_match.group(1))
            label = zone_label_match.group(2)
            self._source_change_callback.zone_label_received(zone_id, label)
            return

        # Line input enable response: <z1.l1,q=e, pri = off/>
        zone_line_input_match = ZONE_LINE_INPUT_ENABLE_RESPONSE.match(message)
        if zone_line_input_match:
            self._logger.debug(f"Line input enable response received: {message}")
            zone_id = int(zone_line_input_match.group(1))
            line_id = int(zone_line_input_match.group(2))
            enabled = zone_line_input_match.group(3).lower() == 'e'
            
            if zone_id not in self._zone_line_inputs_map:
                self._zone_line_inputs_map[zone_id] = {}
            self._zone_line_inputs_map[zone_id][line_id] = enabled
            
            # Check if we've received all 8 line inputs for this zone
            if len(self._zone_line_inputs_map[zone_id]) == 8:
                # Notify listener with complete set
                self._source_change_callback.zone_line_inputs_received(
                    zone_id, self._zone_line_inputs_map[zone_id].copy()
                )
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
            
            self._source_change_callback.group_status_received(group_id, enabled, zones)
            return

        # Group label response: <g1,lqMainBar+Snug/>
        group_label_match = GROUP_LABEL_RESPONSE.match(message)
        if group_label_match:
            self._logger.debug(f"Group label response received: {message}")
            group_id = int(group_label_match.group(1))
            label = group_label_match.group(2)
            self._source_change_callback.group_label_received(group_id, label)
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
                self._source_change_callback.group_line_inputs_received(
                    group_id, self._group_line_inputs_map[group_id].copy()
                )
            return
                
        # System info response
        system_info_match = SYSTEM_INFO.match(message)
        if system_info_match:
            self._logger.debug(f"System info received: {message}")
            # Could store firmware/hardware versions if needed
            return

        self._logger.debug(f"Unhandled message received: {message}")

    def _process_zone_source_received(self, source_id, zone_id):
        self._logger.debug(f"Source ID [{source_id}] Zone id [{zone_id}]")
        source_id_int = int(source_id)
        zone_id_int = int(zone_id)
        self._zone_to_source_map[zone_id_int] = source_id_int
        self._logger.info(f"Zone {zone_id_int} source: {source_id_int}")
        self._source_change_callback.zone_source_received(zone_id_int, source_id_int)

    def _process_group_source_received(self, source_id, group_id):
        self._logger.debug(f"Source ID [{source_id}] Group id [{group_id}]")
        source_id_int = int(source_id)
        group_id_int = int(group_id)
        self._group_to_source_map[group_id_int] = source_id_int
        self._logger.info(f"Group {group_id_int} source: {source_id_int}")
        self._source_change_callback.group_source_received(group_id_int, source_id_int)

    def send_zone_source(self, source_id: int, zone_id: int):
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
        if zone_id in self._zone_source_debounce_tasks:
            old_source, old_task = self._zone_source_debounce_tasks[zone_id]
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
            self._debounce_zone_source_command(zone_id, source_id)
        )
        self._zone_source_debounce_tasks[zone_id] = (source_id, debounce_task)

    def send_zone_volume_level(self, zone_id: int, level):
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
        if zone_id in self._zone_volume_debounce_tasks:
            old_level, old_task = self._zone_volume_debounce_tasks[zone_id]
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
            self._debounce_zone_volume_command(zone_id, level_str, expected_level)
        )
        self._zone_volume_debounce_tasks[zone_id] = (level, debounce_task)

    async def _debounce_zone_volume_command(self, zone_id: int, level_str: str, expected_level: int):
        """Wait for debounce delay, then send zone volume command if not superseded.
        
        Args:
            zone_id: Zone ID (1-8)
            level_str: Level as string (e.g. "47" or "62")
            expected_level: Expected level after command (for confirmation)
        """
        try:
            # Wait for debounce window to pass (no more rapid requests)
            await asyncio.sleep(self._volume_debounce_delay)
            
            # Check if this task was cancelled (superseded by newer request)
            if zone_id in self._zone_volume_debounce_tasks:
                _, current_task = self._zone_volume_debounce_tasks[zone_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Volume command for zone {zone_id} level {level_str} was superseded")
                    return
            
            # Send the actual command
            command = f"<Z{zone_id}.MU,L{level_str}/>\r"
            self._logger.info(f"Queueing debounced volume command: {command.encode()}")
            self._enqueue_command(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_zone_volume(zone_id, expected_level))
                self._zone_volume_confirm_task[zone_id] = confirm_task
            
            # Clean up debounce tracker
            if zone_id in self._zone_volume_debounce_tasks:
                del self._zone_volume_debounce_tasks[zone_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for zone {zone_id} was cancelled")
            if zone_id in self._zone_volume_debounce_tasks:
                del self._zone_volume_debounce_tasks[zone_id]

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
            if group_id in self._group_volume_debounce_tasks:
                _, current_task = self._group_volume_debounce_tasks[group_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Volume command for group {group_id} level {level_str} was superseded")
                    return
            
            # Send the actual command
            command = f"<G{group_id}.MU,L{level_str}/>\r"
            self._logger.info(f"Queueing debounced volume command: {command.encode()}")
            self._enqueue_command(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_group_volume(group_id, expected_level))
                self._group_volume_confirm_task[group_id] = confirm_task
            
            # Clean up debounce tracker
            if group_id in self._group_volume_debounce_tasks:
                del self._group_volume_debounce_tasks[group_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for group {group_id} was cancelled")
            if group_id in self._group_volume_debounce_tasks:
                del self._group_volume_debounce_tasks[group_id]

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
        if not (1 <= group_id <= self._group_count):
            self._logger.error(f"Invalid group_id {group_id}, must be 1-{self._group_count}")
            return
        
        # Cancel any pending debounce timer for this group
        if group_id in self._group_source_debounce_tasks:
            old_source, old_task = self._group_source_debounce_tasks[group_id]
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
        self._group_source_debounce_tasks[group_id] = (source_id, debounce_task)

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
        if not (1 <= group_id <= self._group_count):
            self._logger.error(f"Invalid group_id {group_id}, must be 1-{self._group_count}")
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
        if group_id in self._group_volume_debounce_tasks:
            old_level, old_task = self._group_volume_debounce_tasks[group_id]
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
        self._group_volume_debounce_tasks[group_id] = (level, debounce_task)

    async def _debounce_zone_source_command(self, zone_id: int, source_id: int):
        """Wait for debounce delay, then send zone source command if not superseded.
        
        Args:
            zone_id: Zone ID (1-8)
            source_id: Source ID (1-8)
        """
        try:
            # Wait for debounce window to pass (no more rapid requests)
            await asyncio.sleep(self._volume_debounce_delay)
            
            # Check if this task was cancelled (superseded by newer request)
            if zone_id in self._zone_source_debounce_tasks:
                _, current_task = self._zone_source_debounce_tasks[zone_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Source command for zone {zone_id} to source {source_id} was superseded")
                    return
            
            # Send the actual command
            command = f"<Z{zone_id}.MU,S{source_id}/>\r"
            self._logger.info(f"Queueing debounced source command: {command.encode()}")
            self._enqueue_command(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_zone_source(zone_id, source_id))
                self._zone_source_confirm_task[zone_id] = confirm_task
            
            # Clean up debounce tracker
            if zone_id in self._zone_source_debounce_tasks:
                del self._zone_source_debounce_tasks[zone_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for zone {zone_id} source change was cancelled")
            if zone_id in self._zone_source_debounce_tasks:
                del self._zone_source_debounce_tasks[zone_id]

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
            if group_id in self._group_source_debounce_tasks:
                _, current_task = self._group_source_debounce_tasks[group_id]
                if current_task != asyncio.current_task():
                    # This task was superseded; don't send
                    self._logger.debug(f"Source command for group {group_id} to source {source_id} was superseded")
                    return
            
            # Send the actual command
            command = f"<G{group_id}.MU,S{source_id}/>\r"
            self._logger.info(f"Queueing debounced source command: {command.encode()}")
            self._enqueue_command(command)
            
            # Auto-confirm if enabled - track the task so it can be cancelled if needed
            if self._command_confirmation:
                confirm_task = self._loop.create_task(self._confirm_group_source(group_id, source_id))
                self._group_source_confirm_task[group_id] = confirm_task
            
            # Clean up debounce tracker
            if group_id in self._group_source_debounce_tasks:
                del self._group_source_debounce_tasks[group_id]
        except asyncio.CancelledError:
            # Task was cancelled; another request superseded it
            self._logger.debug(f"Debounce task for group {group_id} source change was cancelled")
            if group_id in self._group_source_debounce_tasks:
                del self._group_source_debounce_tasks[group_id]

    def enqueue_zone_source_query_commands(self):
        self._logger.info(f"Sending status query messages for all zones")
        # DCM1 uses <ZX.MU,SQ/> to query which source is active on zone X
        # Response: <zX.mu,s=N/> where N is the source ID (0 = no source)
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(f"<Z{zone_id}.MU,SQ/>\r", PRIORITY_READ)

    def enqueue_zone_label_query_commands(self):
        self._logger.info(f"Querying zone labels")
        # DCM1 uses <ZX,LQ/> to query the label/name of zone X
        # Response: <zX,lq[label]/> where [label] is the zone name
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(f"<Z{zone_id},LQ/>\r", PRIORITY_READ)

    def enqueue_source_label_query_commands(self):
        self._logger.info(f"Querying source labels")
        # DCM1 uses <LX,LQ/> to query the label/name of line source X
        # Response: <lX,lq[label]/> where [label] is the source name
        for source_id in range(1, self._source_count + 1):
            self._enqueue_command(f"<L{source_id},LQ/>\r", PRIORITY_READ)

    def enqueue_zone_volume_level_query_commands(self):
        self._logger.info(f"Querying zone volume levels")
        # DCM1 uses <ZX.MU,LQ/> to query the volume level of zone X
        # Response: <zX.mu,l=N/> where N is the level (0-61) or "mute"
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)

    def enqueue_zone_line_input_enable_query_commands(self):
        """Query which line inputs are enabled for all zones (1-8)."""
        self._logger.info("Querying line input enables for all zones")
        # DCM1 uses <ZX.LY,Q/> to query if line input Y is enabled for zone X
        # Response: <zX.lY,q=e, pri = off/> where q=e means enabled, q=d means disabled
        for zone_id in range(1, self._zone_count + 1):
            for line_id in range(1, self._source_count + 1):
                self._enqueue_command(f"<Z{zone_id}.L{line_id},Q/>\r", PRIORITY_READ)

    def enqueue_group_line_input_enable_query_commands(self):
        """Query which line inputs are enabled for all groups (1-4)."""
        self._logger.info("Querying line input enables for all groups")
        # DCM1 uses <GX.LY,Q/> to query if line input Y is enabled for group X
        # Response: <gX.lY,q=e, pri = off/> where q=e means enabled, q=d means disabled
        for group_id in range(1, self._group_count + 1):
            for line_id in range(1, self._source_count + 1):
                self._enqueue_command(f"<G{group_id}.L{line_id},Q/>\r", PRIORITY_READ)

    def enqueue_group_status_query_commands(self):
        """Query group status for all groups."""
        self._logger.info("Querying group status for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(f"<G{group_id},Q/>\r", PRIORITY_READ)

    def enqueue_group_label_query_commands(self):
        """Query group labels for all groups."""
        self._logger.info("Querying group labels for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(f"<G{group_id},LQ/>\r", PRIORITY_READ)

    def enqueue_group_source_query_commands(self):
        """Query group source for all groups."""
        self._logger.info("Querying group source for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(f"<G{group_id}.MU,SQ/>\r", PRIORITY_READ)

    def enqueue_group_volume_level_query_commands(self):
        """Query group volume level for all groups."""
        self._logger.info("Querying group volume levels for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)

    def get_zone_source(self, zone_id: int) -> Optional[int]:
        """Get the current source ID for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
        
        Returns:
            Source ID (1-8) or None if not known
        """
        return self._zone_to_source_map.get(zone_id, None)

    def get_zone_volume_level(self, zone_id: int):
        """Get the volume level for a zone.
        
        Returns:
            Volume level (int 0-61) or "mute" or None if not known
        """
        return self._zone_to_volume_map.get(zone_id, None)

    def get_zone_enabled_line_inputs(self, zone_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a zone.
        
        Returns:
            Dictionary mapping line input ID (1-8) to enabled status (True/False)
        """
        return self._zone_line_inputs_map.get(zone_id, {}).copy()

    def get_group_enabled_line_inputs(self, group_id: int) -> dict[int, bool]:
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
        return self._group_to_volume_map.get(group_id)
    
    def get_group_source(self, group_id: int) -> Optional[int]:
        """Get the current source ID for a group.
        
        Returns:
            Source ID (1-8) or None if not known
        """
        return self._group_to_source_map.get(group_id, None)

    def _clear_inflight_command(self, sequence_number: int):
        """Clear an inflight command from tracking after successful confirmation.
        
        Args:
            sequence_number: Command sequence number to clear from inflight commands
        """
        if sequence_number in self._inflight_commands:
            del self._inflight_commands[sequence_number]
            self._logger.debug(f"Cleared inflight command: #{sequence_number}")
        # Clean up retry count when command is confirmed
        if sequence_number in self._inflight_command_reenqueue_counts:
            del self._inflight_command_reenqueue_counts[sequence_number]
    
    def _reenqueue_inflight_command(self, sequence_number: int) -> bool:
        """Attempt to retry an inflight command that failed confirmation.
        
        Handles shared serial line interference by reenqueuing failed commands.
        Only reenqueues if max retries not exceeded.
        
        Args:
            sequence_number: Command sequence number to reenqueue
            
        Returns:
            True if reenqueue was attempted, False if max retries exceeded or command not found
        """
        if sequence_number not in self._inflight_commands:
            self._logger.warning(f"Cannot reenqueue command: #{sequence_number} not in inflight_commands")
            return False
        # If we're offline, don't consume retry budget; reconnection logic will requeue
        if not self._connected or self._transport is None:
            self._logger.debug(f"Skip reenqueue for command #{sequence_number}: transport unavailable (offline)")
            return False
        
        reenqueue_count = self._inflight_command_reenqueue_counts.get(sequence_number, 0)
        if reenqueue_count >= self._max_retries:
            self._logger.error(f"Max retries ({self._max_retries}) exceeded for command #{sequence_number}, giving up")
            return False
        
        priority, message = self._inflight_commands[sequence_number]
        self._logger.warning(f"Reenqueuing failed command #{sequence_number} (attempt {reenqueue_count + 1}/{self._max_retries}): {message.strip()}")
        self._inflight_command_reenqueue_counts[sequence_number] = reenqueue_count + 1
        
        try:
            self._command_queue.put_nowait((priority, sequence_number, (message, None)))
            return True
        except asyncio.QueueFull:
            self._logger.error(f"Queue full, could not retry command #{sequence_number}")
            return False
    
    def _clear_latest_inflight_command_by_pattern(self, pattern: str):
        """Clear the most recent inflight command matching a command pattern (e.g., volume change for zone 1).
        
        Used when a command is confirmed - removes the latest matching inflight command from recovery tracking.
        This assumes the most recent matching command was the one that was confirmed.
        
        Args:
            pattern: Part of the command string to match (e.g., "<Z1.MU,L" for zone 1 volume)
        """
        # Find and remove the most recent inflight command matching this pattern
        for sequence_number in sorted(self._inflight_commands.keys(), reverse=True):
            _, message = self._inflight_commands[sequence_number]
            if pattern in message:
                self._logger.debug(f"Clearing confirmed inflight send: Command #{sequence_number} matching {pattern}")
                del self._inflight_commands[sequence_number]
                return  # Only clear the most recent one and exit

    def _debounce_inflight_commands(self, sequence_number: int, message: str):
        """Remove older inflight commands for the same zone/group AND command type.
    
        Keeps latest command for each (zone, command_type) or (group, command_type) pair.
        Volume and source changes are independent: sending Vol 47 then Vol 42 to zone 1
        keeps only #2, but sending Vol 42 then Source 5 to zone 1 keeps both.
    
        Args:
            sequence_number: Command sequence number being added to inflight commands
            message: Command message (e.g., "<Z1.MU,L42/>\r" or "<Z1.MU,S5/>\r")
        """
        import re
        # Extract zone/group and command type
        # Format: <Z1.MU,L42/> (volume) or <Z1.MU,S5/> (source) or <G1.MU,L.../> or <G1.MU,S.../>
        zone_vol_match = re.search(r'<Z(\d+)\.MU,L', message)
        zone_src_match = re.search(r'<Z(\d+)\.MU,S', message)
        group_vol_match = re.search(r'<G(\d+)\.MU,L', message)
        group_src_match = re.search(r'<G(\d+)\.MU,S', message)
        
        if zone_src_match:
            zone_id = zone_src_match.group(1)
            pattern = f"<Z{zone_id}.MU,S"  # Only source commands for this zone
        elif zone_vol_match:
            zone_id = zone_vol_match.group(1)
            pattern = f"<Z{zone_id}.MU,L"  # Only volume commands for this zone
        elif group_src_match:
            group_id = group_src_match.group(1)
            pattern = f"<G{group_id}.MU,S"  # Only source commands for this group
        elif group_vol_match:
            group_id = group_vol_match.group(1)
            pattern = f"<G{group_id}.MU,L"  # Only volume commands for this group
        else:
            return  # Unknown command format, don't debounce
        
        # Remove all older inflight commands matching the EXACT zone/group AND command type
        to_remove = []
        for old_sequence_number in sorted(self._inflight_commands.keys()):
            if old_sequence_number >= sequence_number:
                continue  # Skip this command and newer ones
            _, old_message = self._inflight_commands[old_sequence_number]
        for old_sequence_number in to_remove:
            del self._inflight_commands[old_sequence_number]
            if pattern in old_message:
                # Same zone/group AND same command type (both volume or both source)
                to_remove.append(old_sequence_number)
                self._logger.debug(f"Removing superseded inflight command: #{old_sequence_number} (newer #{sequence_number} replaces it for {pattern})")
    
        for old_sequence_number in to_remove:
            del self._inflight_commands[old_sequence_number]

    async def _confirm_zone_volume(self, zone_id: int, expected_level: int):
        """Query volume after setting to confirm and broadcast to all listeners.
        
        Args:
            zone_id: Zone ID (1-8)
            expected_level: Expected volume level (0-62, where 62=mute)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between commands is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming volume for zone {zone_id}, expected: {expected_level}")
        
        # Query the volume level - this will broadcast the response to all listeners
        self._enqueue_command(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command sequence_number for potential retry
        # Find the matching inflight command for this zone volume change
        matching_sequence_number = None
        for sequence_number in sorted(self._inflight_commands.keys(), reverse=True):
            _, message = self._inflight_commands[sequence_number]
            if f"<Z{zone_id}.MU,L" in message:
                matching_sequence_number = sequence_number
                break
        
        # Check if the volume matches expected
        actual_level = self.get_zone_volume_level(zone_id)
        if actual_level is None:
            # No response - timeout
            self._logger.warning(f"Volume confirmation: no response received for zone {zone_id}")
            if self._retry_on_confirmation_timeout and matching_sequence_number is not None:
                self._reenqueue_inflight_command(matching_sequence_number)
            return

        # Handle mute cases first
        if actual_level == "mute":
            if expected_level == 62:
                self._logger.info(f"Volume confirmation: zone {zone_id} muted successfully")
                self._clear_latest_inflight_command_by_pattern(f"<Z{zone_id}.MU,L")
                return
            # Retry once if we expected a number but read mute (device can lag)
            self._logger.debug(
                f"Volume mismatch (got mute), retrying query once: zone {zone_id} expected {expected_level}"
            )
            self._enqueue_command(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_zone_volume_level(zone_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: zone {zone_id} set to {retry_level} after retry")
                self._clear_latest_inflight_command_by_pattern(f"<Z{zone_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: zone {zone_id} set to {expected_level}, got {retry_level if retry_level is not None else 'no response'}"
                )
                # Application layer should decide if/when to retry on confirmation mismatch
            return

        # Numeric levels
        if isinstance(actual_level, int) and actual_level != expected_level:
            # Retry once before giving up
            self._logger.debug(
                f"Volume mismatch (first read {actual_level}), retrying query once: zone {zone_id} expected {expected_level}"
            )
            self._enqueue_command(f"<Z{zone_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_zone_volume_level(zone_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: zone {zone_id} set to {retry_level} after retry")
                self._clear_latest_inflight_command_by_pattern(f"<Z{zone_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: zone {zone_id} set to {expected_level}, got {retry_level if retry_level is not None else actual_level}"
                )
                # Application layer should decide if/when to retry on confirmation mismatch
        else:
            self._logger.info(f"Volume confirmation: zone {zone_id} set to {actual_level} successfully")
            self._clear_latest_inflight_command_by_pattern(f"<Z{zone_id}.MU,L")

    async def _confirm_zone_source(self, zone_id: int, expected_source: int):
        """Query source after setting to confirm and broadcast to all listeners.
        
        Args:
            zone_id: Zone ID (1-8)
            expected_source: Expected source ID (1-8)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between commands is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming source for zone {zone_id}, expected: {expected_source}")
        
        # Query the source - this will broadcast the response to all listeners
        self._enqueue_command(f"<Z{zone_id}.MU,SQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command sequence_number for potential retry
        matching_sequence_number = None
        for sequence_number in sorted(self._inflight_commands.keys(), reverse=True):
            _, message = self._inflight_commands[sequence_number]
            if f"<Z{zone_id}.MU,S" in message:
                matching_sequence_number = sequence_number
                break
        
        # Check if the source matches expected
        actual_source = self.get_zone_source(zone_id)
        if actual_source is None:
            # No response - timeout
            self._logger.warning(f"Source confirmation: no response received for zone {zone_id}")
            if self._retry_on_confirmation_timeout and matching_sequence_number is not None:
                self._reenqueue_inflight_command(matching_sequence_number)
        elif actual_source != expected_source:
            # Value mismatch
            self._logger.warning(f"Source mismatch: zone {zone_id} set to {expected_source}, got {actual_source}")
            # Application layer should decide if/when to retry on confirmation mismatch
        else:
            self._logger.info(f"Source confirmation: zone {zone_id} set to source {actual_source} successfully")
            self._clear_latest_inflight_command_by_pattern(f"<Z{zone_id}.MU,S")

    async def _confirm_group_volume(self, group_id: int, expected_level: int):
        """Query group volume after setting to confirm and broadcast to all listeners.
        
        Args:
            group_id: Group ID (1-4)
            expected_level: Expected volume level (0-62, where 62=mute)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between commands is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming volume for group {group_id}, expected: {expected_level}")
        
        # Query the volume level - this will broadcast the response to all listeners
        self._enqueue_command(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command sequence_number for potential retry
        matching_sequence_number = None
        for sequence_number in sorted(self._inflight_commands.keys(), reverse=True):
            _, message = self._inflight_commands[sequence_number]
            if f"<G{group_id}.MU,L" in message:
                matching_sequence_number = sequence_number
                break
        
        # Check if the volume matches expected
        actual_level = self.get_group_volume_level(group_id)
        if actual_level is None:
            # No response - timeout
            self._logger.warning(f"Volume confirmation: no response received for group {group_id}")
            if self._retry_on_confirmation_timeout and matching_sequence_number is not None:
                self._reenqueue_inflight_command(matching_sequence_number)
            return

        # Handle mute cases first
        if actual_level == "mute":
            if expected_level == 62:
                self._logger.info(f"Volume confirmation: group {group_id} muted successfully")
                self._clear_latest_inflight_command_by_pattern(f"<G{group_id}.MU,L")
                return
            # Retry once if we expected a number but read mute (device can lag)
            self._logger.debug(
                f"Volume mismatch (got mute), retrying query once: group {group_id} expected {expected_level}"
            )
            self._enqueue_command(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_group_volume_level(group_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: group {group_id} set to {retry_level} after retry")
                self._clear_latest_inflight_command_by_pattern(f"<G{group_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: group {group_id} set to {expected_level}, got {retry_level if retry_level is not None else 'no response'}"
                )
                # Application layer should decide if/when to retry on confirmation mismatch
            return

        # Numeric levels
        if isinstance(actual_level, int) and actual_level != expected_level:
            # Retry once before giving up
            self._logger.debug(
                f"Volume mismatch (first read {actual_level}), retrying query once: group {group_id} expected {expected_level}"
            )
            self._enqueue_command(f"<G{group_id}.MU,LQ/>\r", PRIORITY_READ)
            await asyncio.sleep(0.4)
            retry_level = self.get_group_volume_level(group_id)
            if retry_level == expected_level:
                self._logger.info(f"Volume confirmation: group {group_id} set to {retry_level} after retry")
                self._clear_latest_inflight_command_by_pattern(f"<G{group_id}.MU,L")
            else:
                # Value mismatch after retry
                self._logger.warning(
                    f"Volume mismatch: group {group_id} set to {expected_level}, got {retry_level if retry_level is not None else actual_level}"
                )
                # Application layer should decide if/when to retry on confirmation mismatch
        else:
            self._logger.info(f"Volume confirmation: group {group_id} set to {actual_level} successfully")
            self._clear_latest_inflight_command_by_pattern(f"<G{group_id}.MU,L")

    async def _confirm_group_source(self, group_id: int, expected_source: int):
        """Query group source after setting to confirm and broadcast to all listeners.
        
        Args:
            group_id: Group ID (1-4)
            expected_source: Expected source ID (1-8)
        """
        # Wait for SET command to be sent and processed by DCM1
        # Min delay between commands is 0.1s, plus DCM1 processing time
        await asyncio.sleep(0.3)
        self._logger.debug(f"Confirming source for group {group_id}, expected: {expected_source}")
        
        # Query the source - this will broadcast the response to all listeners
        self._enqueue_command(f"<G{group_id}.MU,SQ/>\r", PRIORITY_READ)
        
        # Wait for QUERY to be sent (0.1s min delay) + network + DCM1 response + processing
        await asyncio.sleep(0.5)
        
        # Get the inflight command sequence_number for potential retry
        matching_sequence_number = None
        for sequence_number in sorted(self._inflight_commands.keys(), reverse=True):
            _, message = self._inflight_commands[sequence_number]
            if f"<G{group_id}.MU,S" in message:
                matching_sequence_number = sequence_number
                break
        
        # Check if the source matches expected
        actual_source = self.get_group_source(group_id)
        if actual_source is None:
            # No response - timeout
            self._logger.warning(f"Source confirmation: no response received for group {group_id}")
            if self._retry_on_confirmation_timeout and matching_sequence_number is not None:
                self._reenqueue_inflight_command(matching_sequence_number)
        elif actual_source != expected_source:
            # Value mismatch
            self._logger.warning(f"Source mismatch: group {group_id} set to {expected_source}, got {actual_source}")
            # Application layer should decide if/when to retry on confirmation mismatch
        else:
            self._logger.info(f"Source confirmation: group {group_id} set to source {actual_source} successfully")
            self._clear_latest_inflight_command_by_pattern(f"<G{group_id}.MU,S")
