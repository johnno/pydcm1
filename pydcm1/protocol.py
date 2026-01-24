import asyncio
import logging
import re
import time
from asyncio import Task
from typing import Any, Callable, Optional

from pydcm1.listener import SourceChangeListener

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
        heartbeat_time=60,
        reconnect_time=10,
        use_event_connection_for_commands=False,
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
        self._use_event_connection_for_commands = use_event_connection_for_commands
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
        # DCM1 has 8 zones and 8 line sources (hardcoded)
        self._zone_count: int = 8
        self._source_count: int = 8
        self._heartbeat_task = None
        # Command queue to serialize all outgoing commands
        self._command_queue: asyncio.Queue = asyncio.Queue()
        self._command_worker_task: Optional[Task[Any]] = None
        # Track last send time to avoid clashes between commands
        self._last_send_time: float = 0.0
        # Track last received data time to detect silent/stalled connections
        self._last_received_time: float = time.time()
        # Minimum delay between sends to avoid MCU serial port clashes (in seconds)
        self._min_send_delay: float = 0.1

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
        
        # Start command worker and heartbeat tasks
        self._command_worker_task = self._loop.create_task(self._command_worker())
        if self._enable_heartbeat:
            self._heartbeat_task = self._loop.create_task(self._heartbeat())
        
        # Query zone/source labels, volume levels, and current sources
        self.send_zone_label_query_messages()
        self.send_source_label_query_messages()
        self.send_volume_level_query_messages()
        self.send_zone_source_query_messages()

    async def _command_worker(self):
        """Worker task that processes all outgoing commands from the queue."""
        while True:
            try:
                message, use_persistent, response_validator = await self._command_queue.get()
                try:
                    # Enforce minimum delay between commands to avoid MCU serial port clashes
                    time_since_last_send = time.time() - self._last_send_time
                    if time_since_last_send < self._min_send_delay:
                        wait_time = self._min_send_delay - time_since_last_send
                        self._logger.debug(f"Waiting {wait_time:.2f}s before sending command")
                        await asyncio.sleep(wait_time)
                    
                    if use_persistent:
                        if self._transport and self._connected:
                            self._logger.debug(f"data_send persistent: {message.encode()}")
                            self._transport.write(message.encode())
                        else:
                            self._logger.warning("Cannot send on persistent connection: not connected")
                    else:
                        await self._send_ephemeral_internal(message, response_validator)
                    
                    self._last_send_time = time.time()
                except Exception as e:
                    self._logger.error(f"Error sending command: {e}")
                finally:
                    self._command_queue.task_done()
            except asyncio.CancelledError:
                self._logger.debug("Command worker cancelled")
                break

    async def _heartbeat(self):
        """Periodically query zone status (volume and source) to sync with physical panel changes."""
        while True:
            await asyncio.sleep(self._heartbeat_time)
            
            self._logger.debug(f"heartbeat - polling zone status for all {self._zone_count} zones")
            
            # Query volume and source for all zones
            # This keeps HA in sync when staff use physical volume knobs or buttons
            for zone_id in range(1, self._zone_count + 1):
                # Query volume level
                await self._command_queue.put(f"<Z{zone_id}.MU,LQ/>\r")
                # Query source
                await self._command_queue.put(f"<Z{zone_id}.MU,SQ/>\r")

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
        self._connected = False
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._command_worker_task is not None:
            self._command_worker_task.cancel()
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
        self._source_change_callback.disconnected()
        if self._reconnect:
            self._loop.create_task(self._wait_to_reconnect())
        pass

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

    def _data_send(self, message: str, response_validator: Optional[Callable[[str], bool]] = None):
        """Queue a command to be sent (either persistent or ephemeral based on config).
        
        Args:
            message: The message to send
            response_validator: Optional function that returns True if response is valid
        """
        use_persistent = self._use_event_connection_for_commands
        try:
            self._command_queue.put_nowait((message, use_persistent, response_validator))
        except asyncio.QueueFull:
            self._logger.error("Command queue is full, dropping command")

    async def _send_ephemeral_internal(self, message, response_validator: Optional[Callable[[str], bool]] = None):
        """Send a command via ephemeral connection. Called by command worker only."""
        self._logger.debug(f"Ephemeral: Opening connection for command: {message.encode()}")
        
        try:
            # Create protocol handler for this connection to receive responses
            response_received = asyncio.Event()
            response_data = {"matched": False, "messages": []}
            
            class EphemeralProtocol(asyncio.Protocol):
                def __init__(self, response_received, response_data, logger, validator):
                    self._response_received = response_received
                    self._response_data = response_data
                    self._logger = logger
                    self._validator = validator
                    self._buffer = ""
                
                def data_received(self, data):
                    self._logger.debug(f"Ephemeral: Received data: {data}")
                    for letter in data:
                        if letter != ord("\r") and letter != ord("\n"):
                            self._buffer += chr(letter)
                        if letter == ord("\n"):
                            msg = self._buffer
                            self._buffer = ""
                            self._response_data["messages"].append(msg)
                            self._logger.debug(f"Ephemeral: Parsed message: '{msg}'")
                            
                            # Validate response if validator provided
                            if self._validator:
                                if self._validator(msg):
                                    self._logger.info(f"Ephemeral: Command succeeded: '{msg}'")
                                    self._response_data["matched"] = True
                                    self._response_received.set()
                                else:
                                    self._logger.debug(f"Ephemeral: Unexpected response (not a success): '{msg}'")
                            else:
                                # No validator, can't determine success/failure
                                self._logger.debug(f"Ephemeral: Response received (no success validation configured): '{msg}'")
                                self._response_received.set()
            
            transport, protocol = await self._loop.create_connection(
                lambda: EphemeralProtocol(response_received, response_data, self._logger, response_validator),
                host=self._hostname,
                port=self._port
            )
            self._logger.debug(f"Ephemeral: Connection established")
            
            # Send the command immediately (no prompt to wait for)
            self._logger.debug(f"Ephemeral: Writing to transport: {message.encode()}")
            transport.write(message.encode())
            self._logger.debug(f"Ephemeral: Data written to transport")
            
            # Give a tiny delay to ensure write buffer is flushed before we start monitoring
            await asyncio.sleep(0.01)
            
            # Monitor for responses after sending command - exit early when received
            monitor_time = 2.0
            self._logger.debug(f"Ephemeral: Monitoring connection for up to {monitor_time}s for response")
            start_time = time.time()
            
            try:
                await asyncio.wait_for(response_received.wait(), timeout=monitor_time)
                elapsed = time.time() - start_time
                self._logger.debug(f"Ephemeral: Response received after {elapsed:.2f}s")
            except asyncio.TimeoutError:
                elapsed = time.time() - start_time
                self._logger.warning(f"Ephemeral: No response received after sending command (waited {elapsed:.2f}s)")
            
            # Log all messages received
            if response_data['messages']:
                self._logger.debug(
                    f"Ephemeral: Received {len(response_data['messages'])} message(s): {response_data['messages']}"
                )
            
            self._logger.debug(f"Ephemeral: Closing connection after {elapsed:.2f}s")
            transport.close()
            self._logger.debug(f"Ephemeral: Connection closed")
            
        except Exception as e:
            self._logger.error(f"Ephemeral: Error in ephemeral connection: {e}")

    def _data_send_persistent(self, message):
        """Queue a command to be sent via the persistent connection."""
        try:
            self._command_queue.put_nowait((message, True, None))
        except asyncio.QueueFull:
            self._logger.error("Command queue is full, dropping command")

    # noinspection DuplicatedCode
    def _process_received_packet(self, message):
        # DCM1 responses are XML-style
        # Zone source response: <z1.mu,s=7/> means zone 1 is using source 7 (0 = no source)
        zone_source_match = ZONE_SOURCE_RESPONSE.match(message)
        if zone_source_match:
            self._logger.debug(f"Zone source response received: {message}")
            zone_id = int(zone_source_match.group(1))
            source_id = int(zone_source_match.group(2))
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
            self._logger.debug(f"Volume level response received: {message}")
            zone_id = int(volume_level_match.group(1))
            level_str = volume_level_match.group(2)
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

        self._logger.debug(f"Unhandled message received: {message}")

    def _process_source_changed(self, source_id, zone_id):
        self._logger.debug(f"Source ID [{source_id}] Zone id [{zone_id}]")
        source_id_int = int(source_id)
        zone_id_int = int(zone_id)
        self._zone_to_source_map[zone_id_int] = source_id_int
        self._source_change_callback.source_changed(zone_id_int, source_id_int)

    def send_change_source(self, source_id: int, zone_id: int):
        self._logger.info(
            f"Sending Zone source change message - Zone: {zone_id} changed to source: {source_id}"
        )
        # DCM1 uses <ZX.MU,SN/> to set zone X to source N
        # Example: <Z4.MU,S1/> sets zone 4 to source 1
        # Response: <z4.mu,s=1/>
        if not (1 <= source_id <= self._source_count):
            self._logger.error(f"Invalid source_id {source_id}, must be 1-{self._source_count}")
            return
        if not (1 <= zone_id <= self._zone_count):
            self._logger.error(f"Invalid zone_id {zone_id}, must be 1-{self._zone_count}")
            return
        
        # Set the source on this zone - use persistent connection for commands
        command = f"<Z{zone_id}.MU,S{source_id}/>\r"
        self._logger.info(f"Queueing command: {command.encode()}")
        self._data_send_persistent(command)
        
        # Auto-confirm if enabled
        if self._command_confirmation:
            self._loop.create_task(self._confirm_source(zone_id, source_id))

    def send_volume_level(self, zone_id: int, level):
        """Set volume level for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
            level: Volume level - int (0-61 where 20 = -20dB, 62 for mute) or "mute"
        """
        self._logger.info(
            f"Setting volume level - Zone: {zone_id} to level: {level}"
        )
        # DCM1 uses <ZX.MU,LN/> to set zone X to level N
        # Example: <Z4.MU,L21/> sets zone 4 to -21dB
        # Level 62 is mute: <Z4.MU,L62/> mutes zone 4
        # Response: <z4.mu,l=21/> or <z4.mu,l=mute/>
        if not (1 <= zone_id <= self._zone_count):
            self._logger.error(f"Invalid zone_id {zone_id}, must be 1-{self._zone_count}")
            return
        
        # Validate level
        if isinstance(level, str):
            if level.lower() != "mute":
                self._logger.error(f"Invalid level string '{level}', must be 'mute' or integer 0-62")
                return
            level_str = "62"  # Mute is level 62
        elif isinstance(level, int):
            if not (0 <= level <= 62):
                self._logger.error(f"Invalid level {level}, must be 0-62 (62=mute)")
                return
            level_str = str(level)
        else:
            self._logger.error(f"Invalid level type {type(level)}, must be int or 'mute'")
            return
        
        # Set the volume level on this zone
        command = f"<Z{zone_id}.MU,L{level_str}/>\r"
        self._logger.info(f"Queueing command: {command.encode()}")
        self._data_send_persistent(command)
        
        # Auto-confirm if enabled
        if self._command_confirmation:
            expected_level = 62 if level_str == "62" else int(level_str)
            self._loop.create_task(self._confirm_volume(zone_id, expected_level))

    def send_zone_source_query_messages(self):
        self._logger.info(f"Sending status query messages for all zones")
        # DCM1 uses <ZX.MU,SQ/> to query which source is active on zone X
        # Response: <zX.mu,s=N/> where N is the source ID (0 = no source)
        for zone_id in range(1, self._zone_count + 1):
            # Use persistent connection for queries
            self._data_send_persistent(f"<Z{zone_id}.MU,SQ/>\r")

    def send_zone_label_query_messages(self):
        self._logger.info(f"Querying zone labels")
        # DCM1 uses <ZX,LQ/> to query the label/name of zone X
        # Response: <zX,lq[label]/> where [label] is the zone name
        for zone_id in range(1, self._zone_count + 1):
            self._data_send_persistent(f"<Z{zone_id},LQ/>\r")

    def send_source_label_query_messages(self):
        self._logger.info(f"Querying source labels")
        # DCM1 uses <LX,LQ/> to query the label/name of line source X
        # Response: <lX,lq[label]/> where [label] is the source name
        for source_id in range(1, self._source_count + 1):
            self._data_send_persistent(f"<L{source_id},LQ/>\r")

    def send_volume_level_query_messages(self):
        self._logger.info(f"Querying zone volume levels")
        # DCM1 uses <ZX.MU,LQ/> to query the volume level of zone X
        # Response: <zX.mu,l=N/> where N is the level (0-61) or "mute"
        for zone_id in range(1, self._zone_count + 1):
            self._data_send_persistent(f"<Z{zone_id}.MU,LQ/>\r")

    def send_line_input_enable_query_messages(self, zone_id: int):
        """Query which line inputs are enabled for a specific zone.
        
        Args:
            zone_id: Zone ID (1-8)
        """
        self._logger.info(f"Querying line input enables for zone {zone_id}")
        # DCM1 uses <ZX.LY,Q/> to query if line input Y is enabled for zone X
        # Response: <zX.lY,q=e, pri = off/> where q=e means enabled, q=d means disabled
        for line_id in range(1, self._source_count + 1):
            self._data_send_persistent(f"<Z{zone_id}.L{line_id},Q/>\r")

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

    async def _confirm_volume(self, zone_id: int, expected_level: int):
        """Query volume after setting to confirm and broadcast to all listeners.
        
        Args:
            zone_id: Zone ID (1-8)
            expected_level: Expected volume level (0-62, where 62=mute)
        """
        await asyncio.sleep(0.15)  # Wait for command to apply (>100ms delay)
        self._logger.debug(f"Confirming volume for zone {zone_id}, expected: {expected_level}")
        
        # Query the volume level - this will broadcast the response to all listeners
        self._data_send_persistent(f"<Z{zone_id}.MU,LQ/>\r")
        
        # Wait a bit for response to come back and be processed
        await asyncio.sleep(0.2)
        
        # Check if the volume matches expected
        actual_level = self.get_volume_level(zone_id)
        if actual_level is None:
            self._logger.warning(f"Volume confirmation: no response received for zone {zone_id}")
        elif actual_level == "mute" and expected_level == 62:
            self._logger.info(f"Volume confirmation: zone {zone_id} muted successfully")
        elif actual_level == "mute" and expected_level != 62:
            self._logger.warning(f"Volume mismatch: zone {zone_id} set to {expected_level}, got mute")
        elif isinstance(actual_level, int) and actual_level != expected_level:
            self._logger.warning(f"Volume mismatch: zone {zone_id} set to {expected_level}, got {actual_level}")
        else:
            self._logger.info(f"Volume confirmation: zone {zone_id} set to {actual_level} successfully")

    async def _confirm_source(self, zone_id: int, expected_source: int):
        """Query source after setting to confirm and broadcast to all listeners.
        
        Args:
            zone_id: Zone ID (1-8)
            expected_source: Expected source ID (1-8)
        """
        await asyncio.sleep(0.15)  # Wait for command to apply (>100ms delay)
        self._logger.debug(f"Confirming source for zone {zone_id}, expected: {expected_source}")
        
        # Query the source - this will broadcast the response to all listeners
        self._data_send_persistent(f"<Z{zone_id}.MU,SQ/>\r")
        
        # Wait a bit for response to come back and be processed
        await asyncio.sleep(0.2)
        
        # Check if the source matches expected
        actual_source = self.get_status_of_zone(zone_id)
        if actual_source is None:
            self._logger.warning(f"Source confirmation: no response received for zone {zone_id}")
        elif actual_source != expected_source:
            self._logger.warning(f"Source mismatch: zone {zone_id} set to {expected_source}, got {actual_source}")
        else:
            self._logger.info(f"Source confirmation: zone {zone_id} set to source {actual_source} successfully")
