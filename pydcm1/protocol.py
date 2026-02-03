"""Pure transport protocol layer for DCM1 mixer.

This module contains ONLY the asyncio.Protocol transport layer:
- Connection lifecycle (connection_made, connection_lost, data_received)
- Raw message parsing and regex matching
- Callbacks to listener interface when responses are received
- Simple write() method

NO queue, NO heartbeat, NO inflight tracking, NO reconnection, NO watchdog.
All that logic has been moved to DCM1Mixer."""

import asyncio
import logging
import re
from enum import Enum
from typing import Optional

from pydcm1.listener import MixerResponseListener

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

# Zone EQ response: <z1.mu,eq, t = 0, m = 0, b = 0/> (treble, mid, bass: -12 to +12)
ZONE_EQ_RESPONSE = re.compile(r"<z(\d+)\.mu,eq,\s*t\s*=\s*(-?\d+),\s*m\s*=\s*(-?\d+),\s*b\s*=\s*(-?\d+)/>", re.IGNORECASE)

# Individual EQ component responses: <z4.mu,t=0/>, <z4.mu,m=0/>, <z4.mu,b=0/>
ZONE_EQ_TREBLE_RESPONSE = re.compile(r"<z(\d+)\.mu,t=(-?\d+)/>", re.IGNORECASE)
ZONE_EQ_MID_RESPONSE = re.compile(r"<z(\d+)\.mu,m=(-?\d+)/>", re.IGNORECASE)
ZONE_EQ_BASS_RESPONSE = re.compile(r"<z(\d+)\.mu,b=(-?\d+)/>", re.IGNORECASE)

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


class OutputType(str, Enum):
    ZONE = "Z"
    GROUP = "G"


class MixerProtocol(asyncio.Protocol):
    """Pure transport protocol for DCM1 mixer.
    
    Responsibilities:
    - asyncio.Protocol lifecycle (connection_made, connection_lost, data_received)
    - Parse incoming messages using regex
    - Fire callbacks to listener when responses are received
    - Provide simple write(message) method
    
    NOT responsible for:
    - Queue management
    - Heartbeat/polling
    - Inflight command tracking
    - Reconnection logic
    - Connection watchdog
    """

    def __init__(self, listener: MixerResponseListener):
        """Initialize protocol with listener for callbacks.
        
        Args:
            listener: Listener to receive parsed response callbacks
        """
        self._listener = listener
        self._logger = logging.getLogger(__name__)
        self._transport: Optional[asyncio.Transport] = None
        self._received_message = ""
        
        # Temporary storage for collecting line inputs before notifying
        # We need all 8 line inputs before calling the listener
        self._zone_line_inputs_map = {}  # Maps zone_id to dict of line_id: enabled_bool
        self._group_line_inputs_map = {}  # Maps group_id to dict of line_id: enabled_bool

    def connection_made(self, transport):
        """Called when connection is established."""
        self._transport = transport
        peer_name = transport.get_extra_info("peername")
        self._logger.info(f"Transport connection made: {peer_name}")
        self._listener.connected()

    def connection_lost(self, exc):
        """Called when connection is lost."""
        self._logger.info(f"Transport connection lost: {exc}")
        self._transport = None
        self._listener.disconnected()

    def data_received(self, data):
        """Called when data is received from transport."""
        self._logger.debug(f"data_received: {data}")
        
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
                self._logger.debug(f"Processing message: {message}")
                self._process_received_message(message)

    def write(self, message: str):
        """Write a message to the transport.
        
        Args:
            message: Message string to send (will be encoded to bytes)
        """
        if self._transport:
            self._logger.debug(f"Writing to transport: {message.encode()}")
            self._transport.write(message.encode())
        else:
            self._logger.error("Cannot write: transport not connected")

    @staticmethod
    def command_source_pattern(output_type: OutputType, output_id: int) -> str:
        return f"<{output_type.value}{output_id}.MU,S"

    @staticmethod
    def command_volume_pattern(output_type: OutputType, output_id: int) -> str:
        return f"<{output_type.value}{output_id}.MU,L"

    @staticmethod
    def command_source_target_id(output_type: OutputType, message: str) -> Optional[int]:
        match = re.search(rf"<{output_type.value}(\d+)\.MU,S", message, re.IGNORECASE)
        return int(match.group(1)) if match else None

    @staticmethod
    def command_volume_target_id(output_type: OutputType, message: str) -> Optional[int]:
        match = re.search(rf"<{output_type.value}(\d+)\.MU,L", message, re.IGNORECASE)
        return int(match.group(1)) if match else None

    @staticmethod
    def command_set_source(output_type: OutputType, output_id: int, source_id: int) -> str:
        return f"<{output_type.value}{output_id}.MU,S{source_id}/>\r"

    @staticmethod
    def command_set_volume(output_type: OutputType, output_id: int, level_str: str) -> str:
        return f"<{output_type.value}{output_id}.MU,L{level_str}/>\r"

    @staticmethod
    def command_query_source(output_type: OutputType, output_id: int) -> str:
        return f"<{output_type.value}{output_id}.MU,SQ/>\r"

    @staticmethod
    def command_query_volume(output_type: OutputType, output_id: int) -> str:
        return f"<{output_type.value}{output_id}.MU,LQ/>\r"

    @staticmethod
    def command_query_line_input(output_type: OutputType, output_id: int, line_id: int) -> str:
        return f"<{output_type.value}{output_id}.L{line_id},Q/>\r"

    @staticmethod
    def command_query_label(output_type: OutputType, output_id: int) -> str:
        return f"<{output_type.value}{output_id},LQ/>\r"

    @staticmethod
    def command_query_group_status(group_id: int) -> str:
        return f"<G{group_id},Q/>\r"

    @staticmethod
    def command_query_source_label(source_id: int) -> str:
        return f"<L{source_id},LQ/>\r"

    @staticmethod
    def command_query_zone_eq(zone_id: int) -> str:
        """Query zone EQ settings (treble, mid, bass)."""
        return f"<Z{zone_id}.MU,EQ/>\r"

    @staticmethod
    def command_set_zone_eq_treble(zone_id: int, level: int) -> str:
        """Set zone EQ treble level (-12 to +12)."""
        return f"<Z{zone_id}.MU,T{level}/>\r"

    @staticmethod
    def command_set_zone_eq_mid(zone_id: int, level: int) -> str:
        """Set zone EQ mid level (-12 to +12)."""
        return f"<Z{zone_id}.MU,M{level}/>\r"

    @staticmethod
    def command_set_zone_eq_bass(zone_id: int, level: int) -> str:
        """Set zone EQ bass level (-12 to +12)."""
        return f"<Z{zone_id}.MU,B{level}/>\r"

    def _process_received_message(self, message: str):
        """Parse received message and fire appropriate listener callback.
        
        Args:
            message: Complete message string (e.g., "<z1.mu,s=7/>")
        """
        # Process the most expected messages first which will
        # be responses to our polling requests.

        # Zone source response: <z1.mu,s=7/> means zone 1 is using source 7 (0 = no source)
        zone_source_match = ZONE_SOURCE_RESPONSE.match(message)
        if zone_source_match:
            self._logger.info(f"RECV: Zone source response: {message}")
            zone_id = int(zone_source_match.group(1))
            source_id = int(zone_source_match.group(2))
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
            # Parse level - can be numeric or "mute"
            if level_str.lower() == "mute":
                level = "mute"
            else:
                try:
                    level = int(level_str)
                except ValueError:
                    self._logger.warning(f"Invalid zone volume level: {level_str}")
                    return
            self._listener.zone_volume_level_received(zone_id, level)
            return
        
        # Zone EQ response: <z1.mu,eq, t = 0, m = 0, b = 0/>
        zone_eq_match = ZONE_EQ_RESPONSE.match(message)
        if zone_eq_match:
            self._logger.info(f"RECV: Zone EQ response: {message}")
            zone_id = int(zone_eq_match.group(1))
            treble = int(zone_eq_match.group(2))
            mid = int(zone_eq_match.group(3))
            bass = int(zone_eq_match.group(4))
            self._listener.zone_eq_received(zone_id, treble, mid, bass)
            return
        
        # Individual EQ component responses
        zone_treble_match = ZONE_EQ_TREBLE_RESPONSE.match(message)
        if zone_treble_match:
            self._logger.info(f"RECV: Zone treble response: {message}")
            zone_id = int(zone_treble_match.group(1))
            treble = int(zone_treble_match.group(2))
            self._listener.zone_eq_treble_received(zone_id, treble)
            return
        
        zone_mid_match = ZONE_EQ_MID_RESPONSE.match(message)
        if zone_mid_match:
            self._logger.info(f"RECV: Zone mid response: {message}")
            zone_id = int(zone_mid_match.group(1))
            mid = int(zone_mid_match.group(2))
            self._listener.zone_eq_mid_received(zone_id, mid)
            return
        
        zone_bass_match = ZONE_EQ_BASS_RESPONSE.match(message)
        if zone_bass_match:
            self._logger.info(f"RECV: Zone bass response: {message}")
            zone_id = int(zone_bass_match.group(1))
            bass = int(zone_bass_match.group(2))
            self._listener.zone_eq_bass_received(zone_id, bass)
            return
        
        # Group source response: <g1.mu,s=7/>
        group_source_match = GROUP_SOURCE_RESPONSE.match(message)
        if group_source_match:
            self._logger.info(f"RECV: Group source response: {message}")
            group_id = int(group_source_match.group(1))
            source_id = int(group_source_match.group(2))
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
            
            if level_str.lower() == "mute":
                level = "mute"
            else:
                try:
                    level = int(level_str)
                except ValueError:
                    self._logger.warning(f"Invalid group volume level: {level_str}")
                    return
            
            self._logger.info(f"Group {group_id} volume: {level}")
            self._listener.group_volume_level_received(group_id, level)
            return        

        # Source label response: <l7,lqMusic/>
        source_label_match = SOURCE_LABEL_RESPONSE.match(message)
        if source_label_match:
            self._logger.debug(f"Source label response received: {message}")
            source_id = int(source_label_match.group(1))
            label = source_label_match.group(2)
            self._listener.source_label_received(source_id, label)
            return

        # Zone label response: <z1,lqMain Bar/>
        zone_label_match = ZONE_LABEL_RESPONSE.match(message)
        if zone_label_match:
            self._logger.debug(f"Zone label response received: {message}")
            zone_id = int(zone_label_match.group(1))
            label = zone_label_match.group(2)
            self._listener.zone_label_received(zone_id, label)
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
                self._listener.zone_line_inputs_received(
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
            
            self._listener.group_status_received(group_id, enabled, zones)
            return

        # Group label response: <g1,lqMainBar+Snug/>
        group_label_match = GROUP_LABEL_RESPONSE.match(message)
        if group_label_match:
            self._logger.debug(f"Group label response received: {message}")
            group_id = int(group_label_match.group(1))
            label = group_label_match.group(2)
            self._listener.group_label_received(group_id, label)
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
                self._listener.group_line_inputs_received(
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

    def _process_zone_source_received(self, source_id: int, zone_id: int):
        """Process zone source response and notify listener.
        
        Args:
            source_id: Source ID (1-8)
            zone_id: Zone ID (1-8)
        """
        self._logger.debug(f"Source ID [{source_id}] Zone id [{zone_id}]")
        self._logger.info(f"Zone {zone_id} source: {source_id}")
        self._listener.zone_source_received(zone_id, source_id)

    def _process_group_source_received(self, source_id: int, group_id: int):
        """Process group source response and notify listener.
        
        Args:
            source_id: Source ID (1-8)
            group_id: Group ID (1-4)
        """
        self._logger.debug(f"Source ID [{source_id}] Group id [{group_id}]")
        self._logger.info(f"Group {group_id} source: {source_id}")
        self._listener.group_source_received(group_id, source_id)
