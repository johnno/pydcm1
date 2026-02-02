from typing import Optional

from pydcm1.listener import MultiplexingListener, MixerResponseListener
from pydcm1.protocol import MixerProtocol


class Zone:
    """Represents a zone in the DCM1 mixer."""
    def __init__(self, zone_id: int, name: str):
        self.id = zone_id
        self.name = name


class MixerListener(MixerResponseListener):
    """Listener that tracks mixer state updates and reception timestamps.
    
    Note: Most state management (sources, volumes, line inputs, zone/group sources) is handled
    directly by protocol.py. This listener primarily tracks when specific data has been received
    (for wait_for_* timeout logic). Label updates are still applied here since zones/sources/groups
    are high-level objects in this mixer class. This explains why many callback parameters go unused.
    """

    def __init__(self, mixer):
        self._mixer = mixer
        
    def source_label_received(self, source_id: int, label: str):
        """Update the source label when received from device."""
        source = self._mixer.sources_by_id.get(source_id)
        if source:
            # Remove old name from sources_by_name
            old_name = source.name
            if old_name in self._mixer.sources_by_name:
                del self._mixer.sources_by_name[old_name]
            # Update source name
            source.name = label
            self._mixer.sources_by_name[label] = source
            # Track that we received this source's label
            self._mixer._sources_labels_received.add(source_id)
    
    def zone_label_received(self, zone_id: int, label: str):
        """Update the zone label when received from device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            # Remove old name from zones_by_name
            old_name = zone.name
            if old_name in self._mixer.zones_by_name:
                del self._mixer.zones_by_name[old_name]
            # Update zone name
            zone.name = label
            self._mixer.zones_by_name[label] = zone
            # Track that we received this zone's label
            self._mixer._zones_labels_received.add(zone_id)

    def zone_line_inputs_received(self, zone_id: int, line_inputs: dict[int, bool]):
        """Track when a zone's line input data is received."""
        # Protocol only calls this when all 8 line inputs are received
        self._mixer._zones_line_inputs_received.add(zone_id)

    def zone_source_received(self, zone_id: int, source_id: int):
        """Track when a zone's source is received from the device.
        
        Note: source_id is not used because protocol.py already stores the source state.
        This listener only marks that reception occurred (for wait_for_zone_data timeout).
        """
        self._mixer._zones_sources_received.add(zone_id)

    def zone_volume_level_received(self, zone_id: int, level):
        """Track when a zone's volume is received from the device.
        
        Note: level is not used because protocol.py already stores the volume state.
        This listener only marks that reception occurred (for wait_for_zone_data timeout).
        """
        self._mixer._zones_volume_received.add(zone_id)

    def group_label_received(self, group_id: int, label: str):
        """Update the group label when received from device."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            # Remove old name from groups_by_name
            old_name = group.name
            if old_name in self._mixer.groups_by_name:
                del self._mixer.groups_by_name[old_name]
            # Update group name
            group.name = label
            self._mixer.groups_by_name[label] = group
            # Track that we received this group's label
            self._mixer._groups_labels_received.add(group_id)
    
    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        """Update the group status when received from device."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            group.enabled = enabled
            group.zones = zones
            # Track that we received this group's status
            self._mixer._groups_status_received.add(group_id)
    
    def group_volume_level_received(self, group_id: int, level):
        """Track when a group's volume is received.
        
        Note: level is not used because protocol.py already stores the volume state.
        This listener only marks that reception occurred (for wait_for_group_data timeout).
        """
        self._mixer._groups_volume_received.add(group_id)
    
    def group_line_inputs_received(self, group_id: int, line_inputs: dict[int, bool]):
        """Track when a group's line input data is received."""
        # Protocol only calls this when all 8 line inputs are received
        self._mixer._groups_line_inputs_received.add(group_id)
    
    def group_source_received(self, group_id: int, source_id: int):
        """Track when a group's source is received from the device.
        
        Note: source_id is not used because protocol.py already stores the source state.
        This listener only marks that reception occurred (for wait_for_group_data timeout).
        """
        self._mixer._groups_sources_received.add(group_id)    

class Source:
    """Represents a source in the DCM1 mixer."""
    def __init__(self, source_id: int, name: str):
        self.id = source_id
        self.name = name


class Group:
    """Represents a group in the DCM1 mixer."""
    def __init__(self, group_id: int, name: str, enabled: bool = False, zones: list[int] = None):
        self.id = group_id
        self.name = name
        self.enabled = enabled
        self.zones = zones if zones else []


class DCM1Mixer:

    def __init__(self, hostname, port, enable_heartbeat=True):
        self.hostname : str = hostname
        self._multiplex_callback = MultiplexingListener()
        self.protocol = MixerProtocol(hostname, port, self._multiplex_callback, enable_heartbeat=enable_heartbeat)
        self.zones_by_id : dict[int, Zone] = {}
        self.zones_by_name : dict[str, Zone] = {}
        self.sources_by_id : dict[int, Source] = {}
        self.sources_by_name : dict[str, Source] = {}
        self.groups_by_id : dict[int, Group] = {}
        self.groups_by_name : dict[str, Group] = {}
        self.mac : Optional[str] = None
        self.device_name : Optional[str] = None
        self.firmware_version : Optional[str] = None

        # Track which source IDs have received their labels
        self._sources_labels_received : set[int] = set()

        # Track which zone IDs have have received their attributes
        self._zones_labels_received : set[int] = set()
        self._zones_line_inputs_received : set[int] = set()
        self._zones_sources_received : set[int] = set()
        self._zones_volume_received : set[int] = set()

        # Track which group IDs have received their attributes
        self._groups_status_received : set[int] = set()
        self._groups_labels_received : set[int] = set()
        self._groups_volume_received : set[int] = set()
        self._groups_line_inputs_received : set[int] = set()
        self._groups_sources_received : set[int] = set()
                
        # Register listener to update mixer state
        self._mixer_listener = MixerListener(self)
        self._multiplex_callback.register_listener(self._mixer_listener)

    @property
    def source_names(self):
        return list(self.sources_by_name.keys())

    @property
    def zone_names(self):
        return list(self.zones_by_name.keys())

    @property
    def group_names(self):
        return list(self.groups_by_name.keys())
    
    def get_zone_enabled_line_inputs(self, zone_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
        
        Returns:
            Dictionary mapping line input ID (1-8) to enabled status (True/False)
        """
        return self.protocol.get_zone_enabled_line_inputs(zone_id)
    
    def get_zone_source(self, zone_id: int) -> Optional[int]:
        """Get the current source ID for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
        
        Returns:
            Source ID (1-8) or None if not known
        """
        return self.protocol.get_zone_source(zone_id)
    
    def get_zone_volume_level(self, zone_id: int):
        """Get the volume level for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
        
        Returns:
            Volume level (int 0-61) or "mute" or None if not known
        """
        return self.protocol.get_zone_volume_level(zone_id)

    def get_group_source(self, group_id: int) -> Optional[int]:
        """Get the current source ID for a group.
        
        Args:
            group_id: Group ID (1-4)
        
        Returns:
            Source ID (1-8) or None if not known
        """
        return self.protocol.get_group_source(group_id)
    
    def get_group_enabled_line_inputs(self, group_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a group.
        
        Args:
            group_id: Group ID (1-4)
        
        Returns:
            Dictionary mapping line input ID (1-8) to enabled status (True/False)
        """
        return self.protocol.get_group_enabled_line_inputs(group_id)

    def get_group_volume_level(self, group_id: int):
        """Get the volume level for a group.
        
        Args:
            group_id: Group ID (1-4)
        
        Returns:
            Volume level (int 0-61) or "mute" or None if not known
        """
        return self.protocol.get_group_volume_level(group_id)    
    
    def register_listener(self, listener):
        self._multiplex_callback.register_listener(listener)

    def unregister_listener(self, listener):
        self._multiplex_callback.unregister_listener(listener)

    async def async_connect(self):
        # Initialize zones, sources, and groups based on protocol configuration
        for i in range(1, self.protocol._zone_count + 1):
            zone = Zone(i, f"Zone {i}")
            self.zones_by_id[i] = zone
            self.zones_by_name[zone.name] = zone
            
            source = Source(i, f"Source {i}")
            self.sources_by_id[i] = source
            self.sources_by_name[source.name] = source
        
        # Initialize groups
        for i in range(1, self.protocol._group_count + 1):
            group = Group(i, f"Group {i}")
            self.groups_by_id[i] = group
            self.groups_by_name[group.name] = group
        
        # Note that connecting will also start querying all mixer state
        # and MixerListener will populate the zone/source/group names.
        await self.protocol.async_connect()

    def close(self):
        self.protocol.close()

    def set_zone_source(self, zone_id: int, source_id: int):
        """Set a zone to use a specific source."""
        self.protocol.send_zone_source(source_id, zone_id)

    def set_zone_volume(self, zone_id: int, level):
        """Set volume level for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
            level: Volume level - int (0-61 where 20 = -20dB, 62 for mute) or "mute"
        """
        self.protocol.send_zone_volume_level(zone_id, level)

    def set_group_source(self, group_id: int, source_id: int):
        """Set a group to use a specific source.
        
        Args:
            group_id: Group ID (1-4)
            source_id: Source ID (1-8)
        """
        self.protocol.send_group_source(source_id, group_id)

    def set_group_volume(self, group_id: int, level):
        """Set volume level for a group.
        
        Args:
            group_id: Group ID (1-4)
            level: Volume level - int (0-61 where 20 = -20dB, 62 for mute) or "mute"
        """
        self.protocol.send_group_volume_level(group_id, level)

    # Methods below are a hangover from when main.py queried attributes
    # directly not knowing that all attributes are queried on connection
    # by protocol.py.
    #
    # protocol.py is doing more than it should as vibe coding resulted
    # in it putting everything there, so it models the entire mixer state
    # which is what this class should be doing.

    def query_status(self):
        """Query all relevant mixer state from the device."""
        self.query_source_labels()
        self.query_all_zones()
        self.query_all_groups()

    def query_source_labels(self):
        """Query all source labels from the device."""
        self.protocol.enqueue_source_label_query_commands()

    def query_all_zones(self):
        """Query all zone labels, sources, line input enables, and volume levels from the device."""
        self.protocol.enqueue_zone_label_query_commands()
        self.protocol.enqueue_zone_source_query_commands()
        self.protocol.enqueue_zone_line_input_enable_query_commands()
        self.protocol.enqueue_zone_volume_level_query_commands()

    def query_all_groups(self):
        """Query all group statuses, labels, sources, line input enables, and volume levels from the device."""
        self.protocol.enqueue_group_status_query_commands()
        self.protocol.enqueue_group_label_query_commands()
        self.protocol.enqueue_group_line_input_enable_query_commands()        
        self.protocol.enqueue_group_source_query_commands()
        self.protocol.enqueue_group_volume_level_query_commands()

    async def wait_for_source_labels(self, timeout: float = 6.0):
        """Wait for all source labels to be received from the device.
        
        Args:
            timeout: Maximum time to wait in seconds
        Returns:
            True if all source labels received, False if timeout
        """
        import asyncio
        start_time = asyncio.get_event_loop().time()
        expected_source_ids = set(self.sources_by_id.keys())
        while asyncio.get_event_loop().time() - start_time < timeout:
            if self._sources_labels_received >= expected_source_ids:
                return True
            await asyncio.sleep(0.1)
        missing_sources = expected_source_ids - self._sources_labels_received
        if missing_sources:
            print(f"Warning: Timeout waiting for source labels: {missing_sources}")
        return False

    async def wait_for_zone_data(self, timeout: float = 10.0):
        """Wait for all zone data (labels, line inputs, volume, and sources) to be received from the device.
        
        Args:
            timeout: Maximum time to wait in seconds
        Returns:
            True if all data received, False if timeout
        """
        import asyncio
        start_time = asyncio.get_event_loop().time()
        expected_zone_ids = set(self.zones_by_id.keys())
        while asyncio.get_event_loop().time() - start_time < timeout:
            if (self._zones_labels_received >= expected_zone_ids and
                self._zones_line_inputs_received >= expected_zone_ids and
                self._zones_volume_received >= expected_zone_ids and
                self._zones_sources_received >= expected_zone_ids):
                return True
            await asyncio.sleep(0.1)
        # Timeout - log what we're missing
        missing_labels = expected_zone_ids - self._zones_labels_received
        missing_line_inputs = expected_zone_ids - self._zones_line_inputs_received
        missing_volumes = expected_zone_ids - self._zones_volume_received
        missing_sources = expected_zone_ids - self._zones_sources_received
        if missing_labels:
            print(f"Warning: Timeout waiting for zone labels: {missing_labels}")
        if missing_line_inputs:
            print(f"Warning: Timeout waiting for zone line inputs: {missing_line_inputs}")
        if missing_volumes:
            print(f"Warning: Timeout waiting for zone volumes: {missing_volumes}")
        if missing_sources:
            print(f"Warning: Timeout waiting for zone sources: {missing_sources}")
        return False

    async def wait_for_group_data(self, timeout: float = 10.0):
        """Wait for all group data to be received from the device, including sources.
        
        Args:
            timeout: Maximum time to wait in seconds
        Returns:
            True if all data received, False if timeout
        """
        import asyncio
        start_time = asyncio.get_event_loop().time()
        expected_group_ids = set(self.groups_by_id.keys())
        while asyncio.get_event_loop().time() - start_time < timeout:
            # Check if we've received labels, status, volume, line inputs, and sources for all groups
            if (self._groups_labels_received >= expected_group_ids and 
                self._groups_status_received >= expected_group_ids and
                self._groups_volume_received >= expected_group_ids and
                self._groups_line_inputs_received >= expected_group_ids and
                self._groups_sources_received >= expected_group_ids):
                return True
            await asyncio.sleep(0.1)
        # Timeout - log what we're missing
        missing_labels = expected_group_ids - self._groups_labels_received
        missing_status = expected_group_ids - self._groups_status_received
        missing_volume = expected_group_ids - self._groups_volume_received
        missing_line_inputs = expected_group_ids - self._groups_line_inputs_received
        missing_sources = expected_group_ids - self._groups_sources_received
        if missing_labels:
            print(f"Warning: Timeout waiting for group labels: {missing_labels}")
        if missing_status:
            print(f"Warning: Timeout waiting for group status: {missing_status}")
        if missing_volume:
            print(f"Warning: Timeout waiting for group volume: {missing_volume}")
        if missing_line_inputs:
            print(f"Warning: Timeout waiting for group line inputs: {missing_line_inputs}")
        if missing_sources:
            print(f"Warning: Timeout waiting for group sources: {missing_sources}")
        return False
