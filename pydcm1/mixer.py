from typing import Optional

from pydcm1.listener import MultiplexingListener, SourceChangeListener
from pydcm1.protocol import MixerProtocol


class Zone:
    """Represents a zone in the DCM1 mixer."""
    def __init__(self, zone_id: int, name: str):
        self.id = zone_id
        self.name = name


class ZoneLabelListener(SourceChangeListener):
    """Listener that updates zone labels in the mixer."""
    
    def __init__(self, mixer):
        self._mixer = mixer
    
    def source_changed(self, zone_id: int, source_id: int):
        pass
    
    def connected(self):
        pass
    
    def disconnected(self):
        pass
    
    def power_changed(self, power: bool):
        pass
    
    def zone_label_changed(self, zone_id: int, label: str):
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
    
    def source_label_changed(self, source_id: int, label: str):
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
    
    def group_label_changed(self, group_id: int, label: str):
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
    
    def group_status_changed(self, group_id: int, enabled: bool, zones: list[int]):
        """Update the group status when received from device."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            group.enabled = enabled
            group.zones = zones
    
    def volume_level_changed(self, zone_id: int, level):
        pass


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

    def __init__(self, hostname, port):
        self.hostname : str = hostname
        self._multiplex_callback = MultiplexingListener()
        self.protocol = MixerProtocol(hostname, port, self._multiplex_callback)
        self.zones_by_id : dict[int, Zone] = {}
        self.zones_by_name : dict[str, Zone] = {}
        self.sources_by_id : dict[int, Source] = {}
        self.sources_by_name : dict[str, Source] = {}
        self.groups_by_id : dict[int, Group] = {}
        self.groups_by_name : dict[str, Group] = {}
        self.mac : Optional[str] = None
        self.device_name : Optional[str] = None
        self.firmware_version : Optional[str] = None
        
        # Register listener to update zone labels
        self._zone_label_listener = ZoneLabelListener(self)
        self._multiplex_callback.register_listener(self._zone_label_listener)

    @property
    def zone_names(self):
        return list(self.zones_by_name.keys())

    @property
    def source_names(self):
        return list(self.sources_by_name.keys())

    async def async_connect(self):
        # DCM1 has 8 zones, 8 line sources, and 4 groups (hardcoded for now)
        # TODO: In the future, query zone/source names from device
        # For now, create simple numbered zones and sources
        for i in range(1, 9):
            zone = Zone(i, f"Zone {i}")
            self.zones_by_id[i] = zone
            self.zones_by_name[zone.name] = zone
            
            source = Source(i, f"Source {i}")
            self.sources_by_id[i] = source
            self.sources_by_name[source.name] = source
        
        # Initialize 4 groups
        for i in range(1, 5):
            group = Group(i, f"Group {i}")
            self.groups_by_id[i] = group
            self.groups_by_name[group.name] = group
        
        await self.protocol.async_connect()

    def close(self):
        self.protocol.close()

    def set_zone_source(self, zone_id: int, source_id: int):
        """Set a zone to use a specific source."""
        self.protocol.send_change_source(source_id, zone_id)

    def set_volume(self, zone_id: int, level):
        """Set volume level for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
            level: Volume level - int (0-61 where 20 = -20dB, 62 for mute) or "mute"
        """
        self.protocol.send_volume_level(zone_id, level)

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

    def change_source(self, source_id: int, zone_id: int):
        self.protocol.send_change_source(source_id, zone_id)

    def change_source_by_name(self, source_name: str, zone_name: str):
        source = self.sources_by_name.get(source_name)
        zone = self.zones_by_name.get(zone_name)
        if source and zone:
            self.protocol.send_change_source(source.id, zone.id)

    def update_status(self):
        self.protocol.send_zone_source_query_messages()

    def query_all_labels(self):
        """Query all zone and source labels from the device."""
        self.protocol.send_zone_label_query_messages()
        self.protocol.send_source_label_query_messages()
        self.protocol.send_volume_level_query_messages()

    def query_all_groups(self):
        """Query all group labels and statuses from the device."""
        self.protocol.send_all_group_queries()

    def status_of_zone(self, zone_id: int) -> Optional[int]:
        return self.protocol.get_status_of_zone(zone_id)

    def status_of_all_zones(self) -> list[tuple[int, Optional[int]]]:
        return self.protocol.get_status_of_all_zones()

    def get_enabled_line_inputs(self, zone_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
            
        Returns:
            Dictionary mapping line input ID (1-8) to enabled status (True/False)
        """
        return self.protocol.get_enabled_line_inputs(zone_id)

    def query_line_inputs(self, zone_id: int):
        """Query which line inputs are enabled for a zone.
        
        Args:
            zone_id: Zone ID (1-8)
        """
        self.protocol.send_line_input_enable_query_messages(zone_id)

    def send_volume_up(self, zone_id: int):
        """Send volume up command to a zone."""
        self.protocol.send_volume_up(zone_id)

    def send_volume_down(self, zone_id: int):
        """Send volume down command to a zone."""
        self.protocol.send_volume_down(zone_id)

    def send_mute(self, zone_id: int):
        """Send mute toggle command to a zone."""
        self.protocol.send_mute(zone_id)

    def register_listener(self, listener):
        self._multiplex_callback.register_listener(listener)

    def unregister_listener(self, listener):
        self._multiplex_callback.unregister_listener(listener)
