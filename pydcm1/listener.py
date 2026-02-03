from abc import ABC, abstractmethod
import time
from typing import List
import logging


class MixerResponseListener(ABC):

    def connected(self):
        pass

    def disconnected(self):
        pass

    def source_label_received(self, source_id: int, label: str):
        """Called when a source label is received."""
        pass

    def zone_label_received(self, zone_id: int, label: str):
        """Called when a zone label is received."""
        pass

    def zone_line_inputs_received(self, zone_id: int, enabled_inputs: dict[int, bool]):
        """Called when zone line input enabled status is received. Dict maps line_id to enabled status."""
        pass

    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        """Called when group status is received.
        
        Args:
            group_id: Group ID (1-4)
            enabled: Whether the group is enabled
            zones: List of zone IDs assigned to this group
        """
        pass

    def group_label_received(self, group_id: int, label: str):
        """Called when group label is received."""
        pass

    def group_line_inputs_received(self, group_id: int, enabled_inputs: dict[int, bool]):
        """Called when group line input enabled status is received. Dict maps line_id to enabled status."""
        pass

    def zone_source_received(self, zone_id: int, source_id: int):
        """Called when a source value is received for a zone."""
        pass

    def zone_volume_level_received(self, zone_id: int, level):
        """Called when zone volume level is received. Level is int (0-61) or 'mute'."""
        pass

    def group_source_received(self, group_id: int, source_id: int):
        """Called when a group's source is received."""
        pass

    def group_volume_level_received(self, group_id: int, level):
        """Called when a group's volume level is received."""
        pass

    def error(self, error_message: str):
        # By default, do nothing but can be overwritten to be notified of these messages.
        pass

    def source_change_requested(self, zone_id: int, source_id: int):
        # By default, do nothing but can be overwritten to be notified of these messages.
        pass

    def zone_eq_received(self, zone_id: int, treble: int, mid: int, bass: int):
        """Called when zone EQ values are received (combined query response). Values are in range -14 to +14."""
        pass

    def zone_eq_treble_received(self, zone_id: int, treble: int):
        """Called when zone EQ treble value is received. Value is in range -14 to +14."""
        pass

    def zone_eq_mid_received(self, zone_id: int, mid: int):
        """Called when zone EQ mid value is received. Value is in range -14 to +14."""
        pass

    def zone_eq_bass_received(self, zone_id: int, bass: int):
        """Called when zone EQ bass value is received. Value is in range -14 to +14."""
        pass


class MultiplexingListener(MixerResponseListener):
    _listeners: List[MixerResponseListener]

    def __init__(self):
        self._listeners = []

    def connected(self):
        for listener in self._listeners:
            listener.connected()

    def disconnected(self):
        for listener in self._listeners:
            listener.disconnected()

    def source_label_received(self, source_id: int, label: str):
        for listener in self._listeners:
            listener.source_label_received(source_id, label)

    def zone_label_received(self, zone_id: int, label: str):
        for listener in self._listeners:
            listener.zone_label_received(zone_id, label)

    def zone_line_inputs_received(self, zone_id: int, line_inputs: dict[int, bool]):
        for listener in self._listeners:
            listener.zone_line_inputs_received(zone_id, line_inputs)

    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        for listener in self._listeners:
            listener.group_status_received(group_id, enabled, zones)

    def group_label_received(self, group_id: int, label: str):
        for listener in self._listeners:
            listener.group_label_received(group_id, label)

    def group_line_inputs_received(self, group_id: int, enabled_inputs: dict[int, bool]):
        for listener in self._listeners:
            listener.group_line_inputs_received(group_id, enabled_inputs)

    def zone_source_received(self, zone_id: int, source_id: int):
        for listener in self._listeners:
            listener.zone_source_received(zone_id, source_id)

    def zone_volume_level_received(self, zone_id: int, level):
        for listener in self._listeners:
            listener.zone_volume_level_received(zone_id, level)

    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        for listener in self._listeners:
            listener.group_status_received(group_id, enabled, zones)

    def group_line_inputs_received(self, group_id: int, enabled_inputs: dict[int, bool]):
        for listener in self._listeners:
            listener.group_line_inputs_received(group_id, enabled_inputs)

    def group_volume_level_received(self, group_id: int, level):
        for listener in self._listeners:
            listener.group_volume_level_received(group_id, level)

    def group_source_received(self, group_id: int, source_id: int):
        for listener in self._listeners:
            listener.group_source_received(group_id, source_id)

    def error(self, error_message: str):
        for listener in self._listeners:
            listener.error(error_message)

    def source_change_requested(self, zone_id: int, source_id: int):
        for listener in self._listeners:
            listener.source_change_requested(zone_id, source_id)

    def zone_eq_received(self, zone_id: int, treble: int, mid: int, bass: int):
        for listener in self._listeners:
            listener.zone_eq_received(zone_id, treble, mid, bass)

    def zone_eq_treble_received(self, zone_id: int, treble: int):
        for listener in self._listeners:
            listener.zone_eq_treble_received(zone_id, treble)

    def zone_eq_mid_received(self, zone_id: int, mid: int):
        for listener in self._listeners:
            listener.zone_eq_mid_received(zone_id, mid)

    def zone_eq_bass_received(self, zone_id: int, bass: int):
        for listener in self._listeners:
            listener.zone_eq_bass_received(zone_id, bass)

    def register_listener(self, listener: MixerResponseListener):
        self._listeners.append(listener)

    def unregister_listener(self, listener: MixerResponseListener):
        if listener in self._listeners:
            self._listeners.remove(listener)
        else:
            logging.info("Listener isn't registered")

class LoggingListener(MixerResponseListener):

    def __init__(self, logger = logging):
        self.logger = logger

    def connected(self):
        self.logger.info("Connected")

    def disconnected(self):
        self.logger.info("Disconnected")

    def source_label_received(self, source_id: int, label: str):
        self.logger.info(f"Source {source_id} label received: {label}")

    def zone_label_received(self, zone_id: int, label: str):
        self.logger.info(f"Zone {zone_id} label received: {label}")

    def zone_line_inputs_received(self, zone_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        self.logger.info(f"Zone {zone_id} enabled line inputs received: {enabled}")

    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        status = "enabled" if enabled else "disabled"
        self.logger.info(f"Group {group_id} is {status} with zones: {zones}")

    def group_label_received(self, group_id: int, label: str):
        self.logger.info(f"Group {group_id} label received: {label}")

    def group_line_inputs_received(self, group_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        self.logger.info(f"Group {group_id} enabled line inputs received: {enabled}")

    def zone_source_received(self, zone_id: int, source_id: int):
        self.logger.info(f"Zone {zone_id} source received: {source_id}")

    def zone_volume_level_received(self, zone_id: int, level):
        self.logger.info(f"Zone {zone_id} volume received: {level}")

    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        status = "enabled" if enabled else "disabled"
        self.logger.info(f"Group {group_id} is {status} with zones: {zones}")

    def group_volume_level_received(self, group_id: int, level):
        self.logger.info(f"Group {group_id} volume received: {level}")

    def group_line_inputs_received(self, group_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        self.logger.info(f"Group {group_id} enabled line inputs received: {enabled}")

    def group_source_received(self, group_id: int, source_id: int):
        self.logger.info(f"Group {group_id} source received: {source_id}")

    def zone_eq_received(self, zone_id: int, treble: int, mid: int, bass: int):
        self.logger.info(f"Zone {zone_id} EQ combined received: treble={treble:+d}, mid={mid:+d}, bass={bass:+d}")

    def zone_eq_treble_received(self, zone_id: int, treble: int):
        self.logger.info(f"Zone {zone_id} EQ treble received: {treble:+d}")

    def zone_eq_mid_received(self, zone_id: int, mid: int):
        self.logger.info(f"Zone {zone_id} EQ mid received: {mid:+d}")

    def zone_eq_bass_received(self, zone_id: int, bass: int):
        self.logger.info(f"Zone {zone_id} EQ bass received: {bass:+d}")
