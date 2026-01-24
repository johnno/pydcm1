from abc import ABC, abstractmethod
import time
from typing import List
import logging


class SourceChangeListener(ABC):

    @abstractmethod
    def source_changed(self, zone_id: int, source_id: int):
        pass

    @abstractmethod
    def connected(self):
        pass

    @abstractmethod
    def disconnected(self):
        pass

    @abstractmethod
    def power_changed(self, power: bool):
        pass

    def zone_label_changed(self, zone_id: int, label: str):
        pass

    @abstractmethod
    def zone_label_changed(self, zone_id: int, label: str):
        pass

    @abstractmethod
    def source_label_changed(self, source_id: int, label: str):
        pass

    @abstractmethod
    def volume_level_changed(self, zone_id: int, level):
        """Called when zone volume level changes. Level is int (0-61) or 'mute'."""
        pass

    def line_inputs_changed(self, zone_id: int, enabled_inputs: dict[int, bool]):
        """Called when line input enabled status is received. Dict maps line_id to enabled status."""
        pass

    def group_label_changed(self, group_id: int, label: str):
        """Called when group label is received."""
        pass

    def group_status_changed(self, group_id: int, enabled: bool, zones: list[int]):
        """Called when group status is received.
        
        Args:
            group_id: Group ID (1-4)
            enabled: Whether the group is enabled
            zones: List of zone IDs assigned to this group
        """
        pass

    def group_line_inputs_changed(self, group_id: int, enabled_inputs: dict[int, bool]):
        """Called when group line input enabled status is received. Dict maps line_id to enabled status."""
        pass

    def group_volume_changed(self, group_id: int, level):
        """Called when a group's volume level changes."""
        pass

    def volume_level_changed(self, zone_id: int, level):
        pass

    def error(self, error_message: str):
        # By default, do nothing but can be overwritten to be notified of these messages.
        pass

    def source_change_requested(self, zone_id: int, source_id: int):
        # By default, do nothing but can be overwritten to be notified of these messages.
        pass


class MultiplexingListener(SourceChangeListener):

    _listeners: List[SourceChangeListener]

    def __init__(self):
        self._listeners = []

    def source_changed(self, zone_id: int, source_id: int):
        for listener in self._listeners:
            listener.source_changed(zone_id, source_id)

    def power_changed(self, power: bool):
        for listener in self._listeners:
            listener.power_changed(power)

    def zone_label_changed(self, zone_id: int, label: str):
        for listener in self._listeners:
            listener.zone_label_changed(zone_id, label)

    def source_label_changed(self, source_id: int, label: str):
        for listener in self._listeners:
            listener.source_label_changed(source_id, label)

    def volume_level_changed(self, zone_id: int, level):
        for listener in self._listeners:
            listener.volume_level_changed(zone_id, level)

    def group_label_changed(self, group_id: int, label: str):
        for listener in self._listeners:
            listener.group_label_changed(group_id, label)

    def group_status_changed(self, group_id: int, enabled: bool, zones: list[int]):
        for listener in self._listeners:
            listener.group_status_changed(group_id, enabled, zones)

    def group_line_inputs_changed(self, group_id: int, enabled_inputs: dict[int, bool]):
        for listener in self._listeners:
            listener.group_line_inputs_changed(group_id, enabled_inputs)

    def group_volume_changed(self, group_id: int, level):
        for listener in self._listeners:
            listener.group_volume_changed(group_id, level)

    def group_source_changed(self, group_id: int, source_id: int):
        for listener in self._listeners:
            listener.group_source_changed(group_id, source_id)

    def connected(self):
        for listener in self._listeners:
            listener.connected()

    def disconnected(self):
        for listener in self._listeners:
            listener.disconnected()

    def error(self, error_message: str):
        for listener in self._listeners:
            listener.error(error_message)

    def source_change_requested(self, zone_id: int, source_id: int):
        for listener in self._listeners:
            listener.source_change_requested(zone_id, source_id)

    def register_listener(self, listener: SourceChangeListener):
        self._listeners.append(listener)

    def unregister_listener(self, listener: SourceChangeListener):
        if listener in self._listeners:
            self._listeners.remove(listener)
        else:
            logging.info("Listener isn't registered")


class LoggingListener(SourceChangeListener):

    def __init__(self, logger = logging):
        self.logger = logger

    def connected(self):
        self.logger.info("Connected")

    def disconnected(self):
        self.logger.info("Disconnected")

    def source_changed(self, zone_id: int, source_id: int):
        self.logger.info(f"{zone_id} changed to source: {source_id}")

    def power_changed(self, power: bool):
        self.logger.info(f"Power changed to : {power}")

    def zone_label_changed(self, zone_id: int, label: str):
        self.logger.info(f"Zone {zone_id} label: {label}")

    def source_label_changed(self, source_id: int, label: str):
        self.logger.info(f"Source {source_id} label: {label}")

    def volume_level_changed(self, zone_id: int, level):
        self.logger.info(f"Zone {zone_id} volume: {level}")

    def group_label_changed(self, group_id: int, label: str):
        self.logger.info(f"Group {group_id} label: {label}")

    def group_status_changed(self, group_id: int, enabled: bool, zones: list[int]):
        status = "enabled" if enabled else "disabled"
        self.logger.info(f"Group {group_id} is {status} with zones: {zones}")

    def group_volume_changed(self, group_id: int, level):
        self.logger.info(f"Group {group_id} volume: {level}")

    def group_line_inputs_changed(self, group_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        self.logger.info(f"Group {group_id} enabled line inputs: {enabled}")

    def line_inputs_changed(self, zone_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        self.logger.info(f"Zone {zone_id} enabled line inputs: {enabled}")

    def group_source_changed(self, group_id: int, source_id: int):
        self.logger.info(f"Group {group_id} changed to source: {source_id}")


class PrintingListener(SourceChangeListener):

    def connected(self):
        print("Connected")

    def disconnected(self):
        print("Disconnected")

    def source_changed(self, zone_id, source_id):
        print(f"{zone_id} changed to source: {source_id}")

    def power_changed(self, power: bool):
        print(f"Power changed to : {power}")

    def zone_label_changed(self, zone_id: int, label: str):
        print(f"Zone {zone_id} label changed to: {label}")

    def source_label_changed(self, source_id: int, label: str):
        print(f"Source {source_id} label changed to: {label}")

    def volume_level_changed(self, zone_id: int, level):
        print(f"Zone {zone_id} volume changed to: {level}")

    def group_label_changed(self, group_id: int, label: str):
        print(f"Group {group_id} label changed to: {label}")

    def group_status_changed(self, group_id: int, enabled: bool, zones: list[int]):
        status = "enabled" if enabled else "disabled"
        print(f"Group {group_id} is {status} with zones: {zones}")

    def group_volume_changed(self, group_id: int, level):
        print(f"Group {group_id} volume changed to: {level}")

    def group_line_inputs_changed(self, group_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        print(f"Group {group_id} enabled line inputs: {enabled}")

    def line_inputs_changed(self, zone_id: int, line_inputs: dict[int, bool]):
        enabled = [line_id for line_id, enabled in line_inputs.items() if enabled]
        print(f"Zone {zone_id} enabled line inputs: {enabled}")

    def group_source_changed(self, group_id: int, source_id: int):
        print(f"Group {group_id} changed to source: {source_id}")


class TurningOnListener(SourceChangeListener):
    """
    Listener that will turn on the Mixer if a user makes a change using the mixer mobile app.
    The standard behaviour for the mobile app is to ignore source changes if the mixer is turned off.
    """

    TIMEOUT_SECONDS = 15

    def __init__(self, mixer, logger = logging):
        self.last_requested_zone_id = None
        self.last_requested_source_id = None
        self.last_requested_at = 0
        self.change_source_on_power_on = False
        self.mixer = mixer
        self.logger = logger

    def source_changed(self, zone_id: int, source_id: int):
        pass

    def connected(self):
        pass

    def disconnected(self):
        pass

    def power_changed(self, power: bool):
        if self.change_source_on_power_on and power:
            self.change_source_on_power_on = False
            self.logger.info(
                f"Attempting to change mixer source zone "
                f"{self.last_requested_zone_id} to source {self.last_requested_source_id}"
            )
            self.mixer.change_source(int(self.last_requested_source_id), int(self.last_requested_zone_id))
            self.last_requested_zone_id = None
            self.last_requested_source_id = None

    def zone_label_changed(self, zone_id: int, label: str):
        pass

    def source_label_changed(self, source_id: int, label: str):
        pass

    def volume_level_changed(self, zone_id: int, level):
        pass

    def line_inputs_changed(self, zone_id: int, line_inputs: dict[int, bool]):
        pass

    def group_label_changed(self, group_id: int, label: str):
        pass

    def group_status_changed(self, group_id: int, enabled: bool, zones: list[int]):
        pass

    def group_volume_changed(self, group_id: int, level):
        pass

    def group_line_inputs_changed(self, group_id: int, line_inputs: dict[int, bool]):
        pass

    def group_source_changed(self, group_id: int, source_id: int):
        pass

    def error(self, error_message: str):
        error_at = time.time()
        if self.last_requested_at > (error_at - self.TIMEOUT_SECONDS) and\
                self.last_requested_source_id is not None and\
                self.last_requested_zone_id is not None:
            self.last_requested_at = 0
            self.logger.info("Attempting to turn on mixer")
            self.mixer.turn_on()
            self.change_source_on_power_on = True
            # We will change the source once we get a successful mixer turned on event.

    def source_change_requested(self, zone_id: int, source_id: int):
        self.last_requested_zone_id = zone_id
        self.last_requested_source_id = source_id
        self.last_requested_at = time.time()
