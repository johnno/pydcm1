"""DCM1 Mixer - Enhanced with queue, heartbeat, inflight tracking, and reconnection.

This module contains the high-level mixer abstraction with:
- Domain objects (Source, Zone, Group)
- Debouncing logic for volume/source changes
- Confirmation logic for commands
- Queue management and command worker
- Heartbeat/polling for syncing with physical controls
- Inflight command tracking and recovery
- Reconnection logic and watchdog
- MixerProtocol instance creation and management

This class implements MixerResponseListener to receive callbacks from the transport."""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from asyncio import PriorityQueue, Task
from typing import Any, Optional

from pydcm1.listener import MultiplexingListener, MixerResponseListener
from pydcm1.protocol import MixerProtocol, OutputType

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


class Source:
    """Represents a source in the DCM1 mixer."""
    def __init__(self, source_id: int, name: str):
        self._id = source_id
        self._name = name
        self._label_received: bool = False
    
    @property
    def id(self) -> int:
        """Source ID."""
        return self._id
    
    @property
    def name(self) -> str:
        """Source name/label."""
        return self._name
    
    @property
    def label_received(self) -> bool:
        """Whether label has been received from device."""
        return self._label_received


class Output(ABC):
    """Base class for Zone/Group outputs with shared control logic."""
    def __init__(self, output_id: int, name: str, mixer: 'DCM1Mixer' = None):
        self._id = output_id
        self._name = name
        self._mixer = mixer  # Reference to parent mixer for validation and control

        # State: Operational values (stored with underscore prefix)
        self._source: Optional[int] = None
        self._volume: Optional[int | str] = None
        self._line_inputs: dict[int, bool] = {}  # Maps line_id to enabled bool
        self._eq_treble: Optional[int] = None  # EQ treble (-14 to +14)
        self._eq_mid: Optional[int] = None  # EQ mid (-14 to +14)
        self._eq_bass: Optional[int] = None  # EQ bass (-14 to +14)

        # Control: Debounce and confirmation tasks (private implementation details)
        self._source_debounce_task: Optional[Task[Any]] = None
        self._volume_debounce_task: Optional[Task[Any]] = None
        self._source_confirm_task: Optional[Task[Any]] = None
        self._volume_confirm_task: Optional[Task[Any]] = None

        # Tracking: Which attributes have been received from device
        self._label_received: bool = False
        self._line_inputs_received: bool = False
        self._source_received: bool = False
        self._volume_received: bool = False

    @property
    def id(self) -> int:
        """Output ID."""
        return self._id

    @property
    def name(self) -> str:
        """Output name/label."""
        return self._name

    @property
    def label_received(self) -> bool:
        """Whether label has been received from device."""
        return self._label_received

    @property
    def source(self) -> Optional[int]:
        """Source ID for this output."""
        return self._source

    @property
    def source_received(self) -> bool:
        """Whether source has been received from device."""
        return self._source_received

    @property
    def volume(self) -> Optional[int | str]:
        """Volume level for this output."""
        return self._volume

    @property
    def volume_received(self) -> bool:
        """Whether volume has been received from device."""
        return self._volume_received

    @property
    def line_inputs(self) -> dict[int, bool]:
        """Line inputs enabled for this output."""
        return self._line_inputs

    @property
    def line_inputs_received(self) -> bool:
        """Whether line inputs have been received from device."""
        return self._line_inputs_received

    @property
    def eq_treble(self) -> Optional[int]:
        """EQ treble value (-14 to +14)."""
        return self._eq_treble

    @property
    def eq_mid(self) -> Optional[int]:
        """EQ mid value (-14 to +14)."""
        return self._eq_mid

    @property
    def eq_bass(self) -> Optional[int]:
        """EQ bass value (-14 to +14)."""
        return self._eq_bass

    def all_initial_data_received(self) -> bool:
        """Whether all initial state data has been received for this output."""
        return (
            self.label_received
            and self.line_inputs_received
            and self.volume_received
            and self.source_received
        )

    def _update_volume_unless_debouncing(self, value: Optional[int | str]):
        """Update volume from device only if not currently debouncing. Ignores stale responses."""
        if self._volume_debounce_task and not self._volume_debounce_task.done():
            if self._mixer:
                self._mixer._logger.debug(
                    f"Ignoring stale volume response for {self._type_name} {self._id} while command is debouncing"
                )
            return
        self._volume = value
        if value is not None:
            self._volume_received = True

    def _update_source_unless_debouncing(self, value: Optional[int]):
        """Update source from device only if not currently debouncing. Ignores stale responses."""
        if self._source_debounce_task and not self._source_debounce_task.done():
            if self._mixer:
                self._mixer._logger.debug(
                    f"Ignoring stale source response for {self._type_name} {self._id} while command is debouncing"
                )
            return
        self._source = value
        if value is not None:
            self._source_received = True

    def set_source(self, source_id: int):
        """Set source for this output with validation."""
        if not self._mixer:
            raise RuntimeError(f"{self._type_name} {self._id} has no mixer reference")
        if not (1 <= source_id <= self._mixer._source_count):
            self._mixer._logger.error(
                f"Invalid source_id {source_id}, must be 1-{self._mixer._source_count}"
            )
            return
        self._send_source(source_id)

    def _send_source(self, source_id: int):
        """Send source command with debounce."""
        if not self._mixer:
            return
        self._mixer._logger.info(
            f"Source request - {self._type_name}: {self._id} to source: {source_id} (debounced)"
        )

        # Cancel any pending debounce/confirm tasks
        if self._source_debounce_task and not self._source_debounce_task.done():
            self._source_debounce_task.cancel()
        if self._source_confirm_task and not self._source_confirm_task.done():
            self._source_confirm_task.cancel()

        # Create debounce task
        self._source_debounce_task = self._mixer._loop.create_task(
            self._debounce_source(source_id)
        )

    async def _debounce_source(self, source_id: int):
        """Debounce and send source command."""
        try:
            await asyncio.sleep(self._mixer._volume_debounce_delay)
            if self._source_debounce_task != asyncio.current_task():
                return
            command = MixerProtocol.command_set_source(self._output_type, self._id, source_id)
            self._mixer._enqueue_command(command)
            if self._mixer._command_confirmation:
                self._source_confirm_task = self._mixer._loop.create_task(
                    self._confirm_source(source_id)
                )
            self._source_debounce_task = None
        except asyncio.CancelledError:
            self._source_debounce_task = None

    async def _confirm_source(self, expected_source: int):
        """Confirm source was set correctly."""
        await asyncio.sleep(0.3)
        self._mixer._enqueue_command(
            MixerProtocol.command_query_source(self._output_type, self._id),
            PRIORITY_READ,
        )
        await asyncio.sleep(0.5)

        if self.source != expected_source:
            # Find and retry inflight command
            for seq_num in sorted(self._mixer._inflight_commands.keys(), reverse=True):
                _, msg = self._mixer._inflight_commands[seq_num]
                if MixerProtocol.command_source_pattern(self._output_type, self._id) in msg:
                    self._mixer._reenqueue_inflight_command(seq_num)
                    break
        else:
            self._mixer._clear_latest_inflight_command_by_pattern(
                MixerProtocol.command_source_pattern(self._output_type, self._id)
            )

    def set_volume(self, level):
        """Set volume for this output with validation."""
        if not self._mixer:
            raise RuntimeError(f"{self._type_name} {self._id} has no mixer reference")
        if isinstance(level, str):
            if level.lower() != "mute":
                self._mixer._logger.error(
                    f"Invalid level string '{level}', must be 'mute' or integer 0-62"
                )
                return
        elif isinstance(level, int):
            if not (0 <= level <= 62):
                self._mixer._logger.error(f"Invalid level {level}, must be 0-62")
                return
        else:
            self._mixer._logger.error(f"Invalid level type {type(level)}, must be int or 'mute'")
            return
        self._send_volume(level)

    def _send_volume(self, level):
        """Send volume command with debounce."""
        if not self._mixer:
            return

        level_str = "62" if isinstance(level, str) else str(level)
        expected_level = 62 if isinstance(level, str) else level

        self._mixer._logger.info(
            f"Volume request - {self._type_name}: {self._id} to level: {level} (debounced)"
        )

        # Cancel any pending debounce/confirm tasks
        if self._volume_debounce_task and not self._volume_debounce_task.done():
            self._volume_debounce_task.cancel()
        if self._volume_confirm_task and not self._volume_confirm_task.done():
            self._volume_confirm_task.cancel()

        # Create debounce task
        self._volume_debounce_task = self._mixer._loop.create_task(
            self._debounce_volume(level_str, expected_level)
        )

    async def _debounce_volume(self, level_str: str, expected_level: int):
        """Debounce and send volume command."""
        try:
            await asyncio.sleep(self._mixer._volume_debounce_delay)
            if self._volume_debounce_task != asyncio.current_task():
                return
            command = MixerProtocol.command_set_volume(self._output_type, self._id, level_str)
            self._mixer._enqueue_command(command)
            if self._mixer._command_confirmation:
                self._volume_confirm_task = self._mixer._loop.create_task(
                    self._confirm_volume(expected_level)
                )
            self._volume_debounce_task = None
        except asyncio.CancelledError:
            self._volume_debounce_task = None

    async def _confirm_volume(self, expected_level: int):
        """Confirm volume was set correctly."""
        await asyncio.sleep(0.3)
        self._mixer._enqueue_command(
            MixerProtocol.command_query_volume(self._output_type, self._id),
            PRIORITY_READ,
        )
        await asyncio.sleep(0.5)

        if self.volume != expected_level and self.volume is not None:
            # Retry query once
            self._mixer._enqueue_command(
                MixerProtocol.command_query_volume(self._output_type, self._id),
                PRIORITY_READ,
            )
            await asyncio.sleep(0.4)

        if self.volume == expected_level:
            self._mixer._clear_latest_inflight_command_by_pattern(
                MixerProtocol.command_volume_pattern(self._output_type, self._id)
            )
        else:
            # Find and retry inflight command
            for seq_num in sorted(self._mixer._inflight_commands.keys(), reverse=True):
                _, msg = self._mixer._inflight_commands[seq_num]
                if MixerProtocol.command_volume_pattern(self._output_type, self._id) in msg:
                    self._mixer._reenqueue_inflight_command(seq_num)
                    break


class Zone(Output):
    """Represents a zone in the DCM1 mixer with state and control tracking."""
    _output_type = OutputType.ZONE
    _type_name = "Zone"

    def __init__(self, zone_id: int, name: str, mixer: 'DCM1Mixer' = None):
        super().__init__(zone_id, name, mixer=mixer)
        # EQ debounce and confirmation tasks (one set per parameter)
        self._eq_treble_debounce_task: Optional[Task[Any]] = None
        self._eq_treble_confirm_task: Optional[Task[Any]] = None
        self._eq_mid_debounce_task: Optional[Task[Any]] = None
        self._eq_mid_confirm_task: Optional[Task[Any]] = None
        self._eq_bass_debounce_task: Optional[Task[Any]] = None
        self._eq_bass_confirm_task: Optional[Task[Any]] = None

        # Tracking: Which attributes have been received from device
        self._eq_received: bool = False

    @property
    def eq_received(self) -> bool:
        """Whether EQ status has been received from device."""
        return self._eq_received

    def _validate_eq_level(self, level: int) -> bool:
        """Validate EQ level is in range (0 or even values -14 to +14)."""
        return level == 0 or (-14 <= level <= 14 and level % 2 == 0)

    def set_eq(self, treble: int, mid: int, bass: int):
        """Set all three EQ values (treble, mid, bass) for this zone.
        
        Sends three individual commands. Device responds immediately to each.
        
        Args:
            treble: Treble level (0 or even values -14 to +14)
            mid: Mid level (0 or even values -14 to +14)
            bass: Bass level (0 or even values -14 to +14)
        """
        if not self._mixer:
            raise RuntimeError(f"Zone {self._id} has no mixer reference")
        
        if not (self._validate_eq_level(treble) and
                self._validate_eq_level(mid) and
                self._validate_eq_level(bass)):
            self._mixer._logger.error(
                f"Invalid EQ levels for Zone {self._id}: T={treble}, M={mid}, B={bass} (must be 0 or even values -14 to +14)"
            )
            return
        
        # Send all three commands (device responds to each individually)
        self.set_eq_treble(treble)
        self.set_eq_mid(mid)
        self.set_eq_bass(bass)

    def set_eq_treble(self, level: int):
        """Set EQ treble level for this zone.
        
        Device responds immediately with confirmation.
        
        Args:
            level: Treble level (0 or even values -14 to +14)
        """
        if not self._mixer:
            raise RuntimeError(f"Zone {self._id} has no mixer reference")
        
        if not self._validate_eq_level(level):
            self._mixer._logger.error(f"Invalid treble level {level} for Zone {self._id}")
            return
        
        self._mixer._logger.info(f"EQ Treble request - Zone: {self._id} to {level:+d} (debounced)")
        
        # Cancel any pending debounce/confirm tasks
        if self._eq_treble_debounce_task and not self._eq_treble_debounce_task.done():
            self._eq_treble_debounce_task.cancel()
        if self._eq_treble_confirm_task and not self._eq_treble_confirm_task.done():
            self._eq_treble_confirm_task.cancel()
        
        # Create debounce task
        self._eq_treble_debounce_task = self._mixer._loop.create_task(
            self._debounce_eq_treble(level)
        )

    async def _debounce_eq_treble(self, level: int):
        """Debounce and send EQ treble command."""
        try:
            await asyncio.sleep(self._mixer._volume_debounce_delay)
            if self._eq_treble_debounce_task != asyncio.current_task():
                return
            command = MixerProtocol.command_set_zone_eq_treble(self._id, level)
            self._mixer._enqueue_command(command)
            if self._mixer._command_confirmation:
                self._eq_treble_confirm_task = self._mixer._loop.create_task(
                    self._confirm_eq_treble(level)
                )
            self._eq_treble_debounce_task = None
        except asyncio.CancelledError:
            self._eq_treble_debounce_task = None

    async def _confirm_eq_treble(self, expected_level: int):
        """Confirm EQ treble was set correctly.
        
        Device responds immediately to EQ commands, so no query needed.
        Just wait for response to arrive and clear inflight command.
        """
        await asyncio.sleep(0.3)
        # Device responds immediately with <z#.mu,t=value/>, so just clear inflight
        self._mixer._clear_latest_inflight_command_by_pattern(
            MixerProtocol.command_eq_treble_pattern(self._id)
        )

    def set_eq_mid(self, level: int):
        """Set EQ mid level for this zone.
        
        Device responds immediately with confirmation.
        
        Args:
            level: Mid level (0 or even values -14 to +14)
        """
        if not self._mixer:
            raise RuntimeError(f"Zone {self._id} has no mixer reference")
        
        if not self._validate_eq_level(level):
            self._mixer._logger.error(f"Invalid mid level {level} for Zone {self._id}")
            return
        
        self._mixer._logger.info(f"EQ Mid request - Zone: {self._id} to {level:+d} (debounced)")
        
        # Cancel any pending debounce/confirm tasks
        if self._eq_mid_debounce_task and not self._eq_mid_debounce_task.done():
            self._eq_mid_debounce_task.cancel()
        if self._eq_mid_confirm_task and not self._eq_mid_confirm_task.done():
            self._eq_mid_confirm_task.cancel()
        
        # Create debounce task
        self._eq_mid_debounce_task = self._mixer._loop.create_task(
            self._debounce_eq_mid(level)
        )

    async def _debounce_eq_mid(self, level: int):
        """Debounce and send EQ mid command."""
        try:
            await asyncio.sleep(self._mixer._volume_debounce_delay)
            if self._eq_mid_debounce_task != asyncio.current_task():
                return
            command = MixerProtocol.command_set_zone_eq_mid(self._id, level)
            self._mixer._enqueue_command(command)
            if self._mixer._command_confirmation:
                self._eq_mid_confirm_task = self._mixer._loop.create_task(
                    self._confirm_eq_mid(level)
                )
            self._eq_mid_debounce_task = None
        except asyncio.CancelledError:
            self._eq_mid_debounce_task = None

    async def _confirm_eq_mid(self, expected_level: int):
        """Confirm EQ mid was set correctly.
        
        Device responds immediately to EQ commands, so no query needed.
        Just wait for response to arrive and clear inflight command.
        """
        await asyncio.sleep(0.3)
        # Device responds immediately with <z#.mu,m=value/>, so just clear inflight
        self._mixer._clear_latest_inflight_command_by_pattern(
            MixerProtocol.command_eq_mid_pattern(self._id)
        )

    def set_eq_bass(self, level: int):
        """Set EQ bass level for this zone.
        
        Device responds immediately with confirmation.
        
        Args:
            level: Bass level (0 or even values -14 to +14)
        """
        if not self._mixer:
            raise RuntimeError(f"Zone {self._id} has no mixer reference")
        
        if not self._validate_eq_level(level):
            self._mixer._logger.error(f"Invalid bass level {level} for Zone {self._id}")
            return
        
        self._mixer._logger.info(f"EQ Bass request - Zone: {self._id} to {level:+d} (debounced)")
        
        # Cancel any pending debounce/confirm tasks
        if self._eq_bass_debounce_task and not self._eq_bass_debounce_task.done():
            self._eq_bass_debounce_task.cancel()
        if self._eq_bass_confirm_task and not self._eq_bass_confirm_task.done():
            self._eq_bass_confirm_task.cancel()
        
        # Create debounce task
        self._eq_bass_debounce_task = self._mixer._loop.create_task(
            self._debounce_eq_bass(level)
        )

    async def _debounce_eq_bass(self, level: int):
        """Debounce and send EQ bass command."""
        try:
            await asyncio.sleep(self._mixer._volume_debounce_delay)
            if self._eq_bass_debounce_task != asyncio.current_task():
                return
            command = MixerProtocol.command_set_zone_eq_bass(self._id, level)
            self._mixer._enqueue_command(command)
            if self._mixer._command_confirmation:
                self._eq_bass_confirm_task = self._mixer._loop.create_task(
                    self._confirm_eq_bass(level)
                )
            self._eq_bass_debounce_task = None
        except asyncio.CancelledError:
            self._eq_bass_debounce_task = None

    async def _confirm_eq_bass(self, expected_level: int):
        """Confirm EQ bass was set correctly.
        
        Device responds immediately to EQ commands, so no query needed.
        Just wait for response to arrive and clear inflight command.
        """
        await asyncio.sleep(0.3)
        # Device responds immediately with <z#.mu,b=value/>, so just clear inflight
        self._mixer._clear_latest_inflight_command_by_pattern(
            MixerProtocol.command_eq_bass_pattern(self._id)
        )

    def all_initial_data_received(self) -> bool:
        """Whether all initial state data has been received for this zone."""
        return super().all_initial_data_received() and self._eq_received


class Group(Output):
    """Represents a group in the DCM1 mixer with state and control tracking."""
    _output_type = OutputType.GROUP
    _type_name = "Group"

    def __init__(
        self,
        group_id: int,
        name: str,
        enabled: bool = False,
        zones: list[int] = None,
        mixer: 'DCM1Mixer' = None,
    ):
        super().__init__(group_id, name, mixer=mixer)
        self._enabled = enabled
        self._zones = zones if zones else []

        # Tracking: Which attributes have been received from device
        self._status_received: bool = False

    @property
    def enabled(self) -> bool:
        """Whether group is enabled."""
        return self._enabled

    @property
    def zones(self) -> list[int]:
        """Zone IDs in this group."""
        return self._zones

    @property
    def status_received(self) -> bool:
        """Whether group status has been received from device."""
        return self._status_received

    def all_initial_data_received(self) -> bool:
        """Whether all initial state data has been received for this group."""
        return super().all_initial_data_received() and self.status_received



class MixerListener(MixerResponseListener):
    """Listener that tracks mixer state updates and reception timestamps."""

    def __init__(self, mixer):
        self._mixer = mixer

    def data_received(self, data: bytes):
        """Update last-receive timestamp on any inbound data."""
        self._mixer._last_receive_timestamp = time.time()
    
    def connected(self):
        """Forward connection event to mixer."""
        self._mixer._on_connected()
    
    def disconnected(self):
        """Forward disconnection event to mixer."""
        self._mixer._on_disconnected()
        
    def source_label_received(self, source_id: int, label: str):
        """Update the source label when received from device."""
        source = self._mixer.sources_by_id.get(source_id)
        if source:
            # Remove old name from sources_by_name
            old_name = source.name
            if old_name in self._mixer.sources_by_name:
                del self._mixer.sources_by_name[old_name]
            # Update source name and mark label as received
            source._name = label
            source._label_received = True
            self._mixer.sources_by_name[label] = source
    
    def zone_label_received(self, zone_id: int, label: str):
        """Update the zone label when received from device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            # Remove old name from zones_by_name
            old_name = zone.name
            if old_name in self._mixer.zones_by_name:
                del self._mixer.zones_by_name[old_name]
            # Update zone name and mark label as received
            zone._name = label
            zone._label_received = True
            self._mixer.zones_by_name[label] = zone

    def zone_line_inputs_received(self, zone_id: int, line_inputs: dict[int, bool]):
        """Track when a zone's line input data is received."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._line_inputs = line_inputs
            zone._line_inputs_received = True

    def zone_source_received(self, zone_id: int, source_id: int):
        """Track when a zone's source is received from the device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._update_source_unless_debouncing(source_id)
        self._mixer._clear_pending_heartbeat_query(
            MixerProtocol.command_query_source(OutputType.ZONE, zone_id)
        )

    def zone_volume_level_received(self, zone_id: int, level):
        """Track when a zone's volume is received from the device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._update_volume_unless_debouncing(level)
        self._mixer._clear_pending_heartbeat_query(
            MixerProtocol.command_query_volume(OutputType.ZONE, zone_id)
        )

    def zone_eq_received(self, zone_id: int, treble: int, mid: int, bass: int):
        """Track when a zone's EQ values are received from the device (combined query response)."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._eq_treble = treble
            zone._eq_mid = mid
            zone._eq_bass = bass
            zone._eq_received = True

    def zone_eq_treble_received(self, zone_id: int, treble: int):
        """Track when a zone's EQ treble value is received from the device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._eq_treble = treble

    def zone_eq_mid_received(self, zone_id: int, mid: int):
        """Track when a zone's EQ mid value is received from the device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._eq_mid = mid

    def zone_eq_bass_received(self, zone_id: int, bass: int):
        """Track when a zone's EQ bass value is received from the device."""
        zone = self._mixer.zones_by_id.get(zone_id)
        if zone:
            zone._eq_bass = bass

    def group_label_received(self, group_id: int, label: str):
        """Update the group label when received from device."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            # Remove old name from groups_by_name
            old_name = group.name
            if old_name in self._mixer.groups_by_name:
                del self._mixer.groups_by_name[old_name]
            # Update group name and mark label as received
            group._name = label
            group._label_received = True
            self._mixer.groups_by_name[label] = group
    
    def group_status_received(self, group_id: int, enabled: bool, zones: list[int]):
        """Update the group status when received from device."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            group._enabled = enabled
            group._zones = zones
            group._status_received = True
    
    def group_volume_level_received(self, group_id: int, level):
        """Track when a group's volume is received."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            group._update_volume_unless_debouncing(level)
        self._mixer._clear_pending_heartbeat_query(
            MixerProtocol.command_query_volume(OutputType.GROUP, group_id)
        )
    
    def group_line_inputs_received(self, group_id: int, line_inputs: dict[int, bool]):
        """Track when a group's line input data is received."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            group._line_inputs = line_inputs
            group._line_inputs_received = True
    
    def group_source_received(self, group_id: int, source_id: int):
        """Track when a group's source is received from the device."""
        group = self._mixer.groups_by_id.get(group_id)
        if group:
            group._update_source_unless_debouncing(source_id)
        self._mixer._clear_pending_heartbeat_query(
            MixerProtocol.command_query_source(OutputType.GROUP, group_id)
        )


class DCM1Mixer:
    """High-level DCM1 mixer control with queue, heartbeat, and reconnection management.
    
    This class:
    - Creates and manages MixerProtocol instance
    - Manages command queue, worker, heartbeat, watchdog
    - Tracks inflight commands for recovery on reconnection
    - Debounces volume and source changes
    - Confirms commands by querying device
    - Handles reconnection logic
    - Manages domain objects (Zone, Source, Group)
    
    Connection lifecycle events are forwarded from MixerListener.
    """

    def __init__(self, hostname, port, enable_heartbeat=True, heartbeat_time=10,
                 reconnect_time=10, command_confirmation=True):
        """Initialize mixer.
        
        Args:
            hostname: DCM1 device hostname or IP
            port: TCP port (usually 10001)
            enable_heartbeat: Whether to enable periodic status polling
            heartbeat_time: Seconds between heartbeat polls
            reconnect_time: Seconds to wait between reconnection attempts
            command_confirmation: Whether to auto-confirm commands
        """
        self._hostname: str = hostname
        self._port = port
        self._enable_heartbeat = enable_heartbeat
        self._heartbeat_time = heartbeat_time
        self._reconnect_time = reconnect_time
        self._command_confirmation = command_confirmation
        
        self._logger = logging.getLogger(__name__)
        self._loop = asyncio.get_event_loop()

        # DCM1 has 8 zones, 8 line sources, and 4 groups (hardcoded)
        self._zone_count: int = 8
        self._source_count: int = 8
        self._group_count: int = 4

        # Retry inflight commands on confirmation timeout
        self._retry_on_confirmation_timeout: bool = True
        # If connection is down for longer than this, clear inflight commands on reconnect
        # because users may have used the front panel and our commands are now stale
        self._clear_queue_after_reconnection_delay_seconds: float = 60.0

        self._max_retries: int = 1  # Max retry attempts per command

        # Minimum delay between commands to avoid MCU serial port clashes (in seconds)
        self._min_send_delay_seconds: float = 0.1

        self._volume_debounce_delay: float = 0.5  # Wait 500ms after last request before sending

        # Domain objects
        self.zones_by_id: dict[int, Zone] = {}
        self.zones_by_name: dict[str, Zone] = {}
        self.sources_by_id: dict[int, Source] = {}
        self.sources_by_name: dict[str, Source] = {}
        self.groups_by_id: dict[int, Group] = {}
        self.groups_by_name: dict[str, Group] = {}

        # Connection state
        self._connected = False
        self._reconnect = True
        self._connection_lost_timestamp: Optional[float] = None

        # Tasks
        self._heartbeat_task: Optional[Task[Any]] = None
        self._command_worker_task: Optional[Task[Any]] = None
        self._connection_watchdog_task: Optional[Task[Any]] = None

        # Track last send time to avoid clashes between commands
        self._last_send_timestamp: float = 0.0
        # Track last received data time to detect silent/stalled connections
        self._last_receive_timestamp: float = time.time()

        # Priority queue to serialize commands (user commands jump ahead of heartbeat)
        self._command_queue: PriorityQueue = PriorityQueue()
        self._queued_commands_set: set[str] = set()  # Deduplication set for queued commands
        self._command_sequence_number: int = 0  # Sequence number for FIFO order within same priority

        # Track pending heartbeat queries to prevent duplicate polling
        self._pending_heartbeat_queries = set()

        # Track user commands that have been sent but not yet confirmed (inflight)
        # Format: command_sequence_number -> (priority, message)
        self._inflight_commands: dict[int, tuple[int, str]] = {}
        # Track retry attempts per command
        self._inflight_command_reenqueue_counts: dict[int, int] = {}

        # Create multiplexing listener for external listeners
        self._multiplex_callback = MultiplexingListener()
        
        # Register internal listener to update mixer state and handle connection lifecycle
        self._mixer_listener = MixerListener(self)
        self._multiplex_callback.register_listener(self._mixer_listener)
        
        # Create protocol instance
        self._protocol = MixerProtocol(self._multiplex_callback)

    # ========== Connection lifecycle handlers ==========
    
    def _on_connected(self):
        """Called by MixerListener when connection is established."""
        self._logger.info("Mixer connected")
        self._connected = True
        self._last_receive_timestamp = time.time()
        
        # Cancel any existing tasks before creating new ones
        if self._command_worker_task is not None and not self._command_worker_task.done():
            self._command_worker_task.cancel()
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        
        # Check if connection was down for a long time
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
                
                # Cancel and clear all mid-flight debounce/confirm tasks in zones
                for zone in self.zones_by_id.values():
                    if zone._source_debounce_task and not zone._source_debounce_task.done():
                        zone._source_debounce_task.cancel()
                    zone._source_debounce_task = None
                    if zone._volume_debounce_task and not zone._volume_debounce_task.done():
                        zone._volume_debounce_task.cancel()
                    zone._volume_debounce_task = None
                    if zone._eq_treble_debounce_task and not zone._eq_treble_debounce_task.done():
                        zone._eq_treble_debounce_task.cancel()
                    zone._eq_treble_debounce_task = None
                    if zone._eq_mid_debounce_task and not zone._eq_mid_debounce_task.done():
                        zone._eq_mid_debounce_task.cancel()
                    zone._eq_mid_debounce_task = None
                    if zone._eq_bass_debounce_task and not zone._eq_bass_debounce_task.done():
                        zone._eq_bass_debounce_task.cancel()
                    zone._eq_bass_debounce_task = None
                    if zone._source_confirm_task and not zone._source_confirm_task.done():
                        zone._source_confirm_task.cancel()
                    zone._source_confirm_task = None
                    if zone._volume_confirm_task and not zone._volume_confirm_task.done():
                        zone._volume_confirm_task.cancel()
                    zone._volume_confirm_task = None
                    if zone._eq_treble_confirm_task and not zone._eq_treble_confirm_task.done():
                        zone._eq_treble_confirm_task.cancel()
                    zone._eq_treble_confirm_task = None
                    if zone._eq_mid_confirm_task and not zone._eq_mid_confirm_task.done():
                        zone._eq_mid_confirm_task.cancel()
                    zone._eq_mid_confirm_task = None
                    if zone._eq_bass_confirm_task and not zone._eq_bass_confirm_task.done():
                        zone._eq_bass_confirm_task.cancel()
                    zone._eq_bass_confirm_task = None
                
                # Cancel and clear all mid-flight debounce/confirm tasks in groups
                for group in self.groups_by_id.values():
                    if group._source_debounce_task and not group._source_debounce_task.done():
                        group._source_debounce_task.cancel()
                    group._source_debounce_task = None
                    if group._volume_debounce_task and not group._volume_debounce_task.done():
                        group._volume_debounce_task.cancel()
                    group._volume_debounce_task = None
                    if group._source_confirm_task and not group._source_confirm_task.done():
                        group._source_confirm_task.cancel()
                    group._source_confirm_task = None
                    if group._volume_confirm_task and not group._volume_confirm_task.done():
                        group._volume_confirm_task.cancel()
                    group._volume_confirm_task = None
                                
            self._connection_lost_timestamp = None
        
        # Re-queue any inflight user commands that were sent but not confirmed before disconnect
        if reenqueue_inflight_commands and self._inflight_commands:
            self._logger.warning(f"Re-queueing {len(self._inflight_commands)} inflight user commands after reconnection")
            # Clear retry counts for fresh quota on new connection
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
            self._logger.info("Heartbeat task started (interval=%ss)", self._heartbeat_time)
        
        # Start connection watchdog to detect silent disconnections
        self._connection_watchdog_task = self._loop.create_task(self._connection_watchdog())
        
        # Query configuration
        self._enqueue_source_label_query_commands()
        self._enqueue_zone_label_query_commands()
        self._enqueue_zone_line_input_enable_query_commands()
        self._enqueue_group_status_query_commands()
        self._enqueue_group_label_query_commands()
        self._enqueue_group_line_input_enable_query_commands()

        # Query operational status
        self._enqueue_zone_source_query_commands()
        self._enqueue_zone_volume_level_query_commands()
        self._enqueue_zone_eq_query_commands()
        self._enqueue_group_source_query_commands()
        self._enqueue_group_volume_level_query_commands()

    def _on_disconnected(self):
        """Called by MixerListener when connection is lost."""
        self._handle_connection_broken()

    # ========== Public API ==========

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
        """Get which line inputs are enabled for a zone."""
        zone = self._get_zone_by_id(zone_id)
        return zone.line_inputs.copy() if zone else {}
    
    def get_zone_source(self, zone_id: int) -> Optional[int]:
        """Get the current source ID for a zone."""
        zone = self._get_zone_by_id(zone_id)
        return zone.source if zone else None
    
    def get_zone_volume_level(self, zone_id: int):
        """Get the volume level for a zone."""
        zone = self._get_zone_by_id(zone_id)
        return zone.volume if zone else None

    def get_group_source(self, group_id: int) -> Optional[int]:
        """Get the current source ID for a group."""
        group = self._get_group_by_id(group_id)
        return group.source if group else None
    
    def get_group_enabled_line_inputs(self, group_id: int) -> dict[int, bool]:
        """Get which line inputs are enabled for a group."""
        group = self._get_group_by_id(group_id)
        return group.line_inputs.copy() if group else {}

    def get_group_volume_level(self, group_id: int):
        """Get the volume level for a group."""
        group = self._get_group_by_id(group_id)
        return group.volume if group else None

    def register_listener(self, listener):
        """Register external listener for mixer events."""
        self._multiplex_callback.register_listener(listener)

    def unregister_listener(self, listener):
        """Unregister external listener."""
        self._multiplex_callback.unregister_listener(listener)

    async def async_connect(self):
        """Connect to the DCM1 mixer."""
        # Initialize zones, sources, and groups
        for i in range(1, self._zone_count + 1):
            zone = Zone(i, f"Zone {i}", mixer=self)
            self.zones_by_id[i] = zone
            self.zones_by_name[zone.name] = zone
            
            source = Source(i, f"Source {i}")
            self.sources_by_id[i] = source
            self.sources_by_name[source.name] = source
        
        # Initialize groups
        for i in range(1, self._group_count + 1):
            group = Group(i, f"Group {i}", mixer=self)
            self.groups_by_id[i] = group
            self.groups_by_name[group.name] = group
        
        # Create connection to device
        await self._loop.create_connection(
            lambda: self._protocol, host=self._hostname, port=self._port
        )

    def close(self):
        """Close the connection and stop reconnection attempts."""
        self._reconnect = False
        if self._protocol._transport:
            self._protocol._transport.close()

    def set_zone_source(self, zone_id: int, source_id: int):
        """Set a zone to use a specific source."""
        zone = self._get_zone_by_id(zone_id)
        if zone:
            zone.set_source(source_id)  # Zone validates source_id

    def set_zone_volume(self, zone_id: int, level):
        """Set volume level for a zone."""
        zone = self._get_zone_by_id(zone_id)
        if zone:
            zone.set_volume(level)  # Zone validates level

    def set_zone_eq(self, zone_id: int, treble: int, mid: int, bass: int):
        """Set EQ levels for a zone."""
        zone = self._get_zone_by_id(zone_id)
        if zone:
            zone.set_eq(treble, mid, bass)  # Zone validates levels

    def set_zone_eq_treble(self, zone_id: int, level: int):
        """Set EQ treble level for a zone."""
        zone = self._get_zone_by_id(zone_id)
        if zone:
            zone.set_eq_treble(level)  # Zone validates level

    def set_zone_eq_mid(self, zone_id: int, level: int):
        """Set EQ mid level for a zone."""
        zone = self._get_zone_by_id(zone_id)
        if zone:
            zone.set_eq_mid(level)  # Zone validates level

    def set_zone_eq_bass(self, zone_id: int, level: int):
        """Set EQ bass level for a zone."""
        zone = self._get_zone_by_id(zone_id)
        if zone:
            zone.set_eq_bass(level)  # Zone validates level

    def set_group_source(self, group_id: int, source_id: int):
        """Set a group to use a specific source."""
        group = self._get_group_by_id(group_id)
        if group:
            group.set_source(source_id)  # Group validates source_id

    def set_group_volume(self, group_id: int, level):
        """Set volume level for a group."""
        group = self._get_group_by_id(group_id)
        if group:
            group.set_volume(level)  # Group validates level

    # ========== Helpers  ==========

    def _get_zone_by_id(self, zone_id: int) -> Optional[Zone]:
        """Validate zone_id and return Zone."""
        if not (1 <= zone_id <= self._zone_count):
            self._logger.error(f"Invalid zone_id {zone_id}, must be 1-{self._zone_count}")
            return None
        return self.zones_by_id.get(zone_id)

    def _get_group_by_id(self, group_id: int) -> Optional[Group]:
        """Validate group_id and return Group."""
        if not (1 <= group_id <= self._group_count):
            self._logger.error(f"Invalid group_id {group_id}, must be 1-{self._group_count}")
            return None
        return self.groups_by_id.get(group_id)    

    # ========== Queue management ==========

    def _enqueue_command(self, message, priority: int = PRIORITY_WRITE):
        """Queue a command to be sent via the persistent connection."""
        # Deduplication: Only queue if not already present
        msg_key = message.strip()
        if msg_key in self._queued_commands_set:
            self._logger.info(f"QUEUE: Duplicate command not queued: {msg_key}")
            return
        self._queued_commands_set.add(msg_key)
        # Safety: make sure the command worker is alive
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

    def _clear_pending_heartbeat_query(self, query: str) -> None:
        """Clear a pending heartbeat query after its response is received."""
        if query in self._pending_heartbeat_queries:
            self._pending_heartbeat_queries.remove(query)

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
                    
                    if self._connected and self._protocol._transport:
                        self._logger.info(f"SEND: Command #{sequence_number} (priority={priority}): {message.encode()}")
                        self._protocol.write(message)
                        self._logger.info(f"SEND: Command #{sequence_number} written to transport successfully")
                    else:
                        self._logger.error(f"SEND FAILED: Command #{sequence_number} - not connected (connected={self._connected})")
                        # Connection is definitely down - trigger immediate reconnection
                        if self._connected:
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
                break

    # ========== Heartbeat ==========

    async def _heartbeat(self):
        """Periodically query zone and group status to sync with physical panel changes."""
        while True:
            await asyncio.sleep(self._heartbeat_time)
            
            # Query source and volume for all zones
            for zone_id in range(1, self._zone_count + 1):
                # Query source with deduplication
                zone_source_query = MixerProtocol.command_query_source(OutputType.ZONE, zone_id)
                if zone_source_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(zone_source_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (zone_source_query, None)))

                # Query volume level with deduplication
                zone_volume_query = MixerProtocol.command_query_volume(OutputType.ZONE, zone_id)
                if zone_volume_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(zone_volume_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (zone_volume_query, None)))
                            
            # Query source and volume for all groups
            for group_id in range(1, self._group_count + 1):                
                # Query group source with deduplication
                group_source_query = MixerProtocol.command_query_source(OutputType.GROUP, group_id)
                if group_source_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(group_source_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (group_source_query, None)))

                # Query group volume level with deduplication
                group_volume_query = MixerProtocol.command_query_volume(OutputType.GROUP, group_id)
                if group_volume_query not in self._pending_heartbeat_queries:
                    self._command_sequence_number += 1
                    self._pending_heartbeat_queries.add(group_volume_query)
                    await self._command_queue.put((PRIORITY_READ, self._command_sequence_number, (group_volume_query, None)))

    # ========== Connection management ==========

    async def _connection_watchdog(self):
        """Monitor connection health and trigger reconnection if connection dies silently."""
        idle_timeout_seconds = 30.0
        check_interval_seconds = 5.0
        
        while self._reconnect:
            try:
                await asyncio.sleep(check_interval_seconds)
                
                if self._connected:
                    elapsed_seconds_since_data_last_received = time.time() - self._last_receive_timestamp
                    if elapsed_seconds_since_data_last_received > idle_timeout_seconds:
                        self._logger.error(
                            f"[WATCHDOG FALLBACK] Connection appears dead: no data received for {elapsed_seconds_since_data_last_received:.1f}s, reconnecting..."
                        )
                        self._connected = False
                        if self._protocol._transport:
                            self._protocol._transport.close()
                        # Schedule reconnection
                        await self._wait_to_reconnect()
                        return
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

    def _handle_connection_broken(self):
        """Handle connection loss - called either by protocol callback or by send failure detection."""
        self._connected = False
        self._connection_lost_timestamp = time.time()
        
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._command_worker_task is not None:
            self._command_worker_task.cancel()
        
        # Cancel confirmation tasks in zone and group objects
        for zone in self.zones_by_id.values():
            if zone._source_confirm_task and not zone._source_confirm_task.done():
                zone._source_confirm_task.cancel()
            if zone._volume_confirm_task and not zone._volume_confirm_task.done():
                zone._volume_confirm_task.cancel()
            if zone._eq_treble_confirm_task and not zone._eq_treble_confirm_task.done():
                zone._eq_treble_confirm_task.cancel()
            if zone._eq_mid_confirm_task and not zone._eq_mid_confirm_task.done():
                zone._eq_mid_confirm_task.cancel()
            if zone._eq_bass_confirm_task and not zone._eq_bass_confirm_task.done():
                zone._eq_bass_confirm_task.cancel()
        
        for group in self.groups_by_id.values():
            if group._source_confirm_task and not group._source_confirm_task.done():
                group._source_confirm_task.cancel()
            if group._volume_confirm_task and not group._volume_confirm_task.done():
                group._volume_confirm_task.cancel()
        
        if self._connection_watchdog_task is not None:
            self._connection_watchdog_task.cancel()
        
        disconnected_message = f"Disconnected from {self._hostname}"
        if self._reconnect:
            disconnected_message = disconnected_message + f", will try to reconnect in {self._reconnect_time} seconds"
            self._logger.error(disconnected_message)
        else:
            disconnected_message = disconnected_message + ", not reconnecting"
            self._logger.info(disconnected_message)
        
        if self._reconnect:
            self._loop.create_task(self._wait_to_reconnect())

    # ========== Inflight command management ==========

    def _clear_latest_inflight_command_by_pattern(self, pattern: str):
        """Clear the most recent inflight command matching a command pattern."""
        for sequence_number in sorted(self._inflight_commands.keys(), reverse=True):
            _, message = self._inflight_commands[sequence_number]
            if pattern in message:
                self._logger.debug(f"Clearing confirmed inflight send: Command #{sequence_number} matching {pattern}")
                del self._inflight_commands[sequence_number]
                return

    def _reenqueue_inflight_command(self, sequence_number: int) -> bool:
        """Attempt to retry an inflight command that failed confirmation."""
        if sequence_number not in self._inflight_commands:
            self._logger.warning(f"Cannot reenqueue command: #{sequence_number} not in inflight_commands")
            return False
        
        if not self._connected or self._protocol._transport is None:
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

    def _debounce_inflight_commands(self, sequence_number: int, message: str):
        """Remove older inflight commands for the same zone/group AND command type."""
        # Extract zone/group and command type
        zone_vol_id = MixerProtocol.command_volume_target_id(OutputType.ZONE, message)
        zone_src_id = MixerProtocol.command_source_target_id(OutputType.ZONE, message)
        group_vol_id = MixerProtocol.command_volume_target_id(OutputType.GROUP, message)
        group_src_id = MixerProtocol.command_source_target_id(OutputType.GROUP, message)
        
        if zone_src_id is not None:
            pattern = MixerProtocol.command_source_pattern(OutputType.ZONE, zone_src_id)
        elif zone_vol_id is not None:
            pattern = MixerProtocol.command_volume_pattern(OutputType.ZONE, zone_vol_id)
        elif group_src_id is not None:
            pattern = MixerProtocol.command_source_pattern(OutputType.GROUP, group_src_id)
        elif group_vol_id is not None:
            pattern = MixerProtocol.command_volume_pattern(OutputType.GROUP, group_vol_id)
        else:
            return
        
        # Remove all older inflight commands matching the EXACT zone/group AND command type
        to_remove = []
        for old_sequence_number in sorted(self._inflight_commands.keys()):
            if old_sequence_number >= sequence_number:
                continue
            _, old_message = self._inflight_commands[old_sequence_number]
            if pattern in old_message:
                to_remove.append(old_sequence_number)
                self._logger.debug(f"Removing superseded inflight command: #{old_sequence_number} (newer #{sequence_number} replaces it for {pattern})")
        
        for old_sequence_number in to_remove:
            del self._inflight_commands[old_sequence_number]

    # ========== Query commands ==========

    def _enqueue_zone_source_query_commands(self):
        self._logger.info(f"Sending status query messages for all zones")
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_source(OutputType.ZONE, zone_id),
                PRIORITY_READ,
            )

    def _enqueue_zone_label_query_commands(self):
        self._logger.info(f"Querying zone labels")
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_label(OutputType.ZONE, zone_id),
                PRIORITY_READ,
            )

    def _enqueue_source_label_query_commands(self):
        self._logger.info(f"Querying source labels")
        for source_id in range(1, self._source_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_source_label(source_id),
                PRIORITY_READ,
            )

    def _enqueue_zone_volume_level_query_commands(self):
        self._logger.info(f"Querying zone volume levels")
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_volume(OutputType.ZONE, zone_id),
                PRIORITY_READ,
            )

    def _enqueue_zone_eq_query_commands(self):
        """Query EQ settings for all zones."""
        self._logger.info("Querying zone EQ settings")
        for zone_id in range(1, self._zone_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_zone_eq(zone_id),
                PRIORITY_READ,
            )

    def _enqueue_zone_line_input_enable_query_commands(self):
        """Query which line inputs are enabled for all zones (1-8)."""
        self._logger.info("Querying line input enables for all zones")
        for zone_id in range(1, self._zone_count + 1):
            for line_id in range(1, self._source_count + 1):
                self._enqueue_command(
                    MixerProtocol.command_query_line_input(OutputType.ZONE, zone_id, line_id),
                    PRIORITY_READ,
                )

    def _enqueue_group_line_input_enable_query_commands(self):
        """Query which line inputs are enabled for all groups (1-4)."""
        self._logger.info("Querying line input enables for all groups")
        for group_id in range(1, self._group_count + 1):
            for line_id in range(1, self._source_count + 1):
                self._enqueue_command(
                    MixerProtocol.command_query_line_input(OutputType.GROUP, group_id, line_id),
                    PRIORITY_READ,
                )

    def _enqueue_group_status_query_commands(self):
        """Query group status for all groups."""
        self._logger.info("Querying group status for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_group_status(group_id),
                PRIORITY_READ,
            )

    def _enqueue_group_label_query_commands(self):
        """Query group labels for all groups."""
        self._logger.info("Querying group labels for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_label(OutputType.GROUP, group_id),
                PRIORITY_READ,
            )

    def _enqueue_group_source_query_commands(self):
        """Query group source for all groups."""
        self._logger.info("Querying group source for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_source(OutputType.GROUP, group_id),
                PRIORITY_READ,
            )

    def _enqueue_group_volume_level_query_commands(self):
        """Query group volume level for all groups."""
        self._logger.info("Querying group volume levels for all groups")
        for group_id in range(1, self._group_count + 1):
            self._enqueue_command(
                MixerProtocol.command_query_volume(OutputType.GROUP, group_id),
                PRIORITY_READ,
            )

    # ========== Helper methods (for backward compatibility with old API) ==========

    def query_status(self):
        """Query all relevant mixer state from the device."""
        # Query source labels
        self._enqueue_source_label_query_commands()
        # Query all zones
        self._enqueue_zone_label_query_commands()
        self._enqueue_zone_source_query_commands()
        self._enqueue_zone_line_input_enable_query_commands()
        self._enqueue_zone_volume_level_query_commands()
        self._enqueue_zone_eq_query_commands()
        # Query all groups
        self._enqueue_group_status_query_commands()
        self._enqueue_group_label_query_commands()
        self._enqueue_group_line_input_enable_query_commands()
        self._enqueue_group_source_query_commands()
        self._enqueue_group_volume_level_query_commands()

    async def wait_for_source_labels(self, timeout: float = 6.0):
        """Wait for all source labels to be received from the device."""
        start_time = self._loop.time()
        expected_source_ids = set(self.sources_by_id.keys())
        while self._loop.time() - start_time < timeout:
            all_received = all(self.sources_by_id[sid].label_received for sid in expected_source_ids)
            if all_received:
                return True
            await asyncio.sleep(0.1)
        missing_sources = {sid for sid in expected_source_ids if not self.sources_by_id[sid].label_received}
        if missing_sources:
            print(f"Warning: Timeout waiting for source labels: {missing_sources}")
        return False

    async def wait_for_zone_data(self, timeout: float = 10.0):
        """Wait for all zone data (labels, line inputs, volume, and sources) to be received."""
        start_time = self._loop.time()
        expected_zone_ids = set(self.zones_by_id.keys())
        while self._loop.time() - start_time < timeout:
            all_received = True
            for zid in expected_zone_ids:
                zone = self.zones_by_id[zid]
                if not zone.all_initial_data_received():
                    all_received = False
                    break
            if all_received:
                return True
            await asyncio.sleep(0.1)
        # Timeout - log what we're missing
        missing_labels = {zid for zid in expected_zone_ids if not self.zones_by_id[zid].label_received}
        missing_line_inputs = {zid for zid in expected_zone_ids if not self.zones_by_id[zid].line_inputs_received}
        missing_volumes = {zid for zid in expected_zone_ids if not self.zones_by_id[zid].volume_received}
        missing_sources = {zid for zid in expected_zone_ids if not self.zones_by_id[zid].source_received}
        missing_eqs = {zid for zid in expected_zone_ids if not self.zones_by_id[zid].eq_received}

        if missing_labels:
            print(f"Warning: Timeout waiting for zone labels: {missing_labels}")
        if missing_line_inputs:
            print(f"Warning: Timeout waiting for zone line inputs: {missing_line_inputs}")
        if missing_volumes:
            print(f"Warning: Timeout waiting for zone volumes: {missing_volumes}")
        if missing_sources:
            print(f"Warning: Timeout waiting for zone sources: {missing_sources}")
        if missing_eqs:
            print(f"Warning: Timeout waiting for zone eqs: {missing_eqs}")
        return False

    async def wait_for_group_data(self, timeout: float = 10.0):
        """Wait for all group data to be received from the device, including sources."""
        start_time = self._loop.time()
        expected_group_ids = set(self.groups_by_id.keys())
        while self._loop.time() - start_time < timeout:
            all_received = True
            for gid in expected_group_ids:
                group = self.groups_by_id[gid]
                if not group.all_initial_data_received():
                    all_received = False
                    break
            if all_received:
                return True
            await asyncio.sleep(0.1)
        # Timeout - log what we're missing
        missing_labels = {gid for gid in expected_group_ids if not self.groups_by_id[gid].label_received}
        missing_status = {gid for gid in expected_group_ids if not self.groups_by_id[gid].status_received}
        missing_volume = {gid for gid in expected_group_ids if not self.groups_by_id[gid].volume_received}
        missing_line_inputs = {gid for gid in expected_group_ids if not self.groups_by_id[gid].line_inputs_received}
        missing_sources = {gid for gid in expected_group_ids if not self.groups_by_id[gid].source_received}
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
