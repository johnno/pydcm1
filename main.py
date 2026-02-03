"""
Main command-line interface for pydcm1.

This script provides a CLI to interact with Cloud DCM1 Zone Mixer.
"""

import argparse
import asyncio
import logging

from pydcm1.mixer import DCM1Mixer


async def show_status(hostname: str, port: int):
    """Query and display the status of all zones."""
    print(f"Connecting to DCM1 at {hostname}:{port}...")
    
    mixer = DCM1Mixer(hostname, port, enable_heartbeat=False)
    await mixer.async_connect()
    
    # Wait for all initial protocol queries to be sent before displaying status
    print("Waiting for all mixer commands to be sent...")
    while True:
        if mixer._command_queue.empty():
            break
        await asyncio.sleep(0.1)
    
    print("\nZone Status:")
    print("-" * 120)
    
    for zone_id in sorted(mixer.zones_by_id.keys()):
        zone = mixer.zones_by_id[zone_id]
        enabled_inputs = mixer.get_zone_enabled_line_inputs(zone_id)
        source_id = mixer.get_zone_source(zone_id)
        volume = mixer.get_zone_volume_level(zone_id)

        # Format volume display
        if volume == "mute":
            volume_str = "MUTE"
        elif volume is not None:
            db_value = -volume
            volume_str = f"{db_value}dB"
        else:
            volume_str = "unknown"

        # Format enabled inputs
        if enabled_inputs:
            enabled_list = [str(line_id) for line_id, enabled in sorted(enabled_inputs.items()) if enabled]
            inputs_str = ",".join(enabled_list) if enabled_list else "none"
        else:
            inputs_str = "querying..."

        # Format EQ display
        eq_parts = []
        if zone.eq_treble is not None:
            eq_parts.append(f"T:{zone.eq_treble:+d}")
        if zone.eq_mid is not None:
            eq_parts.append(f"M:{zone.eq_mid:+d}")
        if zone.eq_bass is not None:
            eq_parts.append(f"B:{zone.eq_bass:+d}")
        eq_str = " ".join(eq_parts) if eq_parts else "unknown"

        if source_id:
            source = mixer.sources_by_id.get(source_id)
            source_name = source.name if source else f"Source {source_id}"
            zone_label = f"Zone {zone_id} ({zone.name}):"
            print(f"{zone_label:30s} {source_id} - {source_name:20s} | Vol: {volume_str:10s} | EQ: {eq_str:15s} | Inputs: {inputs_str}")
        else:
            zone_label = f"Zone {zone_id} ({zone.name}):"
            print(f"{zone_label:30s} {'OFF':24s} | Vol: {volume_str:10s} | EQ: {eq_str:15s} | Inputs: {inputs_str}")
    
    print("-" * 120)
    
    # Display groups
    print("\nGroup Status:")
    print("-" * 120)
    
    for group_id in sorted(mixer.groups_by_id.keys()):
        group = mixer.groups_by_id[group_id]
        status = "ENABLED " if group.enabled else "DISABLED"
        zones_str = ",".join(str(z) for z in group.zones) if group.zones else "none"
        
        # Get volume for the group
        volume = mixer.get_group_volume_level(group_id)
        if volume == "mute":
            volume_str = "MUTE"
        elif volume is not None:
            db_value = -volume
            volume_str = f"{db_value}dB"
        else:
            volume_str = "unknown"
        
        # Get line inputs for the group
        enabled_inputs = mixer.get_group_enabled_line_inputs(group_id)
        if enabled_inputs:
            enabled_list = [str(line_id) for line_id, enabled in sorted(enabled_inputs.items()) if enabled]
            inputs_str = ",".join(enabled_list) if enabled_list else "none"
        else:
            inputs_str = "querying..."
        
        group_label = f"Group {group_id} ({group.name}):"
        print(f"{group_label:30s} [{status:8s}] | Vol: {volume_str:10s} | Zones: {zones_str:10s} | Inputs: {inputs_str}")
    
    print("-" * 120)
    
    mixer.close()


async def set_zone_source(hostname: str, port: int, zone_id: int, source_id: int):
    """Set a zone to a specific source."""
    print(f"Connecting to DCM1 at {hostname}:{port}...")
    
    mixer = DCM1Mixer(hostname, port)
    await mixer.async_connect()
    
    # Wait a bit for connection to establish
    await asyncio.sleep(1)
    
    print(f"Setting Zone {zone_id} to Source {source_id}...")
    mixer.set_zone_source(zone_id, source_id)
    
    # Wait for command to be processed and sent
    await asyncio.sleep(3)
    
    mixer.close()
    print("Done")


async def set_zone_volume_level(hostname: str, port: int, zone_id: int, level):
    """Set volume level for a zone."""
    print(f"Connecting to DCM1 at {hostname}:{port}...")
    
    mixer = DCM1Mixer(hostname, port)
    await mixer.async_connect()
    
    # Wait a bit for connection to establish
    await asyncio.sleep(1)
    
    if level.lower() == "mute":
        print(f"Muting Zone {zone_id}...")
        mixer.set_zone_volume(zone_id, "mute")
    else:
        try:
            level_int = int(level)
            db_value = -level_int
            print(f"Setting Zone {zone_id} to {db_value}dB (level {level_int})...")
            mixer.set_zone_volume(zone_id, level_int)
        except ValueError:
            print(f"Error: Invalid level '{level}'. Use 0-61 or 'mute'")
            mixer.close()
            return
    
    # Wait for command to be processed and sent
    await asyncio.sleep(3)
    
    mixer.close()
    print("Done")


def main():
    parser = argparse.ArgumentParser(description="Control Cloud DCM1 Zone Mixer")
    parser.add_argument("--host", default="192.168.1.139", help="DCM1 hostname or IP (default: 192.168.1.139)")
    parser.add_argument("--port", type=int, default=4999, help="DCM1 port (default: 4999)")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Status command
    subparsers.add_parser("status", help="Show status of all zones and groups")
    
    # Set zone source command
    set_zone_source_parser = subparsers.add_parser("set_zone_source", help="Set zone to a specific source")
    set_zone_source_parser.add_argument("zone", type=int, help="Zone ID (1-8)")
    set_zone_source_parser.add_argument("source", type=int, help="Source ID (1-8)")
    
    # Set zone volume level command
    set_zone_volume_level_parser = subparsers.add_parser("set_zone_volume_level", help="Set volume level for a zone")
    set_zone_volume_level_parser.add_argument("zone", type=int, help="Zone ID (1-8)")
    set_zone_volume_level_parser.add_argument("level", help="Volume level (0-61 where 20=-20dB, 62=mute, or 'mute')")
    
    args = parser.parse_args()
    
    # Always enable debug logging for now
    logging.basicConfig(level=logging.DEBUG)
    
    if args.command == "status":
        asyncio.run(show_status(args.host, args.port))
    elif args.command == "set_zone_source":
        asyncio.run(set_zone_source(args.host, args.port, args.zone, args.source))
    elif args.command == "set_zone_volume_level":
        asyncio.run(set_zone_volume_level(args.host, args.port, args.zone, args.level))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
