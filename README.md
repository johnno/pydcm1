pydcm1
======

pydcm1 is a Python library to connect to a Cloud DCM1 Zone Mixer over TCP/IP. Control zone sources and volume levels, query labels, and monitor status changes in real-time.

It is primarily being developed with the intent of supporting [home-assistant](https://github.com/home-assistant/home-assistant)


Hardware Requirements
---------------------

**Important:** The Cloud DCM1 has an RS-232 serial port, not an ethernet port. You need a **Serial-to-IP converter** to use this library.

Tested with:
- **Waveshare RS232/485/422 TO POE ETH (B)** - Set "Enable Multi-host: No" for true pass-through mode

This library should also work with the **DCM1e** model (which has native ethernet), but this is untested as I don't have access to one.


Installation
------------

Not yet published to PyPI. Install from source:

```bash
git clone https://github.com/johnno/pydcm1.git
cd pydcm1
pip install -e .
```


Command Line Usage
==================

The library includes a command-line tool for controlling and monitoring your DCM1:

```bash
python3 main.py <command> [options]
```

Commands:
- `status` - Display status of all zones
- `volume <zone_id> <level>` - Set zone volume (0-61 = -0dB to -61dB, 62 = mute)
- `source <zone_id> <source_id>` - Change zone source (1-8)

Examples:

**Show status of all zones:**
```bash
python3 main.py status
```

Output shows zone labels, current sources, volume levels, and enabled line inputs:
```
Zone 1 (Main Bar):             7 - Music                | Vol: -28dB      | Inputs: 1,7,8
Zone 2 (Back Bar):             7 - Music                | Vol: -17dB      | Inputs: 1,2,7,8
Zone 3 (Snug):                 7 - Music                | Vol: -22dB      | Inputs: 1,7
Zone 4 (Zone 4):               1 - TV (Main Bar)        | Vol: -35dB      | Inputs: 1,2,3,4,5,6,7,8
```

**Change zone volume:**
```bash
# Set zone 4 to -35dB
python3 main.py volume 4 35

# Mute zone 4
python3 main.py volume 4 62
```

**Change zone source:**
```bash
# Set zone 2 to source 3
python3 main.py source 2 3
```


Using in an Application
=======================

See `example.py` for detailed usage.

Basic example:

```python
from pydcm1.mixer import DCM1Mixer

# Connect to DCM1
mixer = DCM1Mixer("192.168.1.139", 4999)

# Wait for initial connection
await mixer.protocol.async_connect()
await asyncio.sleep(2)  # Allow time for initial queries

# Change zone source
await mixer.change_zone_source(zone_id=1, source_id=7)

# Set volume level (0-61 = -dB directly, 62 = mute)
await mixer.set_zone_volume_level(zone_id=1, level=28)  # -28dB

# Get current status
source = mixer.protocol.get_status_of_zone(1)
volume = mixer.protocol.get_volume_level(1)
```


Features
========

- Query and set zone sources (1-8)
- Control volume levels (0-61dB attenuation, 62=mute)
- Automatic zone and source label querying
- Line input enable/disable filtering per zone
- Priority queue for responsive user commands
- Background heartbeat polling (syncs with physical panel changes)
- Command confirmation (broadcasts changes to all TCP clients)
- Persistent connection with automatic reconnection


Completed
=========

- [x] Get names of sources and zones from the DCM1
- [x] Volume level control and querying
- [x] Line input enable/disable querying
- [x] Priority queue for command scheduling
- [x] Background heartbeat polling for physical panel sync
- [x] Command confirmation with automatic query-back


TODO
====

- [ ] Implement volume_up(), volume_down(), mute_toggle() methods
- [ ] Publish to PyPI
- [ ] Test with DCM1e model (native ethernet)
