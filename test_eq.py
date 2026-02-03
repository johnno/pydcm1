#!/usr/bin/env python3
"""Quick test to check and set EQ values."""
import asyncio
import logging
import random
from pydcm1.mixer import DCM1Mixer, PRIORITY_READ
from pydcm1.protocol import MixerProtocol

async def test():
    logging.basicConfig(level=logging.WARNING)
    mixer = DCM1Mixer('192.168.1.139', 4999, enable_heartbeat=False)
    await mixer.async_connect()
    print("Connected, waiting for initial state...")
    await asyncio.sleep(3)
    
    print("Querying Zone 8 EQ...")
    mixer._enqueue_command(MixerProtocol.command_query_zone_eq(8), PRIORITY_READ)
    await asyncio.sleep(1)
    
    z8 = mixer.zones_by_id.get(8)
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < 5.0:
        if z8.eq_treble is not None and z8.eq_mid is not None and z8.eq_bass is not None:
            break
        await asyncio.sleep(0.1)
    else:
        print("Timed out waiting for initial EQ values")

    before = (z8.eq_treble, z8.eq_mid, z8.eq_bass)
    print(f'Zone 8 EQ before: T={before[0]}, M={before[1]}, B={before[2]}')

    def random_eq_level() -> int:
        options = [0] + [i for i in range(-14, 15, 2) if i != 0]
        return random.choice(options)

    requested = (random_eq_level(), random_eq_level(), random_eq_level())

    print(f"Setting EQ to T:{requested[0]:+d}, M:{requested[1]:+d}, B:{requested[2]:+d}...")
    mixer.set_zone_eq(8, *requested)
    print("Commands sent, waiting for responses...")
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < 5.0:
        current = (z8.eq_treble, z8.eq_mid, z8.eq_bass)
        if current != before and None not in current:
            break
        await asyncio.sleep(0.1)
    else:
        current = (z8.eq_treble, z8.eq_mid, z8.eq_bass)
        print("Timed out waiting for EQ response")
    
    print(
        f'Zone 8 EQ after set (before query): T={current[0]}, M={current[1]}, B={current[2]} '
        f"(requested T={requested[0]}, M={requested[1]}, B={requested[2]})"
    )
    if current != requested:
        print(
            f"Mismatch: requested T={requested[0]}, M={requested[1]}, B={requested[2]} "
            f"but device returned T={current[0]}, M={current[1]}, B={current[2]}"
        )
    
    print("Querying again...")
    mixer.query_status()
    await asyncio.sleep(3)
    
    print(f'Zone 8 EQ after query: T={z8.eq_treble}, M={z8.eq_mid}, B={z8.eq_bass}')
    mixer.close()

asyncio.run(test())
