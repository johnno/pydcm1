# Transport Extraction Refactor Plan

## Goal
Extract pure transport layer and merge all application logic into DCM1Mixer.

## Current State
- **protocol.py**: MixerProtocol (asyncio.Protocol) - has TCP I/O + queue + heartbeat + inflight tracking + response parsing
- **mixer.py**: DCM1Mixer - has domain objects (Zone/Source/Group) + debouncing + confirmation + public API

## Target State
- **protocol.py**: MixerProtocol (pure asyncio.Protocol) - ONLY TCP I/O + response parsing + callbacks
- **mixer.py**: DCM1Mixer - domain objects + ALL queue/inflight/heartbeat/debouncing/confirmation logic + public API

## Transformation Steps

### Step 1: Create Pure Transport Class (TransportProtocol temp name)
Extract from current MixerProtocol:
- asyncio.Protocol methods: `connection_made`, `connection_lost`, `data_received`
- Response parsing: `_process_received_message` and all regex matches
- Parsing helpers: `_process_zone_source_received`, `_process_group_source_received`
- Write method: Simple `write(message)` that calls `transport.write(message.encode())`
- Callback firing: Call listener methods when responses parsed
- NO queue, NO heartbeat, NO inflight tracking, NO reconnection logic

### Step 2: Move Everything Else to DCM1Mixer
Move from MixerProtocol to DCM1Mixer:
- Command queue + worker (`_command_queue`, `_command_worker`)
- Inflight tracking (`_inflight_commands`, `_debounce_inflight_commands`)
- Heartbeat (`_heartbeat` task)
- Connection watchdog (`_connection_watchdog`)
- Reconnection logic (`_wait_to_reconnect`, `_handle_connection_broken`)
- All send methods (enqueue logic stays, just call transport.write())
- Connection management (`async_connect`, `close`)

### Step 3: Wire Transport to Mixer
- DCM1Mixer creates TransportProtocol instance
- DCM1Mixer implements MixerResponseListener (gets callbacks from transport)
- DCM1Mixer.async_connect() passes self as callback to create_connection()

### Step 4: Rename Classes
- TransportProtocol → MixerProtocol
- Current MixerProtocol logic merged into DCM1Mixer (already done in step 2)

## Final Architecture
```
User Code
    ↓
DCM1Mixer (mixer.py)
    ├── Domain: Zone/Source/Group objects
    ├── Queue: Priority command queue + worker
    ├── Inflight: Recovery tracking
    ├── Heartbeat: Periodic polling
    ├── Debouncing: Coalesce rapid changes
    ├── Confirmation: Query-after-write verification
    └── Uses ↓
        MixerProtocol (protocol.py)
            ├── TCP: connection_made/lost/data_received
            ├── Parse: Regex matching responses
            └── Callback: Fire listener methods
```

## Benefits
- Clean separation: transport vs application
- No coupling: protocol doesn't know about queues/inflight/heartbeat
- Single responsibility: each layer has one job
- Testable: can mock transport independently
