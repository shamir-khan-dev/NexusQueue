# Brag Plan: NexusQueue

## What is this app?
NexusQueue is a distributed asynchronous background task and DAG workflow engine in TypeScript and Node.js engineered for high throughput, strict at least once delivery, and zero data loss crash resilience.

## The angle
Enterprise infrastructure that never loses a job. When worker nodes suddenly crash mid execution or APIs return 503 outages, NexusQueue automatically reclaims orphaned tasks, elevates their priority, and desynchronizes retries using AWS Full Jitter.

## Hook (first 2 to 3 seconds)
A simulated worker node crash. A $50,000 institutional wire transfer is processing, the worker server dies mid flight, and the watchdog supervisor recovers it in under 3.5 seconds with zero data loss.

## Key moments (the middle)
• Heartbeat Watchdog Rescue: Worker Alpha dies, Watchdog detects missed pulse, reclaims the wire transfer, elevates priority to P0, and completes it on Worker Beta.
• Kahn Topological DAG Pipeline: 4 stage pipeline executes Extract, parallel Clean and ML Scoring, and final Warehouse Load without manual promise spaghetti.
• Live Conveyor Belt and Telemetry SLA: Real time sliding window throughput of 156,000+ enqueued tasks per sec with median and tail latency percentiles (p50, p90, p95, p99).

## Outro / punchline
NexusQueue. Zero dropped tasks. Built by Shamir Khan at Wilfrid Laurier University.

## User flow worth showing
1. Entry: High priority task dispatched into the binary min heap queue.
2. Key action: Worker node dies mid flight during execution; watchdog supervisor detects missed heartbeat and safely reclaims the orphaned task.
3. Result: Automatic failover completion with zero data loss, verified across real time WebSocket telemetry.

## Tone
• Preset: cinematic
• Creative direction: Hardened infrastructure product showcase: exact, high throughput, hardware telemetry
• Interpretation: Fast, deliberate cuts paired with solid holds for reading metrics. Dark graphite material, tabular numerals, and restrained cyan accents.

## Format: landscape (1920x1080)
## Duration: 20s

## Visual identity (from the project)
• Background: #090c13
• Surface: #0f1420
• Accent: #38bdf8
• Text: #f1f5f9
• Muted: #64748b
• Display font: Plus Jakarta Sans
• Mono font: JetBrains Mono
• Strongest visual element: Live queue conveyor belt with tactile stage columns and latency SLA cards.

## Share copy (draft)
Meet NexusQueue: an enterprise distributed task and DAG workflow engine built in TypeScript and Node.js with binary min heaps, Kahn topological scheduling, and zero data loss crash recovery.

## Audio direction
• Role: Driving technical electronic bed with motion matched telemetry accents
• Music: Low driving synth pulse
• Music treatment: Low volume bed with smooth swell on the crash recovery moment
• SFX posture: Sparse, motion matched, professional restraint
• Audio coupled moments: Heartbeat pulse, crash alarm hit, task completion chime
• Restraint rule: No distracting comedy sounds; strictly technical and tactile

## Storyboard

### Scene 1: The Crash (0.0s to 5.0s)
Visual: Dark graphite console. Worker Alpha processing institutional wire transfer. Suddenly, Worker Alpha dies (simulated OOM kill). Red alert indicator.
Sequential: Task card glows processing -> Worker crashes -> Alert badge appears.
Audio intent: Sudden tension, subtle low hit.
Transition mood: Dramatic cut to Scene 2.

### Scene 2: Watchdog Failover (5.0s to 10.0s)
Visual: Watchdog supervisor detects missed heartbeat pulse in 3.5s. Task reclaimed, elevated to P0 Critical, and dispatched to Worker Beta.
Sequential: Watchdog scan pulse -> Priority promoted to P0 -> Worker Beta marks completed.
Audio intent: Resolution, crisp confirmation tick.
Transition mood: Clean wipe to Scene 3.

### Scene 3: Kahn DAG Pipeline (10.0s to 15.0s)
Visual: 4 stage DAG workflow executing in parallel. Extract stage branches concurrently into Clean and ML Scoring, then merges into Snowflake Load.
Sequential: Extract completes -> Parallel branch arrows illuminate -> Final warehouse stage loads.
Audio intent: Flowing, rhythmic velocity.
Transition mood: Clean wipe to Scene 4.

### Scene 4: Telemetry SLA and Outro (15.0s to 20.0s)
Visual: Live queue conveyor belt and latency SLA grid (p50: 34ms, p99: 42ms, Throughput: 156,000+ tasks/sec). NexusQueue logo and author attribution.
Sequential: Counters roll up quickly to final numbers -> Logo lockup resolves.
Audio intent: Full electronic resolve and fade.
Transition mood: Terminal hold.

