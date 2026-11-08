# Hyperframes Composition Brief: NexusQueue

## Objective
Create a high impact, polished launch brag video for NexusQueue.

## Output
• Composition directory: `brag-output/composition/`
• Rendered video: `brag-output/brag.mp4`
• Format: landscape (1920x1080)
• Duration: 20 seconds

## Source Material
• Project root: `c:\Users\shami\OneDrive\Desktop\Coding\Backend Projects\NexusQueue`
• Primary files read: `src/dashboard/public/index.html`, `README.md`, `package.json`
• Product name: NexusQueue
• Tagline and strongest claims:
  • "156,000+ Enqueued Tasks per sec"
  • "Zero Data Loss Crash Resilience"
  • "AWS Full Jitter Backoff"
• Key UI moments to recreate:
  • Worker node crash and automated watchdog zombie rescue
  • Kahn topological sort 4 stage DAG pipeline
  • Live queue conveyor belt with p50, p90, p95, p99 latency percentiles
• Copy that must appear verbatim:
  • "NexusQueue"
  • "Distributed Task Engine and DAG Workflow System"
  • "Zero Task Loss Crash Resilience"

## Creative Direction
• Tone preset: cinematic
• Creative direction: Hardened infrastructure product showcase: exact, high throughput, hardware telemetry
• Interpretation: Fast, crisp cuts with solid holds for reading metrics. Dark graphite material, tabular numerals, and restrained cyan accents.
• Angle: Enterprise infrastructure that never drops a job. When worker nodes crash mid flight or APIs return 503 outages, NexusQueue automatically reclaims orphaned tasks, elevates priority to P0, and desynchronizes retries.
• Hook: Worker Alpha crashes mid execution during an institutional wire transfer; watchdog supervisor detects missed pulse in 3.5 seconds and recovers it without data loss.
• Outro: NexusQueue. Zero dropped tasks. Built by Shamir Khan at Wilfrid Laurier University.
• Avoid:
  • Generic SaaS language
  • Abstract filler visuals
  • Unrelated decorative redesigns

## Visual Identity
• Background: #090c13
• Surface: #0f1420
• Accent: #38bdf8
• Text: #f1f5f9
• Muted: #64748b
• Display font: Plus Jakarta Sans
• Mono font: JetBrains Mono

## Storyboard
Use the storyboard in `brag-output/brag-plan.md` as the contract.
Scene breakdown:
1. The Crash (5s): Worker Alpha processing wire transfer dies mid flight. Red warning tag.
2. Watchdog Failover (5s): Watchdog reclaims orphaned task in 3.5s, promotes priority to P0, completes on Worker Beta with zero data loss.
3. Kahn DAG Pipeline (5s): Multi stage DAG pipeline resolves dependencies and branches into parallel execution stages.
4. Telemetry SLA and Outro (5s): Live conveyor belt and latency SLA matrix (p50: 34ms, p99: 42ms, Throughput: 156,000+ tasks/sec) with author lockup.

## Audio
• Audio role: Driving technical electronic bed with motion matched telemetry accents
• Audio arc: Tension during worker crash, crisp resolution on failover, rhythmic flow during DAG execution, terminal resolve on metrics outro
• Music: `happy-beats-business-moves-vol-1-by-ende-dot-app.mp3`
• Music treatment: Low volume background bed with smooth swell on crash recovery
• Audio coupled moments: Heartbeat pulse, crash alarm hit, task completion tick
• SFX selection guidance: Tactical, mechanical, quiet clicks and tones

## Hyperframes Instructions
Use the Hyperframes CLI to scaffold, lint, check, and render the composition in `brag-output/composition/`.
Run `npx hyperframes check` before render.
Render to `brag-output/brag.mp4`.
Extract the best frame as `brag-output/brag.jpg`.

