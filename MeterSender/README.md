RabbitMQ is a Message Broker. It is used in the application for asynchronous communication over Microservices, event-driven architecture, queue processing.
1.	RabbitMQ use Erlang run time environment. So first install Erlang.
    👉 https://www.erlang.org/downloads and Windows installer download and install (otp_win64.exe
	
2.	RabbitMQ Server install
    👉 https://www.rabbitmq.com/install-windows.html and rabbitmq-server-x.x.x.exe download and install
	
3.	RabbitMQ Service Start
		RabbitMQ Commad Prompt (sbin dir): Command prompt Run as Administrator
		C:\Program Files\RabbitMQ Server\rabbitmq_server-3.x\sbin
		rabbitmq-service start
		rabbitmq-service stop
		rabbitmqctl status
		rabbitmq-plugins enable rabbitmq_management
4.	Open RabbitMQ Server dashboard in the browser
		http://localhost:15672
		
	Login: 	
		username: guest
		password: guest
	
	Now you can see 
		Queues
		Exchanges
		Connections
		Channels
		Messages
	
5.	RabbitMQ Architecture
	Producer → Exchange →(Binding)→ Queue → Consumer 

6.	Publish the message using the api
	http://localhost:15672/api/exchanges/%2F/meter_exchange/publish
	
	Authorization: 	Basic Auth
					Username: guest
					Password: guest
	Content-Type: application/json
	
	Pay load: 	{
					"properties": {},
					"routing_key": "meter.reading",
					"payload": "{\"meterId\":105,\"energy\":6.6}",
					"payload_encoding": "string"
				}	


# MeterSender – Reliable RabbitMQ Producer (WinForms / .NET 8)

A Windows Forms application that demonstrates how to build a
**production-grade reliable message producer** that sends meter readings
(Voltage and Current) to a RabbitMQ exchange via the HTTP Management API.

The focus is on **never losing a message**, even during broker outages,
application restarts, or message bursts.

---

## Table of Contents

1. [What This App Does](#1-what-this-app-does)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Descriptions](#3-component-descriptions)
4. [Message Flow – Normal Case](#4-message-flow--normal-case)
5. [Message Flow – RabbitMQ Goes Down](#5-message-flow--rabbitmq-goes-down)
6. [Message Flow – RabbitMQ Comes Back](#6-message-flow--rabbitmq-comes-back)
7. [Ordering Guarantee](#7-ordering-guarantee)
8. [Failure During Queue Drain](#8-failure-during-queue-drain)
9. [Backpressure Handling](#9-backpressure-handling)
10. [Retry Backoff Strategy](#10-retry-backoff-strategy)
11. [Thread Safety](#11-thread-safety)
12. [Logging](#12-logging)
13. [File Structure](#13-file-structure)
14. [RabbitMQ Setup](#14-rabbitmq-setup)
15. [How to Run](#15-how-to-run)
16. [Scenario Walkthroughs](#16-scenario-walkthroughs)
17. [Key Design Decisions](#17-key-design-decisions)

---

## 1. What This App Does

The application reads **Voltage** and **Current** values from text inputs
and publishes them as JSON messages to a RabbitMQ exchange.

It handles:
- Network outages without losing any message
- Ordered delivery (oldest messages always delivered first)
- High-volume bursts (in-memory queue + disk overflow)
- Application restarts (messages survive on disk)
- Slow broker recovery (exponential retry backoff)

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FORM1 (UI Thread)                            │
│                                                                     │
│   [Voltage TextBox] [Send Voltage Button]                           │
│   [Current TextBox] [Send Current Button]                           │
│                                                                     │
│   Status: RabbitMQ ONLINE ✓    Queue: 3 msg    Voltage.txt: 0       │
│                                                                     │
│   Activity Log:                                                     │
│   [12:05:01] [Worker] ✓ Queue message sent. Key=meter.voltage       │
│   [12:05:02] [Health] RabbitMQ is UP ✓                              │
└──────────────┬──────────────────────────────────────────────────────┘
               │ TryEnqueue()
               ▼
┌──────────────────────────────────────────────┐
│           ProducerQueue (×2)                 │
│                                              │
│   _voltageQueue: ConcurrentQueue<string>     │   Max 10,000 msgs
│   _currentQueue: ConcurrentQueue<string>     │   per queue
└──────────────────┬───────────────────────────┘
                   │  If queue full (backpressure)
                   │  ──────────────────────────►  DiskStore.SaveAsync()
                   │                                     │
                   │  TryDequeue()                       │
                   ▼                                     ▼
┌──────────────────────────────────────────────────────────────────┐
│                   MessageSender (Background Task)                │
│                                                                  │
│  Loop:                                                           │
│    1. HealthChecker.CheckAsync()   ──── DOWN? ──► backoff wait   │
│                                                                  │
│    2. FlushFileAsync(Voltage.txt)  ◄── Priority 1 (OLDEST)       │
│       FlushFileAsync(Current.txt)                                │
│                                                                  │
│    3. DrainQueueAsync(voltageQ)    ◄── Priority 2 (NEWER)        │
│       DrainQueueAsync(currentQ)                                  │
│                                                                  │
│    4. idle wait 300 ms → repeat                                  │
└──────────────────────────────┬───────────────────────────────────┘
                               │  HTTP POST (Basic Auth)
                               ▼
┌───────────────────────────────────────────────────────────────────┐
│          RabbitMQ Management HTTP API  (port 15672)               │
│                                                                   │
│  POST /api/exchanges/%2F/meter_exchange/publish                   │
│                                                                   │
│  Body: {                                                          │
│    "properties": {},                                              │
│    "routing_key": "meter.voltage",                                │
│    "payload": "{\"meterId\":105,\"voltage\":230.5,...}",          │
│    "payload_encoding": "string"                                   │
│  }                                                                │
└───────────────────────────────────────────────────────────────────┘

         ┌──────────────────────────────────────────────┐
         │               DISK (local files)             │
         │                                              │
         │  Voltage.txt  ← one JSON line per message    │
         │  Current.txt  ← one JSON line per message    │
         │  logs.txt     ← all activity with timestamps │
         └──────────────────────────────────────────────┘
```

---

## 3. Component Descriptions

### `Logger.cs`
A static utility that writes every log entry to `logs.txt` on disk.
It also fires a `static event Action<string> OnLog` so the UI can display
the same entries in the activity log panel in real time.

Uses a plain `lock` object to prevent two threads from writing to the file
at the same time. Thread-safe.

---

### `HealthChecker.cs`
Performs a simple `GET` to `http://localhost:15672/api/overview` using the
pre-configured `HttpClient` (which has Basic Auth and a 5-second timeout).

- HTTP 200 → returns `true`  (broker is up)
- Any exception or non-2xx → returns `false`  (broker is down)

Never throws. All errors are caught and logged.

---

### `DiskStore.cs`  (static)
Provides thread-safe append, read, and line-removal operations for
`Voltage.txt` and `Current.txt`.

| Method | Purpose |
|---|---|
| `SaveAsync(file, json)` | Appends one JSON line to the file |
| `LoadAllAsync(file)` | Reads all lines as a list |
| `RemoveFirstLineAsync(file)` | Removes the oldest line (after successful send) |
| `CountAsync(file)` | Returns the number of stored records |

**Thread safety:** Each file has its own `SemaphoreSlim(1,1)` — an async
mutex.  `SemaphoreSlim` is used instead of `lock{}` because our methods
are `async` and `lock` cannot span an `await` boundary.

---

### `ProducerQueue.cs`
A thin wrapper around `ConcurrentQueue<string>` with a capacity limit.

The UI thread calls `TryEnqueue()` after each button click.
The background worker calls `TryDequeue()` to consume messages.

`ConcurrentQueue<T>` is part of `System.Collections.Concurrent` and is
lock-free and thread-safe by design.

**Backpressure:** `TryEnqueue()` returns `false` when the queue reaches
`MaxCapacity` (10,000). The caller (Form1) then saves the message directly
to disk instead.

---

### `MessageSender.cs`
The core worker. Runs as a single background `Task` (started with
`Task.Run(WorkerLoopAsync)`) for the lifetime of the application.

Its infinite loop follows this priority order:

```
while running:
    1. HealthChecker.CheckAsync()
       ├─ DOWN  → wait (backoff)  →  continue
       └─ UP    →
            2. FlushFileAsync(Voltage.txt, "meter.voltage")
               ├─ send fails mid-flush  → return false  → continue (re-check health)
               └─ file empty            → continue to next step
            3. FlushFileAsync(Current.txt, "meter.current")
               (same pattern)
            4. DrainQueueAsync(voltageQueue, "meter.voltage", Voltage.txt)
               ├─ send fails → save failed item to Voltage.txt → return false → continue
               └─ queue empty → continue to next step
            5. DrainQueueAsync(currentQueue, "meter.current", Current.txt)
               (same pattern)
            6. idle 300ms
```

**Important:** Steps 2 and 3 (file flush) always run before steps 4 and 5
(queue drain). This is the ordering guarantee.

---

### `Form1.cs`
The WinForms UI. Built entirely in code (no `.Designer.cs`).

Responsibilities:
- Creates all service objects (plain constructors, no DI container)
- Button click handlers: validate → `TryEnqueue()` → backpressure fallback
- Subscribes to `Logger.OnLog` → appends to the activity log panel
- Subscribes to `_sender.ServerHealthChanged` → updates the status label
- A 1-second timer refreshes the queue size and file record counters
- Disposes `HttpClient` and stops the worker on close

---

## 4. Message Flow – Normal Case

```
User types 230.5, clicks "Send Voltage"
        │
        ▼
Form1.BtnSendVoltage_Click()
  builds JSON: {"meterId":105,"voltage":230.5,"timestamp":"2026-03-16T12:30:15Z"}
        │
        ▼
_voltageQueue.TryEnqueue(json)  →  returns true (queue has space)
        │
        ▼ (worker is polling every 300ms)
MessageSender.WorkerLoopAsync()
    ├─ HealthChecker: GET /api/overview  →  200 OK  →  healthy = true
    ├─ FlushFileAsync(Voltage.txt)  →  file empty → skip
    ├─ FlushFileAsync(Current.txt)  →  file empty → skip
    └─ DrainQueueAsync(voltageQueue)
           TryDequeue() → gets the JSON
           PublishAsync("meter.voltage", json)
             POST http://localhost:15672/api/exchanges/%2F/meter_exchange/publish
             Body: {"properties":{},"routing_key":"meter.voltage","payload":"{...}","payload_encoding":"string"}
             ← 200 OK  {"routed":true}
           Logger.Log("✓ Queue message sent")
```

Total latency: < 300ms (next worker poll)

---

## 5. Message Flow – RabbitMQ Goes Down

```
User clicks "Send Voltage" while RabbitMQ is down
        │
        ▼
_voltageQueue.TryEnqueue(json)  →  true  (message sits in queue)

Background worker wakes up:
    ├─ HealthChecker: connection refused  →  healthy = false
    ├─ Logger: "RabbitMQ DOWN. Retry #1 in 2s"
    ├─ _retryAttempt = 1
    └─ await Task.Delay(2000)  →  repeat

Next iteration (2 seconds later):
    ├─ HealthChecker: still down  →  healthy = false
    ├─ Logger: "RabbitMQ DOWN. Retry #2 in 5s"
    ├─ _retryAttempt = 2
    └─ await Task.Delay(5000)  →  repeat

Meanwhile the user keeps clicking buttons.
Each click adds to _voltageQueue (or _currentQueue).
Messages accumulate in memory, waiting safely.
```

---

## 6. Message Flow – RabbitMQ Comes Back

```
Worker iteration after broker restarts:
    ├─ HealthChecker: 200 OK  →  healthy = true
    ├─ Logger: "✓ RabbitMQ reconnected!"
    ├─ _retryAttempt = 0  (reset backoff counter)
    │
    ├─ FlushFileAsync(Voltage.txt)
    │     (if any messages were persisted to disk previously,
    │      they are sent first – OLDEST messages first)
    │
    └─ DrainQueueAsync(voltageQueue)
          Dequeues and sends every message that accumulated
          while the broker was down – in the exact order they
          were enqueued.
```

---

## 7. Ordering Guarantee

Messages are always delivered to RabbitMQ in the order they were created.

```
Timeline:
  t=0   Voltage.txt has: [msg-A, msg-B, msg-C]  (failed earlier)
  t=1   User sends msg-D  →  enters voltageQueue
  t=2   User sends msg-E  →  enters voltageQueue
  t=5   RabbitMQ reconnects

Delivery order to RabbitMQ:
  msg-A  (from Voltage.txt, position 1)
  msg-B  (from Voltage.txt, position 2)
  msg-C  (from Voltage.txt, position 3)
  msg-D  (from voltageQueue, first enqueued)
  msg-E  (from voltageQueue, second enqueued)

✓ Strict FIFO. No messages jump the queue.
```

This is enforced by the worker loop ordering:
**Step 2 (file flush) always completes before Step 3 (queue drain).**

---

## 8. Failure During Queue Drain

This handles Requirement 7: "while sending the 6th record, connection fails."

```
voltageQueue has: [q1, q2, q3, q4, q5, q6]
Voltage.txt is empty.

DrainQueueAsync begins:
  TryDequeue() → q1 → PublishAsync → ✓ sent
  TryDequeue() → q2 → PublishAsync → ✓ sent
  TryDequeue() → q3 → PublishAsync → ✓ sent
  TryDequeue() → q4 → PublishAsync → ✓ sent
  TryDequeue() → q5 → PublishAsync → ✓ sent
  TryDequeue() → q6 → PublishAsync → ✗ FAILS

  q6 was already removed from queue (TryDequeue happened before send).
  Action: DiskStore.SaveAsync(Voltage.txt, q6)
          → Voltage.txt now contains: [q6]
  Returns false → worker goes back to top of loop.

voltageQueue still has: []  (empty – q1-q5 sent, q6 saved to disk)
Voltage.txt now has: [q6]

Next worker iteration:
  HealthChecker → down → backoff → ...

When server comes back:
  FlushFileAsync(Voltage.txt):
    LoadAll → [q6]
    PublishAsync(q6) → ✓ sent
    RemoveFirstLineAsync → Voltage.txt deleted

  DrainQueueAsync(voltageQueue):
    IsEmpty → nothing to do

✓ q6 delivered. No data loss. Order maintained.
```

---

## 9. Backpressure Handling

If messages are produced faster than they can be consumed (e.g. during a
long outage with many button clicks), the `ProducerQueue` could grow
without bound and exhaust memory.

**Protection mechanism:**

```
ProducerQueue.MaxCapacity = 10,000

When user clicks "Send Voltage":
    _voltageQueue.TryEnqueue(json)
        ├─ queue.Count < 10,000  →  enqueue, return true   ← normal path
        └─ queue.Count >= 10,000 →  return false            ← backpressure

When TryEnqueue returns false (Form1 button handler):
    DiskStore.SaveAsync(Voltage.txt, json)  ← persist to disk instead
    MessageBox: "Buffer full. Message saved to Voltage.txt."
```

The message is never lost — it flows to disk instead of memory.
When the queue drains, the disk file is processed first (Priority 1),
so these overflow messages are sent before any new UI messages.

---

## 10. Retry Backoff Strategy

When RabbitMQ is unreachable, the worker waits progressively longer
between retries to avoid hammering a server that may be recovering.

```
Retry #   Wait before next attempt
───────   ────────────────────────
  1         2 seconds
  2         5 seconds
  3        10 seconds
  4        30 seconds
  5+       60 seconds  (stays at 60s for all subsequent retries)
```

The counter resets to 0 immediately when the broker responds with HTTP 200.

**Implementation:**
```csharp
private static readonly int[] BackoffDelays = { 2, 5, 10, 30, 60 };

int delayIndex = Math.Min(_retryAttempt, BackoffDelays.Length - 1);
int delaySecs  = BackoffDelays[delayIndex];
await Task.Delay(delaySecs * 1000, cancellationToken);
```

---

## 11. Thread Safety

| Component | Mechanism | Why |
|---|---|---|
| `Logger` (file write) | `lock (_lock)` | Simple; file write is synchronous |
| `DiskStore` (file read/write) | `SemaphoreSlim(1,1)` per file | File ops are async; lock{} can't span await |
| `ProducerQueue` (enqueue/dequeue) | `ConcurrentQueue<T>` (built-in) | Lock-free; designed for multi-thread use |
| `MessageSender` (worker) | Single `Task.Run` | One worker = inherently sequential sends |
| UI updates from events | `BeginInvoke()` | Marshal background thread calls to UI thread |

---

## 12. Logging

All activity is logged to `logs.txt` (timestamped, one entry per line).
The same entries appear in the UI activity panel in real time.

**Log categories:**

| Prefix | Meaning |
|---|---|
| `[Health]` | RabbitMQ reachability check result |
| `[Worker]` | Worker loop status, retry info, flush/drain progress |
| `[Publish]` | HTTP POST result (✓ or ✗) |
| `[Queue]`  | Enqueue/dequeue activity, backpressure events |
| `[Disk]`   | File save/remove/delete operations |
| `[UI]`     | Button click activity |
| `[ERROR]`  | Caught exceptions with context |

**Sample log output:**

```
[12:05:00] [Health] RabbitMQ is UP ✓
[12:05:00] [Worker] Flushing Voltage.txt: 3 record(s) remaining.
[12:05:00] [Publish] ✓ [meter.voltage] {"routed":true}
[12:05:00] [Worker] ✓ File record sent and removed.
[12:05:01] [Publish] ✓ [meter.voltage] {"routed":true}
[12:05:01] [Worker] ✓ File record sent and removed.
[12:05:01] [Disk] Voltage.txt fully flushed and deleted.
[12:05:01] [Worker] Sending from queue. Remaining in queue: 2
[12:05:01] [Publish] ✓ [meter.voltage] {"routed":true}
[12:05:01] [Worker] ✓ Queue message sent. Key=meter.voltage
```

---

## 13. File Structure

```
MeterSender/
├── MeterSender.csproj   ← .NET 8 WinForms project definition
├── Program.cs           ← Entry point (3 lines)
├── Logger.cs            ← Static logger → logs.txt + UI event
├── HealthChecker.cs     ← GET /api/overview → true/false
├── DiskStore.cs         ← Read/write Voltage.txt, Current.txt
├── ProducerQueue.cs     ← In-memory ConcurrentQueue + backpressure
├── MessageSender.cs     ← Background worker (ordering, retry, flush)
└── Form1.cs             ← UI (inputs, status, activity log)

Runtime files (created automatically):
├── Voltage.txt          ← Persisted unsent voltage messages
├── Current.txt          ← Persisted unsent current messages
└── logs.txt             ← Activity log
```

---

## 14. RabbitMQ Setup

### Option A – Docker (recommended, fastest)

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

Wait ~10 seconds, then open:  `http://localhost:15672`
Login: **guest** / **guest**

### Create the exchange

1. Click **Exchanges** in the top nav
2. Click **Add a new exchange**
3. Fill in:
   - Name: `meter_exchange`
   - Type: `topic`
   - Durable: ✓ (checked)
4. Click **Add exchange**

### Create queues and bindings (optional, to see messages)

1. Go to **Queues** → **Add a new queue**
   - Name: `voltage_queue`  → Add queue
   - Name: `current_queue`  → Add queue

2. Go to **Exchanges** → `meter_exchange` → **Bindings**
   - Bind `voltage_queue` with routing key `meter.voltage`
   - Bind `current_queue` with routing key `meter.current`

Now messages will appear in the queues under the **Get messages** button.

---

## 15. How to Run

### Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- Windows OS (required for WinForms)
- RabbitMQ running on `localhost:15672`

### From command line

```bash
cd MeterSender
dotnet run
```

### From Visual Studio

1. Open `MeterSender.sln`
2. Press **F5**

### Testing without RabbitMQ

You can run the app without RabbitMQ running.
- Click **Send Voltage** or **Send Current**
- Messages will accumulate in the in-memory queue
- The status label will show **OFFLINE**
- The worker will retry with backoff (check `logs.txt`)

When you start RabbitMQ later, messages flush automatically.

---

## 16. Scenario Walkthroughs

### Scenario A – Normal operation

1. Start app, start RabbitMQ
2. Type `230.5`, click **Send Voltage**
3. Within 300ms the worker sends it, log shows `✓ Queue message sent`
4. Check RabbitMQ Management UI → message appears in `voltage_queue`

### Scenario B – Outage and recovery

1. Start app
2. Send 5 voltage messages (all delivered)
3. Stop RabbitMQ (`docker stop rabbitmq`)
4. Send 10 more voltage messages
   - They accumulate in `_voltageQueue`
   - Log shows "RabbitMQ DOWN. Retry #1 in 2s..." etc.
5. Start RabbitMQ again (`docker start rabbitmq`)
6. Within seconds, the worker reconnects and flushes all 10 messages
7. Check the queue – 10 messages delivered, in order

### Scenario C – App restart during outage

1. Send 5 messages while RabbitMQ is down
   - They sit in the in-memory queue
   - **But no disk file is written yet** (queue has space)
2. Close the app – messages are LOST (in-memory only)
3. Restart the app – queue is empty

**Lesson:** If you need messages to survive app restarts, you need the
disk persistence path. This happens automatically when:
- The queue overflows (backpressure path)
- A send fails during drain

For guaranteed persistence of ALL messages before they are confirmed
by the broker, you would need to write to disk immediately on enqueue
(before attempting to send). This is a trade-off between simplicity
and durability – the current design prioritizes simplicity for learning.

### Scenario D – Backpressure

1. Stop RabbitMQ
2. Rapidly click **Send Voltage** thousands of times
   (or programmatically enqueue 10,001 messages)
3. At the 10,001st message, the queue is full
4. Form shows: "Buffer full. Message saved to Voltage.txt."
5. `Voltage.txt` now contains the overflow messages
6. Start RabbitMQ – worker flushes `Voltage.txt` first (oldest messages)
7. Then drains the queue – all messages delivered in order

---

## 17. Key Design Decisions

### Why HTTP API instead of AMQP?

The RabbitMQ AMQP protocol uses a persistent TCP connection, which requires
the `RabbitMQ.Client` NuGet package. The HTTP Management API requires no
external dependencies – just `HttpClient` from the standard library.
For learning purposes the HTTP API is simpler to understand and debug.

### Why a single background Task?

Having one worker that processes messages sequentially makes ordering
trivially easy to guarantee. There are no race conditions between multiple
senders. The `ConcurrentQueue` and `SemaphoreSlim` in `DiskStore` protect
the data structures that are shared with the UI thread.

### Why dequeue before sending?

```
TryDequeue() → then → PublishAsync()
```

The message is removed from the queue before the network call.
If the call fails, the message is explicitly saved to disk.
The alternative (dequeue only on success) would require "peeking" at the
head without removing, which `ConcurrentQueue` does not support atomically.

### Why not write to disk immediately on enqueue?

Writing every message to disk on every button click would add latency to
the UI and generate unnecessary I/O when RabbitMQ is healthy (99% of the
time). The current design treats disk as a fallback, not the primary path.

### Why RemoveFirstLineAsync rewrites the whole file?

For a learning project, simplicity wins. For high throughput (millions of
messages) you would maintain a byte offset into the file and seek past
delivered records instead of rewriting.

---

*Built for learning. No external NuGet packages required.*
*Only standard .NET 8 libraries are used.*
