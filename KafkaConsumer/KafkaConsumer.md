# KafkaConsumer — Complete Developer Reference & Interview Prep

> **Project:** Enterprise .NET 10 Windows Service  
> **Stack:** Kafka (Confluent) · C# · Clean Architecture  
> **Purpose:** Consume both Kafka meter topics → Write raw JSON lines to daily text files

---

## Table of Contents

1. [Project in One Paragraph](#1-project-in-one-paragraph)
2. [Architecture Layers](#2-architecture-layers)
3. [Project File Structure](#3-project-file-structure)
4. [Data Models](#4-data-models)
5. [Complete Data Flow](#5-complete-data-flow)
6. [Component-by-Component Explanation](#6-component-by-component-explanation)
7. [Output File Format](#7-output-file-format)
8. [Threading & Concurrency Model](#8-threading--concurrency-model)
9. [Backpressure Design](#9-backpressure-design)
10. [Kafka Failure & Reconnect](#10-kafka-failure--reconnect)
11. [File Write Failure & Retry Queue](#11-file-write-failure--retry-queue)
12. [Events & Delegates System](#12-events--delegates-system)
13. [Graceful Shutdown](#13-graceful-shutdown)
14. [Configuration Reference](#14-configuration-reference)
15. [Windows Service Setup](#15-windows-service-setup)
16. [Key Bug That Was Fixed](#16-key-bug-that-was-fixed)
17. [Interview Scenarios — Problems & How They Were Handled](#17-interview-scenarios--problems--how-they-were-handled)

---

## 1. Project in One Paragraph

KafkaConsumer is a **.NET 10 Windows Service** that subscribes to two Kafka topics — `meter_voltage` and `meter_current` — using a single consumer under the `MeterReaderGroup` consumer group. Every message it receives is written as a raw JSON line into a daily rotating text file (one file per topic per day). The write pipeline is fully decoupled from Kafka: a bounded `Channel<T>` connects the Kafka poll loop to the file writer. If the file write fails, messages go into an in-memory retry queue and are retried with exponential backoff — Kafka offsets are auto-committed independently so the two concerns never interfere with each other.

---

## 2. Architecture Layers

```
┌──────────────────────────────────────────────────────────┐
│                      WORKER LAYER                        │
│   KafkaWorker.cs   (BackgroundService entry point)      │
│   Program.cs       (DI wiring + UseWindowsService)      │
├──────────────────────────────────────────────────────────┤
│                   INFRASTRUCTURE LAYER                    │
│   KafkaConsumerService   (Confluent poll loop)          │
│   MessagePipeline        (bounded Channel<T>)           │
│   FileWriterService      (batch file writer)            │
│   KafkaConsumerSettings  (config POCOs)                 │
├──────────────────────────────────────────────────────────┤
│                      CORE LAYER                          │
│   ConsumedMessage, MeterReading  (models)               │
│   IKafkaConsumerService, IFileWriterService (interfaces)│
│   ConsumerEvents  (all EventArgs definitions)           │
└──────────────────────────────────────────────────────────┘
```

**Rule:** Core has zero external dependencies. Infrastructure depends only on Core. Worker depends on both.

---

## 3. Project File Structure

```
KafkaConsumer/
├── Program.cs                                  ← DI wiring + UseWindowsService()
├── appsettings.json                            ← All tuneable settings (NO comments)
├── KafkaConsumer.csproj                        ← net10.0-windows, Confluent.Kafka 2.6.3
│
├── Core/
│   ├── Models/
│   │   └── ConsumedMessage.cs                  ← MeterReading + ConsumedMessage
│   ├── Interfaces/
│   │   └── IServices.cs                        ← IKafkaConsumerService, IFileWriterService
│   └── Events/
│       └── ConsumerEvents.cs                   ← All EventArgs classes
│
├── Infrastructure/
│   ├── Configuration/
│   │   └── KafkaConsumerSettings.cs            ← Strongly-typed settings POCOs
│   ├── Kafka/
│   │   ├── KafkaConsumerService.cs             ← Poll loop, reconnect, builds own IConsumer
│   │   └── MessagePipeline.cs                  ← Bounded Channel<ConsumedMessage>
│   └── FileWriter/
│       └── FileWriterService.cs                ← Batch writer, retry queue, StreamWriter pool
│
└── Worker/
    └── KafkaWorker.cs                          ← BackgroundService, events, health timer
```

---

## 4. Data Models

### 4.1 Kafka Incoming Payloads

```json
// From meter_voltage topic:
{"meterId":105,"voltage":221.5,"timestamp":"2026-03-19T07:04:42Z"}

// From meter_current topic:
{"meterId":107,"current":18.3,"timestamp":"2026-03-19T07:04:43Z"}
```

### 4.2 MeterReading.cs — Deserialized domain model

```csharp
public sealed class MeterReading
{
    [JsonPropertyName("meterId")]
    public int MeterId { get; init; }

    [JsonPropertyName("voltage")]
    public double? Voltage { get; init; }   // null for current messages

    [JsonPropertyName("current")]
    public double? Current { get; init; }   // null for voltage messages

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; init; }
}
```

`Voltage` and `Current` are **nullable** because each topic only sends one of them. The other field deserializes as `null` — this is correct and intentional.

### 4.3 ConsumedMessage.cs — Pipeline unit of work

```csharp
public sealed class ConsumedMessage
{
    public long SequenceId { get; init; }       // Monotonic counter — for ordering diagnostics
    public string TopicName { get; init; }      // meter_voltage or meter_current
    public int Partition { get; init; }         // Kafka partition number
    public long Offset { get; init; }           // Kafka offset
    public string RawJson { get; init; }        // Exact string from Kafka — written to file as-is
    public MeterReading? Payload { get; init; } // Deserialized — used for logging/metrics only
    public DateTime ReceivedAt { get; init; }   // UTC consume time
    public int RetryCount { get; set; }         // Mutable — incremented on each retry attempt
    public string DateKey => ReceivedAt.ToString("yyyy-MM-dd"); // Daily file key
}
```

**Key insight:** `RawJson` stores the exact string received from Kafka. This is what gets written to the file. `Payload` is deserialized only for logging/metrics and is not used for file writing.

---

## 5. Complete Data Flow

```
Kafka broker
  [meter_voltage partition 0,1,2]
  [meter_current partition 0,1,2]
        │
        │  consumer.Consume(pollTimeout)
        ▼
KafkaConsumerService  ← single consumer, single poll loop, both topics
        │
        │  BuildMessage(result) → ConsumedMessage
        │  SequenceId = Interlocked.Increment()
        │
        ▼
MessagePipeline.Writer.WriteAsync(message, ct)
        │
        │  ← BACKPRESSURE: if Channel full, WriteAsync suspends here
        │    FileWriterService catches up — no message dropped, no spin
        │
  [ Channel<ConsumedMessage> — bounded 1000 items ]
        │
        ▼
FileWriterService.MainWriteLoopAsync()
        │
        │  Accumulate up to BatchSize=50 messages OR FlushIntervalMs=2000ms
        │  Group by TopicName
        │
        ▼
WriteLinesToFileAsync(filePath, batch)
        │
        │  SemaphoreSlim(1,1) — one thread writes at a time
        │  StringBuilder — build entire batch as one string
        │  StreamWriter.WriteAsync() + FlushAsync()
        │
        ├─ SUCCESS
        │      OnBatchWritten event raised
        │      KafkaWorker logs: Topic, Count, Offsets, FileName
        │
        └─ FAILURE (exception)
               Retry up to MaxFileWriteRetries=5 (exponential backoff)
               On max retries: EnqueueForRetry()
               RetryWorkerLoopAsync() retries every FlushIntervalMs

OUTPUT FILES:
  C:\ProgramData\KafkaConsumer\Output\meter_voltage_2026-03-19.txt
  C:\ProgramData\KafkaConsumer\Output\meter_current_2026-03-19.txt
```

---

## 6. Component-by-Component Explanation

### 6.1 Program.cs

DI registrations — all Singletons:

```csharp
services.Configure<KafkaConsumerSettings>(...);    // settings
services.AddSingleton<MessagePipeline>();           // shared Channel
services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
services.AddSingleton<IFileWriterService, FileWriterService>();
services.AddHostedService<KafkaWorker>();
```

**Critically absent:** No `IConsumer<string,string>` registration. `KafkaConsumerService` builds and owns its own consumer internally — nothing else touches Kafka.

---

### 6.2 MessagePipeline.cs

The connector between Kafka consumer and file writer.

```csharp
_channel = Channel.CreateBounded<ConsumedMessage>(new BoundedChannelOptions(capacity)
{
    FullMode = BoundedChannelFullMode.Wait,   // backpressure — not drop
    SingleReader = true,                       // FileWriterService only
    SingleWriter = true,                       // KafkaConsumerService only
    AllowSynchronousContinuations = false      // prevent stack overflows
});
```

`Complete()` is called when Kafka consumer stops. This signals FileWriterService that no more messages will arrive — it drains remaining items then exits.

---

### 6.3 KafkaConsumerService.cs

**Builds its own `IConsumer<string,string>` privately** — no DI injection. Key config:

```csharp
EnableAutoCommit     = true        // Kafka auto-commits every 5 seconds
AutoCommitIntervalMs = 5000        // no manual commit coordination needed
AutoOffsetReset      = Earliest    // if no committed offset exists: start from beginning
```

**Outer reconnect loop** wraps the inner poll loop:
```
while (!ct.IsCancellationRequested)
{
    BuildConsumer() → Subscribe() → ConsumeLoopAsync()
    ↕ KafkaException caught → exponential backoff → rebuild
}
```

**Inner poll loop:**
```csharp
result = consumer.Consume(pollTimeout);   // synchronous by Confluent design
// null result = no messages in timeout window — continue
await _pipeline.Writer.WriteAsync(message, ct);  // suspends if Channel full
```

**Why `consumer.Consume()` is synchronous:**
Confluent.Kafka's design — `Consume()` internally handles the network I/O on the librdkafka thread pool. Wrapping it in `Task.Run` is not needed; the call returns quickly when a message is available.

---

### 6.4 FileWriterService.cs

**Zero Kafka dependency** — no `IConsumer`, no offsets, no broker calls.

**StreamWriter Pool:**
```
Dictionary<string, StreamWriter> _writerPool
Key = "C:\...\meter_voltage_2026-03-19.txt"
Value = open StreamWriter (append mode, AutoFlush=false)
```

One writer stays open across all batches for the same topic+day. Opening and closing on every batch would be expensive (OS-level file handle operations). Writers are flushed after every batch but kept open.

**Date rollover:**
When `DateKey` changes at midnight, `GetOrCreateWriterAsync` detects the stale writer (key prefix no longer matches today's date), closes it, and creates a new file.

**Batch accumulation:**
```csharp
while (batch.Count < BatchSize)
{
    var remaining = deadline - DateTime.UtcNow;
    if (remaining <= TimeSpan.Zero) break;           // flush interval elapsed

    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    linkedCts.CancelAfter(remaining);

    var msg = await _pipeline.Reader.ReadAsync(linkedCts.Token);
    batch.Add(msg);
}
// Flush whatever we have — even 1 message — after FlushIntervalMs
```

This means at low volume, even a single message gets written within 2 seconds.

---

### 6.5 KafkaWorker.cs

Orchestrator — wires events, starts both services via `Task.WhenAll`, logs health.

```csharp
await Task.WhenAll(
    _kafkaConsumer.RunAsync(stoppingToken),   // Task A — Kafka poll
    _fileWriter.RunAsync(stoppingToken)        // Task B — file write
).ConfigureAwait(false);
```

`Task.WhenAll` runs A and B concurrently. When A finishes (service stopped), `pipeline.Complete()` is called → B drains remaining Channel messages → B finishes → `WhenAll` resolves.

---

## 7. Output File Format

### File location
```
C:\ProgramData\KafkaConsumer\Output\
```

### File naming
```
meter_voltage_2026-03-19.txt    ← all voltage readings for that day
meter_current_2026-03-19.txt    ← all current readings for that day
meter_voltage_2026-03-20.txt    ← new file created automatically at midnight
```

### File content (one JSON per line = JSONL format)
```
meter_voltage_2026-03-19.txt:
{"meterId":105,"voltage":221.5,"timestamp":"2026-03-19T07:04:42Z"}
{"meterId":106,"voltage":218.0,"timestamp":"2026-03-19T07:04:43Z"}
{"meterId":105,"voltage":220.1,"timestamp":"2026-03-19T07:04:44Z"}

meter_current_2026-03-19.txt:
{"meterId":107,"current":18.3,"timestamp":"2026-03-19T07:04:43Z"}
{"meterId":108,"current":22.1,"timestamp":"2026-03-19T07:04:44Z"}
```

**Why append mode?** `StreamWriter(path, append: true)` — if the service restarts mid-day, it appends to the existing file rather than overwriting it. No data from the current day is lost on restart.

---

## 8. Threading & Concurrency Model

### All concurrent operations

| Thread / Task | What runs on it | Mechanism |
|---|---|---|
| Task A | `KafkaConsumerService.ConsumeLoopAsync()` | Thread pool |
| Task B (main) | `FileWriterService.MainWriteLoopAsync()` | Thread pool |
| Task B (retry) | `FileWriterService.RetryWorkerLoopAsync()` | Thread pool via `Task.WhenAll` |
| Timer thread | `KafkaWorker.LogHealth()` | `System.Threading.Timer` |

### Concurrency controls

| Mechanism | Location | Purpose |
|---|---|---|
| `Channel<T>` bounded (1000) | `MessagePipeline` | Async handoff — no shared state between A and B |
| `SemaphoreSlim(1, 1)` | `FileWriterService._writerPoolLock` | One thread writes to file at a time |
| `Interlocked.Increment` | `KafkaConsumerService._sequenceCounter` | Lock-free monotonic sequence ID |
| `Interlocked.Increment/Decrement` | `FileWriterService._retryQueueCount` | Lock-free retry queue depth count |
| `Interlocked.Read` | `KafkaWorker._totalConsumed/Written/Failed` | Lock-free metric reads |
| `volatile bool _isConnected` | `KafkaConsumerService` | Lock-free read by health logger |
| `ConcurrentQueue<T>` | `FileWriterService._retryQueue` | Thread-safe enqueue/dequeue between main and retry loops |

### No blocking calls

- No `.Wait()`, `.Result`, `Thread.Sleep()` anywhere
- `SemaphoreSlim.WaitAsync()` — async wait, thread returns to pool
- `Channel.ReadAsync()` — suspends, thread returns to pool
- `Task.Delay()` — suspends, thread returns to pool
- `consumer.Consume(timeout)` — synchronous but librdkafka-managed, does not block the thread pool long

---

## 9. Backpressure Design

Backpressure prevents the Kafka consumer from outrunning the file writer and filling memory unboundedly.

### How it works

```
FileWriter is slow (disk busy, large batch) 
    → Channel fills up toward capacity (1000)
    → KafkaConsumerService calls WriteAsync()
    → Channel is full → WriteAsync() SUSPENDS
    → Poll loop pauses — no more Consume() calls
    → Kafka broker sees consumer is slow → broker-side flow control
    → FileWriter catches up → Channel drains
    → WriteAsync() resumes → poll loop continues
```

### Why `FullMode.Wait` not `FullMode.DropWrite`

`DropWrite` silently discards messages when full — that would lose data. `Wait` suspends the producer (Kafka poll loop) until space is available. This is always the correct choice when data must not be lost.

---

## 10. Kafka Failure & Reconnect

### Failure detection

`consumer.Consume()` throws `KafkaException` or `ConsumeException`:
- `ConsumeException` with `IsFatal = false` → log warning, continue polling (e.g., EOF)
- `ConsumeException` with `IsFatal = true` → re-throw as `KafkaException` → trigger reconnect
- `KafkaException` → caught by outer loop → reconnect with backoff

### Exponential backoff formula

```csharp
var delayMs = Math.Min(
    ReconnectBaseDelayMs * Math.Pow(2, reconnectAttempt - 1),
    ReconnectMaxDelayMs);

// Attempt 1: 2000ms
// Attempt 2: 4000ms
// Attempt 3: 8000ms
// Attempt 4: 16000ms
// Attempt 5: 32000ms
// Attempt 6+: 60000ms (capped)
```

### What happens to messages during Kafka outage

```
Kafka goes down
    → consumer.Consume() throws
    → ConsumeLoopAsync exits
    → outer loop catches exception
    → SafeClose(consumer) — cleanly closes current consumer
    → Task.Delay(backoff) — waits
    → BuildConsumer() — creates fresh consumer
    → Subscribe() — re-subscribes both topics
    → ConsumeLoopAsync() — resumes from last auto-committed offset
```

No messages are lost because:
1. `EnableAutoCommit = true` commits every 5 seconds
2. On reconnect, `AutoOffsetReset = Earliest` ensures un-committed messages are re-delivered

### `MaxReconnectAttempts = 0`

`0` means retry forever. The service will keep trying until Kafka is back. Change to e.g. `10` if you want it to give up after 10 attempts.

---

## 11. File Write Failure & Retry Queue

### What happens when a file write fails

```
WriteLinesToFileAsync() throws (disk full, permission denied, path error)
    │
    ▼
attempt++ 
    │
    ├─ attempt <= MaxFileWriteRetries (5)?
    │       YES → exponential backoff delay → retry WriteBatchAsync
    │       NO  → EnqueueForRetry(batch)
    │
    ▼
EnqueueForRetry():
    │  _retryQueueCount >= RetryQueueCapacity (500)?
    │       YES → LogCritical + DROP message
    │       NO  → _retryQueue.Enqueue(msg) + Interlocked.Increment
    │
    ▼
RetryWorkerLoopAsync() (runs concurrently, checks every FlushIntervalMs):
    │  TryDequeue all items → snapshot
    │  Group by (TopicName, DateKey)
    │  WriteLinesToFileAsync() for each group
    │       SUCCESS → log info
    │       FAIL    → re-enqueue if RetryCount < MaxFileWriteRetries
    │               → LogCritical + DROP if RetryCount >= MaxFileWriteRetries
```

### Kafka offset during file failures

`EnableAutoCommit = true` means Kafka auto-commits offsets every 5 seconds regardless of file write status. If the service restarts after a file write failure, Kafka may re-deliver those messages (if they weren't auto-committed yet). This could cause duplicate lines in the file — acceptable for this use case. If strict exactly-once file writing is needed, manual commit coordination is required.

---

## 12. Events & Delegates System

### All events

```csharp
// KafkaConsumerService raises:
event EventHandler<KafkaConnectionEventArgs>?  OnConnectionChanged;   // connect/disconnect
event EventHandler<MessageConsumedEventArgs>?  OnMessageConsumed;     // per message

// FileWriterService raises:
event EventHandler<BatchWrittenEventArgs>?       OnBatchWritten;       // batch success
event EventHandler<FileWriteFailedEventArgs>?    OnWriteFailed;        // write failure
event EventHandler<MessageRetryQueuedEventArgs>? OnMessageRetryQueued; // retry queued
```

### Subscription lifecycle

```csharp
// KafkaWorker.WireEvents() — called once at startup
_kafkaConsumer.OnConnectionChanged += OnKafkaConnectionChangedHandler;
_kafkaConsumer.OnMessageConsumed   += OnMessageConsumedHandler;
_fileWriter.OnBatchWritten         += OnBatchWrittenHandler;
_fileWriter.OnWriteFailed          += OnWriteFailedHandler;
_fileWriter.OnMessageRetryQueued   += OnMessageRetryQueuedHandler;

// KafkaWorker.UnwireEvents() — called in finally block on shutdown
// Exact reverse — all -= calls
```

### Why named methods, not lambdas

```csharp
// WRONG — cannot unsubscribe anonymous lambda
_fileWriter.OnBatchWritten += (s, e) => { ... };   // ❌

// CORRECT — named method can be -= unsubscribed
_fileWriter.OnBatchWritten += OnBatchWrittenHandler;  // ✅
```

Anonymous lambda event handlers cannot be unsubscribed because you cannot get a reference back to them. This is a common .NET memory leak — the event publisher holds a delegate reference which keeps the subscriber object alive even after it is logically disposed.

### All EventArgs are `sealed` + `init`-only

```csharp
public sealed class BatchWrittenEventArgs : EventArgs
{
    public string TopicName { get; init; } = string.Empty;
    // ...
}
```

`init` = settable only during object construction. After creation the object is effectively immutable — safe to pass across threads without locks. No handler can corrupt the args seen by another handler.

---

## 13. Graceful Shutdown

### Shutdown sequence (step by step)

```
1. Windows SCM sends STOP  (or Ctrl+C in console mode)
2. stoppingToken is cancelled
3. KafkaConsumerService.ConsumeLoopAsync():
       next WriteAsync(message, ct) throws OperationCanceledException
       loop exits cleanly
4. KafkaConsumerService.RunAsync():
       catches OperationCanceledException → breaks outer loop
       calls pipeline.Complete()  ← no more messages will arrive
5. KafkaConsumerService.SafeClose():
       consumer.Close()  ← commits final auto-offsets to Kafka
       consumer.Dispose()
6. FileWriterService.MainWriteLoopAsync():
       ReadAsync throws ChannelClosedException (pipeline complete)
       exits accumulation loop
       DrainRemainingAsync() — reads all remaining items via TryRead()
       writes final batches to files
7. FileWriterService.RetryWorkerLoopAsync():
       next Task.Delay catches OperationCanceledException → exits
8. FileWriterService.CloseAllWritersAsync():
       FlushAsync() + DisposeAsync() on every StreamWriter in pool
9. KafkaWorker.CleanupAsync():
       UnwireEvents()  ← all event handlers removed
       _healthTimer.Dispose()
       DisposeAsync() on all services
       pipeline.Dispose()
10. Host exits
```

### Why `pipeline.Complete()` is the key

```
KafkaConsumerService stops → pipeline.Complete()
    ↓
FileWriterService.ReadAsync() throws ChannelClosedException
    ↓
FileWriterService exits its loop cleanly (no hung await)
    ↓
Both tasks complete → Task.WhenAll() resolves → ExecuteAsync() returns
```

Without `pipeline.Complete()`, `FileWriterService` would hang forever in `ReadAsync()` waiting for a message that never comes.

---

## 14. Configuration Reference

```json
{
  "KafkaConsumerSettings": {
    "Kafka": {
      "BootstrapServers": "localhost:9092",
      "GroupId": "MeterReaderGroup",
      "VoltageTopicName": "meter_voltage",
      "CurrentTopicName": "meter_current",
      "AutoOffsetReset": "Earliest",
      "MaxPollMessages": 100,
      "PollTimeoutMs": 1000,
      "ReconnectBaseDelayMs": 2000,
      "ReconnectMaxDelayMs": 60000,
      "MaxReconnectAttempts": 0
    },
    "FileWriter": {
      "OutputDirectory": "C:\\ProgramData\\KafkaConsumer\\Output",
      "BatchSize": 50,
      "FlushIntervalMs": 2000,
      "RetryQueueCapacity": 500,
      "MaxFileWriteRetries": 5,
      "FileWriteRetryDelayMs": 2000
    },
    "Processing": {
      "ChannelCapacity": 1000,
      "HealthLogIntervalSeconds": 30
    }
  }
}
```

**Critical rule: NO `//` comments in JSON.** Standard JSON does not support comments. They silently break `System.Text.Json` config parsing — settings load with default values and no error is thrown. This was the root cause of the output directory never being created.

---

## 15. Windows Service Setup

```powershell
# 1. Publish
dotnet publish KafkaConsumer.csproj `
  --configuration Release `
  --runtime win-x64 `
  --output C:\Services\KafkaConsumer

# 2. Create output directory
New-Item -ItemType Directory -Force -Path 'C:\ProgramData\KafkaConsumer\Output'

# 3. Install
sc create KafkaConsumer `
   binPath= "C:\Services\KafkaConsumer\KafkaConsumer.exe" `
   DisplayName= "Kafka Consumer Service" `
   start= auto

sc description KafkaConsumer "Consumes Kafka meter topics and writes JSON to daily text files"

# 4. Auto-restart on failure
sc failure KafkaConsumer reset= 86400 actions= restart/5000/restart/10000/restart/60000

# 5. Start
sc start KafkaConsumer

# 6. Status
sc query KafkaConsumer

# 7. Stop
sc stop KafkaConsumer

# 8. Remove
sc delete KafkaConsumer
```

### Run as console (development)

```powershell
cd KafkaConsumer
dotnet run

# Expected log output:
# [INFO] Output directory ready: C:\ProgramData\KafkaConsumer\Output
# [INFO] KafkaConsumer service RUNNING
# [INFO] Kafka consumer connected — polling [meter_voltage] and [meter_current]
# [INFO] [WRITTEN] Topic=meter_voltage Count=3 Offsets=0-2 File=meter_voltage_2026-03-19.txt
# [INFO] [HEALTH] Consumed=50 Written=50 Failed=0 RetryQueue=0 ChannelDepth=0 KafkaUp=True
```

---

## 16. Key Bug That Was Fixed

### The Original Architecture Bug

The original design had `IConsumer<string,string>` registered in DI and injected into `FileWriterService` so it could call `CommitOffsets()` after each file write.

**What went wrong:**

```
Program.cs           → creates Consumer #1 (registered in DI)
KafkaConsumerService → creates Consumer #2 (built inside BuildConsumer())
FileWriterService    → receives Consumer #1 (from DI)
```

Consumer #1 was never subscribed to any topic. When `FileWriterService.CommitOffsets()` called `_kafkaConsumer.StoreOffset()` on Consumer #1:

```
StoreOffset()  → throws KafkaException (unsubscribed consumer)
    ↓
caught by WriteBatchAsync catch block
    ↓
treated as a FILE WRITE FAILURE
    ↓
retries 5 times (each time: writes file ✅, throws on commit ❌)
    ↓
after 5 retries: EnqueueForRetry()
    ↓
RetryWorkerLoop: writes file ✅, throws on commit ❌, re-enqueues
    ↓
infinite loop — files written 5+ times as duplicates, nothing ever completes
```

**The fix:**

1. Removed `IConsumer<string,string>` from DI entirely
2. Removed `CommitOffsets()` from `FileWriterService` entirely  
3. Set `EnableAutoCommit = true` in `KafkaConsumerService.BuildConsumer()`
4. Kafka auto-commits every 5 seconds independently — no coordination with file writing

**Lesson:** Never share a Confluent.Kafka `IConsumer` instance across services. The consumer owns its subscription state. Injecting it elsewhere causes exactly this kind of subtle failure.

---

## 17. Interview Scenarios — Problems & How They Were Handled

---

### Q1. How do you consume two Kafka topics in one service without two separate consumers?

**Answer:**
`consumer.Subscribe()` accepts a `string[]` — subscribing to multiple topics in one call. One `IConsumer<string,string>` instance, one poll loop, one connection. Each `ConsumeResult` carries a `result.Topic` property that tells you which topic the message came from. Inside `BuildMessage()`, `result.Topic` is stored on `ConsumedMessage.TopicName`. `FileWriterService` uses `TopicName` to determine which daily file to write to.

---

### Q2. What is the Channel<T> and why did you use it instead of just calling the file writer directly?

**Answer:**
`Channel<T>` from `System.Threading.Channels` is a bounded async queue. It decouples the Kafka poll loop (producer) from the file writer (consumer) so they run at their own speeds. The key benefit is **backpressure**: `BoundedChannelOptions` with `FullMode.Wait` means if the file writer is slow (disk busy), the channel fills up and `WriteAsync()` suspends the Kafka poll loop automatically — no busy-spinning, no memory growth, no dropped messages. If I called the file writer directly from the poll loop, a slow disk would block the Kafka consumer thread and cause a session timeout and partition rebalance.

---

### Q3. What happens if Kafka goes down while the service is running?

**Answer:**
`consumer.Consume()` throws a `KafkaException`. The `ConsumeLoopAsync` method re-throws it to the outer `RunAsync` loop. The outer loop catches it, calls `SafeClose(consumer)` to cleanly dispose the current consumer, then waits with exponential backoff (2s → 4s → 8s → max 60s). After the delay, it calls `BuildConsumer()` to create a fresh consumer and `Subscribe()` again. Because `EnableAutoCommit = true` commits every 5 seconds, on reconnect the consumer resumes from the last auto-committed offset — so any messages that arrived during the outage and were already committed are not re-delivered. Messages consumed but not yet auto-committed will be re-delivered, which may cause duplicate lines in the file.

---

### Q4. What happens if the output file cannot be written (disk full)?

**Answer:**
`WriteLinesToFileAsync()` throws an exception. `WriteBatchAsync()` catches it, increments the attempt counter, raises the `OnWriteFailed` event, and retries after an exponential backoff delay (2s, 4s, 8s...). After `MaxFileWriteRetries` (5) failures, `EnqueueForRetry()` moves each message individually into a bounded `ConcurrentQueue`. The `RetryWorkerLoop` checks this queue every `FlushIntervalMs` (2s) and attempts to write again. If the queue exceeds `RetryQueueCapacity` (500), new messages are permanently dropped with a `LogCritical` entry. Kafka offsets are auto-committed independently, so the service does not block on file failures.

---

### Q5. How does daily file rotation work?

**Answer:**
Every `ConsumedMessage` has a `DateKey` property that returns `ReceivedAt.ToString("yyyy-MM-dd")`. The file path is `OutputDirectory\{topicName}_{dateKey}.txt`. The `StreamWriter` pool uses the full file path as the dictionary key. When `GetOrCreateWriterAsync()` is called with tomorrow's path, it doesn't find a match. It then looks for any existing writers whose key starts with the same topic prefix (everything before the date part), closes and disposes them, then opens a new `StreamWriter` for today's path. This happens naturally when the first message arrives after midnight — no timer or scheduled job needed.

---

### Q6. How do you prevent memory leaks in a long-running service?

**Answer:**
Four specific measures:

1. **Event unsubscription:** `UnwireEvents()` called in the `finally` block of `ExecuteAsync()`. Every `+=` in `WireEvents()` has a matching `-=`. Named handler methods are used — never anonymous lambdas, which cannot be unsubscribed.

2. **Bounded Channel:** `Channel.CreateBounded(1000)` — cannot grow beyond 1000 messages. `FullMode.Wait` suspends the producer rather than dropping or unboundedly growing.

3. **Bounded retry queue:** `ConcurrentQueue<ConsumedMessage>` with a manual count check against `RetryQueueCapacity` (500). Messages beyond the limit are dropped with a critical log rather than growing the queue indefinitely.

4. **StreamWriter pool cleanup:** `CloseAllWritersAsync()` is called in `DisposeAsync()` and in the shutdown sequence. Every open `StreamWriter` is flushed and disposed. Date rollover explicitly closes stale writers.

---

### Q7. Why is `EnableAutoCommit = true` and what are the trade-offs?

**Answer:**
`EnableAutoCommit = true` means Confluent.Kafka automatically commits the last consumed offset to Kafka every `AutoCommitIntervalMs` (5000ms). The advantage is simplicity — `FileWriterService` has zero knowledge of Kafka and zero coordination is needed. The trade-off is that on a crash or restart, the service might re-process messages that were auto-committed but not yet written to the file, causing duplicate lines. For meter telemetry written to a log file, this is acceptable. If exactly-once file writing was a hard requirement, manual `EnableAutoCommit = false` with explicit `Commit()` after confirmed file write would be needed — but that requires the consumer and file writer to share the same `IConsumer` instance correctly, which adds significant complexity.

---

### Q8. What does `pipeline.Complete()` do and why is it critical for shutdown?

**Answer:**
`MessagePipeline.Complete()` calls `_channel.Writer.TryComplete()`. This marks the Channel writer as finished — no more items will ever be written. `FileWriterService.MainWriteLoopAsync()` is blocked on `_pipeline.Reader.ReadAsync()`. When the Channel is completed, `ReadAsync()` throws `ChannelClosedException` — this is the signal for `FileWriterService` to exit its batch accumulation loop, drain remaining items via `TryRead()`, flush final batches to disk, and return. Without `Complete()`, `FileWriterService` would hang forever in `ReadAsync()` waiting for a message that will never arrive.

---

### Q9. Why is `SemaphoreSlim(1, 1)` used in FileWriterService and not `lock`?

**Answer:**
`lock` is a synchronous blocking primitive — if one thread holds the lock, any other thread calling `lock` is **blocked** (parked, consuming a thread from the thread pool). `SemaphoreSlim.WaitAsync()` is asynchronous — if the semaphore is held, the awaiting code **suspends** and the thread returns to the thread pool. For an async method like `WriteLinesToFileAsync()` that uses `await writer.WriteAsync()`, using `lock` would be a deadlock risk (you cannot `await` inside a `lock`). `SemaphoreSlim(1, 1)` is the correct async mutual exclusion primitive.

---

### Q10. Why is `ConsumedMessage.RawJson` written to the file instead of re-serializing `Payload`?

**Answer:**
Two reasons. First, it is simpler and faster — no re-serialization step, no allocation of a new JSON string. Second, it is safer — re-serializing `Payload` could introduce differences if the `MeterReading` class does not perfectly model all fields in the original JSON (e.g., extra fields would be dropped by `System.Text.Json` by default). Writing `RawJson` preserves the exact byte sequence from Kafka — what came in is exactly what goes to the file. This is the correct design for a pass-through archival service.

---

### Q11. What is `Interlocked` and where is it used?

**Answer:**
`Interlocked` provides atomic operations (read-modify-write) without locks. It uses CPU-level compare-and-swap instructions. Used in:

- `KafkaConsumerService._sequenceCounter` → `Interlocked.Increment()` — assigns a unique monotonic ID to every message, thread-safe with no lock
- `FileWriterService._retryQueueCount` → `Interlocked.Increment/Decrement()` — tracks retry queue depth across the main write loop and retry worker loop which run concurrently  
- `KafkaWorker._totalConsumed/Written/Failed` → `Interlocked.Read()` and `Interlocked.Increment/Add()` — health metrics updated from event handlers (which fire on various threads) and read by the timer thread

The alternative (a `lock` around every counter update) would be slower and risk contention on the hot message-consumed path.

---

*Last updated: March 2026 | KafkaConsumer v1.0.0 | CABCON TECHNOLOGIES PVT. LTD.*
