// ============================================================
//  MessageSender.cs  –  Background Worker: Ordering, Retry, Fault Tolerance
// ============================================================
//
//  This is the brain of the application.  A single background
//  Task runs a continuous loop that:
//
//    1. Checks RabbitMQ health.
//    2. If DOWN: waits using exponential backoff, retries.
//    3. If UP:   flushes disk files FIRST (older messages),
//                then drains the in-memory queues (newer messages).
//
//  ─────────────────────────────────────────────────────────────
//  ORDERING GUARANTEE  (FIFO, strict)
//  ─────────────────────────────────────────────────────────────
//  Delivery priority within each message type (voltage / current):
//
//    Priority 1 ─ Records in Voltage.txt / Current.txt
//                 (written when sends failed previously)
//    Priority 2 ─ Records in the in-memory ProducerQueue
//                 (freshly produced by the UI)
//
//  New UI messages NEVER jump ahead of stored file messages.
//
//  ─────────────────────────────────────────────────────────────
//  FAILURE DURING DRAIN  (requirement 7)
//  ─────────────────────────────────────────────────────────────
//  Example: queue has 6 items, items 1-5 sent OK, item 6 fails.
//
//    Step A: item 6 was already dequeued (removed from queue).
//    Step B: send fails  →  item 6 appended to Voltage.txt.
//    Step C: worker returns to top of loop.
//    Step D: health check shows server down → backoff.
//    Step E: server comes back → FlushFile sends Voltage.txt[item6].
//    Step F: queue is empty → nothing left.  Done.
//
//  Order delivered to RabbitMQ:  item1 item2 item3 item4 item5 item6  ✓
//
//  ─────────────────────────────────────────────────────────────
//  RETRY BACKOFF  (requirement 5)
//  ─────────────────────────────────────────────────────────────
//  Delays between retries (seconds):
//    1st  →  2 s
//    2nd  →  5 s
//    3rd  → 10 s
//    4th  → 30 s
//    5th+ → 60 s
//  Counter resets to 0 on first successful health check.
//
//  ─────────────────────────────────────────────────────────────
//  CONCURRENCY / THREAD SAFETY
//  ─────────────────────────────────────────────────────────────
//  • Only ONE worker task runs (enforced by _isRunning flag).
//  • File access is protected by SemaphoreSlim inside DiskStore.
//  • ProducerQueue uses ConcurrentQueue (lock-free, thread-safe).
//  • Events (ServerHealthChanged) are invoked from the background
//    thread – subscribers (Form1) must use BeginInvoke.
// ============================================================

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public class MessageSender
{
    // ---- Retry backoff delays in seconds ----------------------
    // Index: retry attempt (clamped to last entry for 5th+ retry)
    private static readonly int[] BackoffDelays = { 2, 5, 10, 30, 60 };

    // ---- Dependencies -----------------------------------------
    private readonly HttpClient     _http;
    private readonly HealthChecker  _health;
    private readonly ProducerQueue  _voltageQueue;
    private readonly ProducerQueue  _currentQueue;

    // ---- State ------------------------------------------------
    private int  _retryAttempt = 0;
    private bool _isRunning    = false;
    private CancellationTokenSource _cts = new CancellationTokenSource();

    // ---- RabbitMQ endpoint ------------------------------------
    private const string PublishUrl =
        "http://localhost:15672/api/exchanges/%2F/meter_exchange/publish";

    // ---- Events for UI ----------------------------------------

    /// <summary>
    /// Fired whenever the health status changes so Form1 can update
    /// the status label.  Invoked on the background thread.
    /// </summary>
    public event Action<bool>? ServerHealthChanged;

    // ---- Constructor ------------------------------------------

    public MessageSender(
        HttpClient    http,
        HealthChecker health,
        ProducerQueue voltageQueue,
        ProducerQueue currentQueue)
    {
        _http          = http;
        _health        = health;
        _voltageQueue  = voltageQueue;
        _currentQueue  = currentQueue;
    }

    // ---- Lifecycle --------------------------------------------

    /// <summary>Starts the background worker task (safe to call once).</summary>
    public void Start()
    {
        if (_isRunning) return;
        _isRunning = true;
        _cts = new CancellationTokenSource();
        Task.Run(WorkerLoopAsync);   // fire and forget (worker runs until Stop())
        Logger.Log("[Worker] Message sender worker started.");
    }

    /// <summary>Signals the worker to stop after its current iteration.</summary>
    public void Stop()
    {
        _cts.Cancel();
        Logger.Log("[Worker] Stop requested.");
    }

    // ===========================================================
    //  MAIN WORKER LOOP
    // ===========================================================

    private async Task WorkerLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                // ── STEP 1: Health Check ─────────────────────────────
                bool healthy = await _health.CheckAsync();
                ServerHealthChanged?.Invoke(healthy);   // tell UI

                if (!healthy)
                {
                    // Calculate how long to wait before retrying
                    int delayIndex  = Math.Min(_retryAttempt, BackoffDelays.Length - 1);
                    int delaySecs   = BackoffDelays[delayIndex];

                    Logger.Log(
                        $"[Worker] RabbitMQ DOWN. " +
                        $"Retry #{_retryAttempt + 1} in {delaySecs}s. " +
                        $"VoltageQ={_voltageQueue.Count} CurrentQ={_currentQueue.Count}");

                    _retryAttempt++;

                    // Wait for the backoff period, but cancel immediately if Stop() called
                    await Task.Delay(delaySecs * 1000, _cts.Token)
                              .ContinueWith(_ => { });   // swallow TaskCanceledException

                    continue;   // go back to top → re-check health
                }

                // ── Server is UP ────────────────────────────────────
                if (_retryAttempt > 0)
                {
                    Logger.Log("[Worker] ✓ RabbitMQ reconnected! Resuming message delivery.");
                    _retryAttempt = 0;   // reset backoff counter
                }

                // ── STEP 2: PRIORITY 1 – Flush disk files ───────────
                // Voltage file first (preserves creation order)
                bool voltageFileOk = await FlushFileAsync(
                    DiskStore.VoltageFile, "meter.voltage");

                if (!voltageFileOk) continue;   // server went down during flush → retry

                bool currentFileOk = await FlushFileAsync(
                    DiskStore.CurrentFile, "meter.current");

                if (!currentFileOk) continue;

                // ── STEP 3: PRIORITY 2 – Drain in-memory queues ─────
                // Only reached when BOTH files are fully empty.
                bool voltageQueueOk = await DrainQueueAsync(
                    _voltageQueue, "meter.voltage", DiskStore.VoltageFile);

                if (!voltageQueueOk) continue;

                bool currentQueueOk = await DrainQueueAsync(
                    _currentQueue, "meter.current", DiskStore.CurrentFile);

                if (!currentQueueOk) continue;

                // ── IDLE: nothing to send right now ─────────────────
                // Wait briefly before polling again to avoid busy-spin
                await Task.Delay(300, _cts.Token)
                          .ContinueWith(_ => { });
            }
            catch (OperationCanceledException)
            {
                // Stop() was called
                break;
            }
            catch (Exception ex)
            {
                // Catch-all: log and continue so the worker never dies
                Logger.LogException("WorkerLoop", ex);
                await Task.Delay(1000).ConfigureAwait(false);
            }
        }

        _isRunning = false;
        Logger.Log("[Worker] Message sender worker stopped.");
    }

    // ===========================================================
    //  STEP 2 HELPER: FLUSH DISK FILE → RABBITMQ
    // ===========================================================

    /// <summary>
    /// Reads Voltage.txt (or Current.txt) and sends each record to
    /// RabbitMQ one by one, oldest first.
    ///
    /// Removes each record from the file immediately after a
    /// successful send so the file shrinks as messages are delivered.
    ///
    /// Returns true  = file is now empty (all records sent).
    /// Returns false = a send failed mid-way (server went down).
    ///                 Caller should re-check health before continuing.
    /// </summary>
    private async Task<bool> FlushFileAsync(string filePath, string routingKey)
    {
        while (true)
        {
            // Read current state of the file
            List<string> lines = await DiskStore.LoadAllAsync(filePath);

            if (lines.Count == 0)
                return true;   // File is empty – done

            Logger.Log($"[Worker] Flushing {System.IO.Path.GetFileName(filePath)}: {lines.Count} record(s) remaining.");

            // lines[0] is the OLDEST record (written first, top of file)
            string innerJson = lines[0];

            bool sent = await PublishAsync(routingKey, innerJson);

            if (sent)
            {
                // ✓ Remove the sent record from the file
                await DiskStore.RemoveFirstLineAsync(filePath);
                Logger.Log($"[Worker] ✓ File record sent and removed.");
            }
            else
            {
                // ✗ Server went down during flush
                // The record is still in the file – leave it there.
                // On the next iteration the health check will detect
                // the outage and enter backoff mode.
                Logger.Log($"[Worker] ✗ Send failed during file flush. Will retry after backoff.");
                return false;   // signal caller to re-check health
            }
        }
    }

    // ===========================================================
    //  STEP 3 HELPER: DRAIN IN-MEMORY QUEUE → RABBITMQ
    // ===========================================================

    /// <summary>
    /// Dequeues and sends messages from the in-memory ProducerQueue
    /// one at a time.
    ///
    /// IMPORTANT: The message is dequeued (removed from queue) BEFORE
    /// the send attempt.  This is intentional:
    ///   • If the send succeeds → great, message delivered.
    ///   • If the send fails → the message is written to disk so it
    ///     is NOT lost.  The file will be flushed on the next retry
    ///     (Step 2), preserving correct ordering.
    ///
    /// Remaining queue items (not yet dequeued) stay in the queue and
    /// will be processed after the failed item's file entry is flushed.
    ///
    /// Returns true  = queue fully drained with no failures.
    /// Returns false = a send failed (item saved to disk).
    ///                 Caller should re-check health.
    /// </summary>
    private async Task<bool> DrainQueueAsync(
        ProducerQueue queue,
        string        routingKey,
        string        persistFilePath)
    {
        while (queue.TryDequeue(out string message))
        {
            Logger.Log($"[Worker] Sending from queue. Remaining in queue: {queue.Count}");

            bool sent = await PublishAsync(routingKey, message);

            if (sent)
            {
                Logger.Log($"[Worker] ✓ Queue message sent. Key={routingKey}");
            }
            else
            {
                // ✗ Send failed — save the dequeued (and not-yet-sent) message to disk
                // so it is picked up in Step 2 on the next retry cycle.
                await DiskStore.SaveAsync(persistFilePath, message);
                Logger.Log($"[Worker] ✗ Queue send failed. Message saved to disk. " +
                           $"Remaining in queue: {queue.Count}");

                // Return false — remaining queue items stay in queue and will be
                // sent AFTER the file record above is flushed (correct ordering).
                return false;
            }
        }

        return true;   // queue drained successfully
    }

    // ===========================================================
    //  LOW-LEVEL HTTP PUBLISH
    // ===========================================================

    /// <summary>
    /// Sends one message to RabbitMQ via the HTTP Management API.
    ///
    /// The RabbitMQ HTTP publish endpoint expects this JSON body:
    /// {
    ///   "properties":       {},
    ///   "routing_key":      "meter.voltage",
    ///   "payload":          "{\"meterId\":105,\"voltage\":6.6,...}",
    ///   "payload_encoding": "string"
    /// }
    ///
    /// The "payload" value is the inner meter JSON embedded as a
    /// JSON string, so its double-quotes are escaped with \".
    ///
    /// Returns true on HTTP 2xx, false on any error (network or HTTP).
    /// Never throws – all exceptions are caught and logged.
    /// </summary>
    private async Task<bool> PublishAsync(string routingKey, string innerJson)
    {
        try
        {
            // Escape the inner JSON so it can sit inside a JSON string value.
            // We must replace \ first to avoid double-escaping.
            string escaped = innerJson
                .Replace("\\", "\\\\")
                .Replace("\"", "\\\"");

            // Build the outer wrapper expected by the RabbitMQ HTTP API
            string outerJson =
                $"{{" +
                $"\"properties\":{{}}," +
                $"\"routing_key\":\"{routingKey}\"," +
                $"\"payload\":\"{escaped}\"," +
                $"\"payload_encoding\":\"string\"" +
                $"}}";

            var content = new StringContent(outerJson, Encoding.UTF8, "application/json");
            HttpResponseMessage response = await _http.PostAsync(PublishUrl, content);

            if (response.IsSuccessStatusCode)
            {
                // RabbitMQ returns {"routed":true} on success
                string body = await response.Content.ReadAsStringAsync();
                Logger.Log($"[Publish] ✓ [{routingKey}] {body}");
                return true;
            }
            else
            {
                string body = await response.Content.ReadAsStringAsync();
                Logger.Log($"[Publish] ✗ HTTP {(int)response.StatusCode}: {body}");
                return false;
            }
        }
        catch (Exception ex)
        {
            Logger.Log($"[Publish] ✗ Exception during publish: {ex.Message}");
            return false;
        }
    }
}
