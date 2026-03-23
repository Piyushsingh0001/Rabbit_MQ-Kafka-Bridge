// ============================================================
//  ProducerQueue.cs  –  In-Memory Producer Queue with Backpressure
// ============================================================
//
//  PURPOSE
//  ───────
//  The UI thread never sends directly to RabbitMQ.  Instead it
//  enqueues messages here.  The background worker dequeues and
//  sends them.  This decouples the UI from the network, keeping
//  the UI responsive at all times.
//
//  Flow:
//    UI Button Click
//        │
//        ▼
//    ProducerQueue.TryEnqueue()
//        │
//        ▼ (background worker picks up)
//    MessageSender worker
//        │
//        ▼
//    RabbitMQ HTTP API
//
//  BACKPRESSURE
//  ────────────
//  If messages are produced faster than they are consumed
//  (e.g. during a long RabbitMQ outage with rapid button clicks),
//  the queue could grow without bound and exhaust memory.
//
//  To prevent this, TryEnqueue() returns false when the queue
//  size reaches MaxCapacity (10,000).  The caller (Form1 button
//  handler) redirects overflowing messages directly to disk.
//
//  THREAD SAFETY
//  ─────────────
//  ConcurrentQueue<T> is thread-safe by design: multiple threads
//  can enqueue and dequeue simultaneously without any additional
//  locking.
// ============================================================

using System.Collections.Concurrent;

public class ProducerQueue
{
    // Maximum number of messages allowed in memory.
    // At ~200 bytes per JSON message, 10,000 = ~2 MB of RAM.
    public const int MaxCapacity = 10_000;

    // Thread-safe queue from System.Collections.Concurrent
    private readonly ConcurrentQueue<string> _queue = new ConcurrentQueue<string>();

    // ---- Public API -------------------------------------------

    /// <summary>
    /// Adds a message to the back of the queue.
    ///
    /// Returns TRUE  → message enqueued successfully.
    /// Returns FALSE → queue is full (backpressure triggered).
    ///                 Caller should persist the message to disk.
    /// </summary>
    public bool TryEnqueue(string jsonMessage)
    {
        // Check capacity BEFORE enqueuing
        // Note: Count on ConcurrentQueue is not guaranteed to be perfectly
        // accurate in the presence of concurrent operations, but it is
        // close enough for backpressure purposes.
        if (_queue.Count >= MaxCapacity)
        {
            Logger.Log($"[Queue] ⚠ BACKPRESSURE! Queue at capacity ({MaxCapacity}). Redirecting to disk.");
            return false;   // Signal caller to use disk persistence
        }

        _queue.Enqueue(jsonMessage);
        Logger.Log($"[Queue] Enqueued. Queue size: {_queue.Count}");
        return true;
    }

    /// <summary>
    /// Removes and returns the oldest message from the queue.
    /// Returns false if the queue is empty.
    /// </summary>
    public bool TryDequeue(out string message)
        => _queue.TryDequeue(out message!);

    /// <summary>Current number of messages waiting in the queue.</summary>
    public int Count => _queue.Count;

    /// <summary>True when the queue has no pending messages.</summary>
    public bool IsEmpty => _queue.IsEmpty;
}
