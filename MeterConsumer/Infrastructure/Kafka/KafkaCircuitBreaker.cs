using MeterConsumer.Core.Events;

namespace MeterConsumer.Infrastructure.Kafka;

/// <summary>
/// Simple circuit breaker for Kafka produce calls.
///
/// States:
///  Closed   → normal operation, failures are counted
///  Open     → Kafka unavailable, calls blocked immediately
///  HalfOpen → probe: one test call allowed; success=Closed, fail=Open
///
/// Thread-safe: all state transitions use Interlocked / lock.
/// No external dependencies — no Polly.
/// </summary>
public sealed class KafkaCircuitBreaker
{
    // ── State ────────────────────────────────────────────────────────────────
    private enum State { Closed, Open, HalfOpen }

    private volatile State _state = State.Closed;
    private int _failureCount = 0;                         // Interlocked
    private DateTime _openedAt = DateTime.MinValue;
    private readonly object _transitionLock = new();

    // ── Config ───────────────────────────────────────────────────────────────
    private readonly int _failureThreshold;
    private readonly TimeSpan _resetTimeout;

    // ── Events ───────────────────────────────────────────────────────────────
    public event EventHandler<ConnectionStatusChangedEventArgs>? OnStatusChanged;

    public KafkaCircuitBreaker(int failureThreshold, int resetSeconds)
    {
        _failureThreshold = failureThreshold;
        _resetTimeout = TimeSpan.FromSeconds(resetSeconds);
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /// <summary>True when circuit is Closed or HalfOpen (allowed to attempt).</summary>
    public bool IsAvailable
    {
        get
        {
            if (_state == State.Closed) return true;
            if (_state == State.Open)
            {
                // Check if reset timeout has elapsed → move to HalfOpen
                if (DateTime.UtcNow - _openedAt >= _resetTimeout)
                {
                    lock (_transitionLock)
                    {
                        if (_state == State.Open)  // double-check inside lock
                        {
                            _state = State.HalfOpen;
                            return true;   // allow one probe attempt
                        }
                    }
                }
                return false;
            }
            return _state == State.HalfOpen;
        }
    }

    /// <summary>Call after a successful Kafka delivery.</summary>
    public void RecordSuccess()
    {
        if (_state == State.Closed) return;   // fast path — no lock needed

        lock (_transitionLock)
        {
            Interlocked.Exchange(ref _failureCount, 0);
            var prev = _state;
            _state = State.Closed;

            if (prev != State.Closed)
                RaiseStatusChanged(isConnected: true, "Kafka circuit closed — delivery confirmed");
        }
    }

    /// <summary>Call after a failed Kafka delivery attempt.</summary>
    public void RecordFailure()
    {
        var count = Interlocked.Increment(ref _failureCount);

        if (_state == State.HalfOpen || count >= _failureThreshold)
        {
            lock (_transitionLock)
            {
                if (_state != State.Open)
                {
                    _state = State.Open;
                    _openedAt = DateTime.UtcNow;
                    RaiseStatusChanged(isConnected: false,
                        $"Kafka circuit opened after {count} failures");
                }
            }
        }
    }

    // ── Private ──────────────────────────────────────────────────────────────

    private void RaiseStatusChanged(bool isConnected, string reason) =>
        OnStatusChanged?.Invoke(this, new ConnectionStatusChangedEventArgs
        {
            ServiceName = "Kafka",
            IsConnected = isConnected,
            Reason = reason
        });
}
