// ============================================================
//  Logger.cs  –  Thread-Safe File Logger + UI Event
// ============================================================
//
//  Writes every log entry to:
//    • logs.txt  (on disk, always)
//    • OnLog event (so Form1 can show it in the activity panel)
//
//  Thread safety:
//    A simple lock object prevents two threads from writing to
//    the file at the same time (corrupting it).
//
//  Usage:
//    Logger.Log("[Worker] Sent voltage message.");
//    Logger.LogException("FlushFile", ex);
// ============================================================

using System;
using System.IO;

public static class Logger
{
    // Path to the log file (created in the app's working directory)
    private static readonly string LogFile = "logs.txt";

    // Prevents two threads writing simultaneously
    private static readonly object _lock = new object();

    /// <summary>
    /// Fired on every log call so the UI can display the entry
    /// in the activity log panel without polling the file.
    /// NOTE: This event is raised on the calling thread (which may
    /// be a background thread), so subscribers must use BeginInvoke.
    /// </summary>
    public static event Action<string>? OnLog;

    /// <summary>
    /// Write a timestamped log entry to disk and fire the UI event.
    /// Safe to call from any thread.
    /// </summary>
    public static void Log(string message)
    {
        string entry = $"[{DateTime.Now:HH:mm:ss}] {message}";

        // Write to file under lock (thread-safe)
        lock (_lock)
        {
            File.AppendAllText(LogFile, entry + Environment.NewLine);
        }

        // Notify UI (subscribers handle thread marshalling themselves)
        OnLog?.Invoke(entry);
    }

    /// <summary>Convenience overload that logs an exception with context label.</summary>
    public static void LogException(string context, Exception ex)
        => Log($"[ERROR] [{context}] {ex.GetType().Name}: {ex.Message}");
}
