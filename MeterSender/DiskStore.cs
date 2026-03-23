// ============================================================
//  DiskStore.cs  –  Disk Persistence for Unsent Messages
// ============================================================
//
//  PURPOSE
//  ───────
//  When RabbitMQ is unavailable (or a publish fails), messages
//  must not be lost.  This class appends them to a local text
//  file — one JSON line per message — so they survive app
//  restarts and can be replayed when the broker comes back.
//
//  FILES
//  ─────
//    Voltage.txt   – one voltage JSON per line
//    Current.txt   – one current JSON per line
//
//  Example line in Voltage.txt:
//    {"meterId":105,"voltage":230.5,"timestamp":"2026-03-16T12:30:15Z"}
//
//  THREAD SAFETY
//  ─────────────
//  Two SemaphoreSlim instances (one per file) prevent concurrent
//  reads/writes that would corrupt the files.  SemaphoreSlim is
//  used instead of lock{} because our methods are async and
//  lock{} cannot span an await boundary.
//
//  REMOVAL STRATEGY
//  ────────────────
//  When a message is successfully sent, RemoveFirstLineAsync()
//  rewrites the file without the first line.  This is simple and
//  correct for a learning example.  For very high throughput you
//  would use a pointer/offset approach instead.
// ============================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public static class DiskStore
{
    // ---- File paths -------------------------------------------
    public const string VoltageFile = "Voltage.txt";
    public const string CurrentFile = "Current.txt";

    // ---- One async-compatible lock per file -------------------
    // SemaphoreSlim(1,1) = async mutex: only one task at a time.
    private static readonly SemaphoreSlim _voltageLock = new SemaphoreSlim(1, 1);
    private static readonly SemaphoreSlim _currentLock = new SemaphoreSlim(1, 1);

    // ---- Public API -------------------------------------------

    /// <summary>
    /// Appends one JSON line to the appropriate file.
    /// Called when a message could not be sent to RabbitMQ.
    /// </summary>
    public static async Task SaveAsync(string filePath, string jsonLine)
    {
        SemaphoreSlim sem = GetLock(filePath);
        await sem.WaitAsync();
        try
        {
            // AppendAllTextAsync creates the file if it doesn't exist yet.
            await File.AppendAllTextAsync(filePath, jsonLine + Environment.NewLine);
            Logger.Log($"[Disk] Saved to {Path.GetFileName(filePath)}: {Truncate(jsonLine)}");
        }
        catch (Exception ex)
        {
            Logger.LogException("DiskStore.SaveAsync", ex);
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>
    /// Reads ALL lines currently stored in the file.
    /// Returns an empty list if the file does not exist.
    /// </summary>
    public static async Task<List<string>> LoadAllAsync(string filePath)
    {
        SemaphoreSlim sem = GetLock(filePath);
        await sem.WaitAsync();
        try
        {
            if (!File.Exists(filePath))
                return new List<string>();

            string[] lines = await File.ReadAllLinesAsync(filePath);

            // Filter out any blank lines (e.g. trailing newline)
            return lines.Where(l => !string.IsNullOrWhiteSpace(l)).ToList();
        }
        catch (Exception ex)
        {
            Logger.LogException("DiskStore.LoadAllAsync", ex);
            return new List<string>();
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>
    /// Removes the FIRST line from the file (the oldest message).
    /// Called after a message has been successfully sent.
    ///
    /// Algorithm:
    ///   1. Read all lines
    ///   2. Skip the first one
    ///   3. Write the rest back
    ///   4. If nothing remains, delete the file (keep things clean)
    /// </summary>
    public static async Task RemoveFirstLineAsync(string filePath)
    {
        SemaphoreSlim sem = GetLock(filePath);
        await sem.WaitAsync();
        try
        {
            if (!File.Exists(filePath)) return;

            List<string> lines = (await File.ReadAllLinesAsync(filePath))
                .Where(l => !string.IsNullOrWhiteSpace(l))
                .ToList();

            if (lines.Count <= 1)
            {
                // Only one record left (or file is empty) – delete file entirely
                File.Delete(filePath);
                Logger.Log($"[Disk] {Path.GetFileName(filePath)} fully flushed and deleted.");
            }
            else
            {
                // Write back everything except the first line
                await File.WriteAllLinesAsync(filePath, lines.Skip(1));
            }
        }
        catch (Exception ex)
        {
            Logger.LogException("DiskStore.RemoveFirstLineAsync", ex);
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>Returns number of records currently stored in the file.</summary>
    public static async Task<int> CountAsync(string filePath)
    {
        var lines = await LoadAllAsync(filePath);
        return lines.Count;
    }

    // ---- Private Helpers --------------------------------------

    private static SemaphoreSlim GetLock(string filePath)
        => filePath == VoltageFile ? _voltageLock : _currentLock;

    private static string Truncate(string s, int max = 90)
        => s.Length <= max ? s : s[..max] + "...";
}
