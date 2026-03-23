// ============================================================
//  HealthChecker.cs  –  RabbitMQ Server Health Check
// ============================================================
//
//  Before every send attempt the worker calls CheckAsync().
//  It performs a simple GET on RabbitMQ's /api/overview endpoint.
//
//  Why /api/overview?
//    It is a lightweight, read-only endpoint that returns broker
//    statistics.  HTTP 200 = broker is running and credentials
//    are valid.  Any exception = broker is unreachable.
//
//  The HttpClient has a 5-second timeout configured in Form1,
//  so a hung server will fail quickly rather than blocking
//  the worker for a long time.
// ============================================================

using System;
using System.Net.Http;
using System.Threading.Tasks;

public class HealthChecker
{
    private readonly HttpClient _http;
    private const string OverviewUrl = "http://localhost:15672/api/overview";

    public HealthChecker(HttpClient http)
    {
        _http = http;
    }

    /// <summary>
    /// Returns true if RabbitMQ responds with HTTP 2xx.
    /// Returns false on any network failure or non-2xx response.
    /// Never throws – exceptions are caught and logged.
    /// </summary>
    public async Task<bool> CheckAsync()
    {
        try
        {
            HttpResponseMessage response = await _http.GetAsync(OverviewUrl);

            bool up = response.IsSuccessStatusCode;
            Logger.Log($"[Health] RabbitMQ is {(up ? "UP ✓" : $"DOWN ✗ (HTTP {(int)response.StatusCode})")}");
            return up;
        }
        catch (Exception ex)
        {
            // Connection refused, DNS failure, timeout, etc.
            Logger.Log($"[Health] RabbitMQ unreachable: {ex.Message}");
            return false;
        }
    }
}
