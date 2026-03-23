// ============================================================
//  Form1.cs  –  Main Application Window
// ============================================================
//
//  This file contains:
//    1. All UI controls (built in code, no .Designer.cs)
//    2. Button click handlers for Voltage and Current
//    3. Subscription to Logger events → activity log panel
//    4. Subscription to MessageSender health event → status label
//    5. A 1-second UI timer to refresh queue size counters
//    6. Clean shutdown in OnFormClosed
//
//  SERVICE WIRING (no DI container – plain constructors):
//
//    HttpClient
//        │
//        ├──► HealthChecker
//        │
//        └──► MessageSender ◄── ProducerQueue (voltage)
//                           ◄── ProducerQueue (current)
//                           ◄── HealthChecker
//
//  DiskStore  – static class, no instance needed
//  Logger     – static class, no instance needed
// ============================================================

using System;
using System.Drawing;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Windows.Forms;

public class Form1 : Form
{
    // ==========================================================
    //  UI CONTROLS
    // ==========================================================

    // Voltage section
    private TextBox txtVoltage = null!;
    private Button btnSendVoltage = null!;

    // Current section
    private TextBox txtCurrent = null!;
    private Button btnSendCurrent = null!;

    // Status indicators
    private Label lblStatus = null!;   // "RabbitMQ: ONLINE / OFFLINE"
    private Label lblVoltageQueue = null!;   // "Voltage Queue: 3"
    private Label lblCurrentQueue = null!;   // "Current Queue: 0"
    private Label lblVoltageFile = null!;   // "Voltage.txt: 0 records"
    private Label lblCurrentFile = null!;   // "Current.txt: 0 records"

    // Activity log
    private RichTextBox rtbLog = null!;

    // ==========================================================
    //  SERVICES
    // ==========================================================

    private readonly HttpClient _http;
    private readonly HealthChecker _health;
    private readonly ProducerQueue _voltageQueue;
    private readonly ProducerQueue _currentQueue;
    private readonly MessageSender _sender;

    // Ticks every 1 second to refresh counters in the UI
    private readonly System.Windows.Forms.Timer _uiTimer;

    // ==========================================================
    //  CONSTRUCTOR
    // ==========================================================

    public Form1()
    {
        // ---- Wire up services --------------------------------
        _http = CreateHttpClient();
        _health = new HealthChecker(_http);
        _voltageQueue = new ProducerQueue();
        _currentQueue = new ProducerQueue();
        _sender = new MessageSender(_http, _health, _voltageQueue, _currentQueue);

        // ---- Build the UI ------------------------------------
        BuildUI();

        // ---- Subscribe to Logger (fires on any thread) -------
        // GUARD: IsHandleCreated must be true before BeginInvoke.
        // The constructor runs BEFORE the OS window handle exists.
        // Calling BeginInvoke without this guard throws:
        //   "Invoke or BeginInvoke cannot be called on a control
        //    until the window handle has been created."
        // Early log calls (from Start() or the worker startup) are
        // silently dropped here; they appear in logs.txt instead.
        Logger.OnLog += entry =>
        {
            if (IsHandleCreated)
                BeginInvoke(() =>
                {
                    try
                    {
                        AppendLog(entry);
                    }
                    catch (Exception ex)
                    {
                        // temporarily log somewhere else
                        Console.WriteLine(ex.Message);
                    }
                });
        };

        // ---- Subscribe to health changes from sender ---------
        // Same guard for the same reason.
        _sender.ServerHealthChanged += isUp =>
        {
            if (IsHandleCreated)
                BeginInvoke(() => SetStatus(
                    isUp ? "RabbitMQ: ONLINE  ✓" : "RabbitMQ: OFFLINE  ✗",
                    isUp ? Color.DarkGreen : Color.Crimson));
        };

        // ---- UI refresh timer (queue sizes + file record counts)
        _uiTimer = new System.Windows.Forms.Timer { Interval = 1000 };
        _uiTimer.Tick += async (_, _) =>
        {
            lblVoltageQueue.Text = $"Voltage Queue : {_voltageQueue.Count} msg";
            lblCurrentQueue.Text = $"Current Queue : {_currentQueue.Count} msg";

            int vf = await DiskStore.CountAsync(DiskStore.VoltageFile);
            int cf = await DiskStore.CountAsync(DiskStore.CurrentFile);
            lblVoltageFile.Text = $"Voltage.txt   : {vf} records";
            lblCurrentFile.Text = $"Current.txt   : {cf} records";
        };
        _uiTimer.Start();

        // ---- Defer worker start to the Load event -----------
        // The Load event fires AFTER the OS window handle is created,
        // so BeginInvoke is safe from that point on.
        // Starting _sender.Start() here (in the constructor) would
        // trigger Logger.Log → OnLog → BeginInvoke BEFORE the handle
        // exists, which is exactly the crash we are fixing.
        Load += Form1_Load;
    }

    /// <summary>
    /// Fires once the form's OS window handle has been created.
    /// BeginInvoke / Invoke are safe to call from any thread now.
    /// </summary>
    private void Form1_Load(object? sender, EventArgs e)
    {
        _sender.Start();
        AppendLog("Application started. Background worker is running.");
        Logger.Log("Form1 ready.");
    }

    // ==========================================================
    //  HTTP CLIENT SETUP
    // ==========================================================

    /// <summary>
    /// Creates a shared HttpClient configured with:
    ///   • Basic Auth: guest / guest  (RabbitMQ default)
    ///   • 5-second timeout           (so health checks fail fast)
    /// </summary>
    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

        string creds = Convert.ToBase64String(Encoding.ASCII.GetBytes("guest:guest"));
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic", creds);

        return client;
    }

    // ==========================================================
    //  BUTTON HANDLERS
    // ==========================================================

    /// <summary>
    /// "Send Voltage" clicked.
    ///
    /// 1. Validate input.
    /// 2. Build inner JSON with meterId, voltage, timestamp.
    /// 3. Attempt to enqueue in ProducerQueue.
    ///    • If queue is full (backpressure) → save to Voltage.txt directly.
    /// </summary>
    private async void BtnSendVoltage_Click(object? sender, EventArgs e)
    {
        if (!double.TryParse(txtVoltage.Text.Trim(), out double voltage))
        {
            MessageBox.Show(
                "Please enter a valid numeric voltage value.\nExample: 230.5",
                "Validation Error", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            txtVoltage.Focus();
            return;
        }

        // Build the inner JSON (this is what eventually lands in the RabbitMQ queue)
        string timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string json = $"{{\"meterId\":105,\"voltage\":{voltage},\"timestamp\":\"{timestamp}\"}}";

        // Try to add to the in-memory producer queue
        if (_voltageQueue.TryEnqueue(json))
        {
            AppendLog($"[UI] Voltage {voltage} V → enqueued (queue size: {_voltageQueue.Count})");
        }
        else
        {
            // ── BACKPRESSURE PATH ──────────────────────────────────
            // Queue is full (10,000 messages).  Save directly to disk
            // so the message is not lost.  The worker will pick it up
            // from Voltage.txt when the queue drains.
            await DiskStore.SaveAsync(DiskStore.VoltageFile, json);
            AppendLog($"[UI] ⚠ BACKPRESSURE: Voltage {voltage} V → saved to Voltage.txt");

            MessageBox.Show(
                $"Message buffer is full ({ProducerQueue.MaxCapacity:N0} messages).\n\n" +
                "Your reading has been saved to Voltage.txt and will be sent\n" +
                "automatically once the queue has space again.",
                "Buffer Full – Message Persisted",
                MessageBoxButtons.OK, MessageBoxIcon.Warning);
        }
    }

    /// <summary>
    /// "Send Current" clicked.  Same pattern as voltage.
    /// </summary>
    private async void BtnSendCurrent_Click(object? sender, EventArgs e)
    {
        if (!double.TryParse(txtCurrent.Text.Trim(), out double current))
        {
            MessageBox.Show(
                "Please enter a valid numeric current value.\nExample: 15.3",
                "Validation Error", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            txtCurrent.Focus();
            return;
        }

        string timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string json = $"{{\"meterId\":105,\"current\":{current},\"timestamp\":\"{timestamp}\"}}";

        if (_currentQueue.TryEnqueue(json))
        {
            AppendLog($"[UI] Current {current} A → enqueued (queue size: {_currentQueue.Count})");
        }
        else
        {
            await DiskStore.SaveAsync(DiskStore.CurrentFile, json);
            AppendLog($"[UI] ⚠ BACKPRESSURE: Current {current} A → saved to Current.txt");

            MessageBox.Show(
                $"Message buffer is full ({ProducerQueue.MaxCapacity:N0} messages).\n\n" +
                "Your reading has been saved to Current.txt and will be sent\n" +
                "automatically once the queue has space again.",
                "Buffer Full – Message Persisted",
                MessageBoxButtons.OK, MessageBoxIcon.Warning);
        }
    }

    // ==========================================================
    //  UI HELPERS
    // ==========================================================

    /// <summary>Appends a line to the on-screen activity log. Call on UI thread.</summary>
    private void AppendLog(string entry)
    {
        // rtbLog is a RichTextBox with a dark terminal look

        try
        {
            rtbLog.AppendText(entry + Environment.NewLine);
            rtbLog.ScrollToCaret();

            // Keep log panel manageable – trim oldest lines when over 500
            if (rtbLog.Lines.Length > 500)
            {
                var lines = rtbLog.Lines;

                if (lines.Length > 100)
                {
                    rtbLog.Lines = lines.Skip(100).ToArray();
                }
            }
        }
        catch (Exception ex)
        {

        }
    }

    /// <summary>Updates the status label text and colour.</summary>
    private void SetStatus(string message, Color color)
    {
        lblStatus.Text = message;
        lblStatus.ForeColor = color;
    }

    // ==========================================================
    //  UI CONSTRUCTION  (no .Designer.cs – everything in code)
    // ==========================================================

    /// <summary>
    /// Builds the entire form layout in code.
    ///
    ///  ┌──────────────────────────────────────────────────────┐
    ///  │  ⚡ Voltage   [____230.5____]  [Send Voltage]         │
    ///  │  🔌 Current   [_____15.3___]  [Send Current]         │
    ///  ├──────────────────────────────────────────────────────┤
    ///  │  Status: RabbitMQ: ONLINE ✓                          │
    ///  │  Voltage Queue: 0 msg   Voltage.txt: 0 records       │
    ///  │  Current Queue: 0 msg   Current.txt: 0 records       │
    ///  ├──────────────────────────────────────────────────────┤
    ///  │  📋 Activity Log                                      │
    ///  │  [HH:mm:ss] [Worker] ✓ Queue message sent. ...       │
    ///  │  [HH:mm:ss] [Health] RabbitMQ is UP ✓                │
    ///  └──────────────────────────────────────────────────────┘
    /// </summary>
    private void BuildUI()
    {
        // ---- Form -----------------------------------------------
        Text = "Meter Sender – Reliable RabbitMQ Producer  (.NET 8)";
        ClientSize = new Size(620, 640);
        StartPosition = FormStartPosition.CenterScreen;
        FormBorderStyle = FormBorderStyle.FixedSingle;
        MaximizeBox = false;
        BackColor = Color.FromArgb(240, 242, 248);

        // ---- Voltage group --------------------------------------
        GroupBox grpV = MakeGroup("⚡  Voltage", 16, 16, 588, 74);

        var lblV = MakeLabel("Voltage (V):", 12, 30, 90, 22);
        txtVoltage = MakeTextBox(108, 27, 200, 26, "e.g. 230.5");
        btnSendVoltage = MakeButton("Send Voltage", 320, 26, 130, 30, Color.FromArgb(25, 105, 210));
        btnSendVoltage.Click += BtnSendVoltage_Click;
        grpV.Controls.AddRange(new Control[] { lblV, txtVoltage, btnSendVoltage });

        // ---- Current group --------------------------------------
        GroupBox grpC = MakeGroup("🔌  Current", 16, 100, 588, 74);

        var lblC = MakeLabel("Current (A):", 12, 30, 90, 22);
        txtCurrent = MakeTextBox(108, 27, 200, 26, "e.g. 15.3");
        btnSendCurrent = MakeButton("Send Current", 320, 26, 130, 30, Color.FromArgb(30, 140, 60));
        btnSendCurrent.Click += BtnSendCurrent_Click;
        grpC.Controls.AddRange(new Control[] { lblC, txtCurrent, btnSendCurrent });

        // ---- Status panel ---------------------------------------
        GroupBox grpS = MakeGroup("📊  Status", 16, 184, 588, 100);

        lblStatus = MakeLabel("RabbitMQ: checking...", 12, 22, 560, 22);
        lblStatus.Font = new Font("Segoe UI", 10f, FontStyle.Bold);
        lblStatus.ForeColor = Color.DimGray;

        lblVoltageQueue = MakeLabel("Voltage Queue : 0 msg", 12, 48, 270, 20);
        lblCurrentQueue = MakeLabel("Current Queue : 0 msg", 300, 48, 270, 20);
        lblVoltageFile = MakeLabel("Voltage.txt   : 0 records", 12, 70, 270, 20);
        lblCurrentFile = MakeLabel("Current.txt   : 0 records", 300, 70, 270, 20);

        foreach (var l in new[] { lblVoltageQueue, lblCurrentQueue, lblVoltageFile, lblCurrentFile })
            l.Font = new Font("Consolas", 8.5f);

        grpS.Controls.AddRange(new Control[]
            { lblStatus, lblVoltageQueue, lblCurrentQueue, lblVoltageFile, lblCurrentFile });

        // ---- Activity log ---------------------------------------
        GroupBox grpL = MakeGroup("📋  Activity Log", 16, 294, 588, 320);

        rtbLog = new RichTextBox
        {
            Location = new Point(10, 22),
            Size = new Size(568, 290),
            ReadOnly = true,
            BackColor = Color.FromArgb(15, 15, 25),
            ForeColor = Color.FromArgb(170, 255, 170),
            Font = new Font("Consolas", 8f),
            BorderStyle = BorderStyle.None,
            ScrollBars = RichTextBoxScrollBars.Vertical
        };
        grpL.Controls.Add(rtbLog);

        // ---- Add all to form ------------------------------------
        Controls.AddRange(new Control[] { grpV, grpC, grpS, grpL });
    }

    // ==========================================================
    //  CLEANUP
    // ==========================================================

    protected override void OnFormClosed(FormClosedEventArgs e)
    {
        _uiTimer.Stop();
        _uiTimer.Dispose();
        _sender.Stop();
        _http.Dispose();
        Logger.Log("Application closed. Resources disposed.");
        base.OnFormClosed(e);
    }

    // ==========================================================
    //  UI FACTORY HELPERS  (reduce repetition in BuildUI)
    // ==========================================================

    private static GroupBox MakeGroup(string text, int x, int y, int w, int h) =>
        new GroupBox
        {
            Text = text,
            Location = new Point(x, y),
            Size = new Size(w, h),
            Font = new Font("Segoe UI", 9.5f, FontStyle.Bold)
        };

    private static Label MakeLabel(string text, int x, int y, int w, int h) =>
        new Label
        {
            Text = text,
            Location = new Point(x, y),
            Size = new Size(w, h),
            Font = new Font("Segoe UI", 9f),
            TextAlign = ContentAlignment.MiddleLeft
        };

    private static TextBox MakeTextBox(int x, int y, int w, int h, string placeholder) =>
        new TextBox
        {
            Location = new Point(x, y),
            Size = new Size(w, h),
            Font = new Font("Segoe UI", 10f),
            PlaceholderText = placeholder
        };

    private static Button MakeButton(string text, int x, int y, int w, int h, Color color) =>
        new Button
        {
            Text = text,
            Location = new Point(x, y),
            Size = new Size(w, h),
            BackColor = color,
            ForeColor = Color.White,
            FlatStyle = FlatStyle.Flat,
            Font = new Font("Segoe UI", 9f, FontStyle.Bold),
            Cursor = Cursors.Hand
        };
}
