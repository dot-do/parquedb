/**
 * Compaction Dashboard
 *
 * Generates an HTML dashboard for monitoring compaction system health.
 * Uses Chart.js via CDN for lightweight charting with auto-refresh.
 */

import type {
  CompactionMetrics,
  DashboardConfig,
  HealthIndicator,
} from './types'
import { DEFAULT_DASHBOARD_CONFIG } from './types'

// =============================================================================
// Health Evaluation
// =============================================================================

/**
 * Evaluate overall health based on metrics and thresholds
 */
export function evaluateHealth(
  metrics: CompactionMetrics,
  config: DashboardConfig = DEFAULT_DASHBOARD_CONFIG
): HealthIndicator {
  const { thresholds } = config

  // Any stuck windows is immediately unhealthy
  if (metrics.windows_stuck >= thresholds.stuckWindowsUnhealthy) {
    return 'unhealthy'
  }

  // Check pending windows threshold
  if (metrics.windows_pending >= thresholds.pendingWindowsUnhealthy) {
    return 'unhealthy'
  }

  // Check window age threshold (convert ms to hours)
  const ageHours = metrics.oldest_window_age_ms / (1000 * 60 * 60)
  if (ageHours >= thresholds.windowAgeUnhealthyHours) {
    return 'unhealthy'
  }

  // Check degraded thresholds
  if (
    metrics.windows_pending >= thresholds.pendingWindowsDegraded ||
    ageHours >= thresholds.windowAgeDegradedHours
  ) {
    return 'degraded'
  }

  return 'healthy'
}

/**
 * Evaluate aggregated health across multiple namespaces
 */
export function evaluateAggregatedHealth(
  metricsByNamespace: Map<string, CompactionMetrics>,
  config: DashboardConfig = DEFAULT_DASHBOARD_CONFIG
): HealthIndicator {
  let worstHealth: HealthIndicator = 'healthy'

  for (const metrics of Array.from(metricsByNamespace.values())) {
    const health = evaluateHealth(metrics, config)
    if (health === 'unhealthy') {
      return 'unhealthy'
    }
    if (health === 'degraded') {
      worstHealth = 'degraded'
    }
  }

  return worstHealth
}

// =============================================================================
// Dashboard HTML Generation
// =============================================================================

/**
 * Generate the compaction dashboard HTML page
 *
 * @param baseUrl - Base URL for API endpoints
 * @param namespaces - List of namespaces to monitor
 * @param config - Dashboard configuration
 */
export function generateDashboardHtml(
  baseUrl: string,
  namespaces: string[],
  config: DashboardConfig = DEFAULT_DASHBOARD_CONFIG
): string {
  const namespacesJson = JSON.stringify(namespaces)
  const refreshInterval = config.refreshIntervalSeconds * 1000

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ParqueDB Compaction Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg-color: #1a1a2e;
      --card-bg: #16213e;
      --text-color: #eaeaea;
      --text-muted: #8892a0;
      --border-color: #2a3a5c;
      --healthy: #10b981;
      --degraded: #f59e0b;
      --unhealthy: #ef4444;
      --primary: #3b82f6;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
      background-color: var(--bg-color);
      color: var(--text-color);
      line-height: 1.6;
      padding: 20px;
    }

    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--border-color);
    }

    .header h1 {
      font-size: 24px;
      font-weight: 600;
    }

    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 16px;
      border-radius: 8px;
      font-weight: 500;
      font-size: 14px;
    }

    .status-badge.healthy {
      background-color: rgba(16, 185, 129, 0.1);
      color: var(--healthy);
      border: 1px solid var(--healthy);
    }

    .status-badge.degraded {
      background-color: rgba(245, 158, 11, 0.1);
      color: var(--degraded);
      border: 1px solid var(--degraded);
    }

    .status-badge.unhealthy {
      background-color: rgba(239, 68, 68, 0.1);
      color: var(--unhealthy);
      border: 1px solid var(--unhealthy);
    }

    .status-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      animation: pulse 2s infinite;
    }

    .status-dot.healthy { background-color: var(--healthy); }
    .status-dot.degraded { background-color: var(--degraded); }
    .status-dot.unhealthy { background-color: var(--unhealthy); }

    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.5; }
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }

    .card {
      background-color: var(--card-bg);
      border-radius: 12px;
      padding: 20px;
      border: 1px solid var(--border-color);
    }

    .card-title {
      font-size: 14px;
      color: var(--text-muted);
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .card-value {
      font-size: 32px;
      font-weight: 600;
    }

    .card-value.healthy { color: var(--healthy); }
    .card-value.degraded { color: var(--degraded); }
    .card-value.unhealthy { color: var(--unhealthy); }

    .card-subtitle {
      font-size: 12px;
      color: var(--text-muted);
      margin-top: 4px;
    }

    .chart-container {
      background-color: var(--card-bg);
      border-radius: 12px;
      padding: 20px;
      border: 1px solid var(--border-color);
      margin-bottom: 24px;
    }

    .chart-title {
      font-size: 16px;
      font-weight: 600;
      margin-bottom: 16px;
    }

    .chart-wrapper {
      height: 300px;
      position: relative;
    }

    .namespace-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
      gap: 16px;
    }

    .namespace-card {
      background-color: var(--card-bg);
      border-radius: 12px;
      padding: 20px;
      border: 1px solid var(--border-color);
    }

    .namespace-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 16px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--border-color);
    }

    .namespace-name {
      font-size: 18px;
      font-weight: 600;
    }

    .metrics-table {
      width: 100%;
      border-collapse: collapse;
    }

    .metrics-table td {
      padding: 8px 0;
      border-bottom: 1px solid var(--border-color);
    }

    .metrics-table td:first-child {
      color: var(--text-muted);
    }

    .metrics-table td:last-child {
      text-align: right;
      font-weight: 500;
    }

    .metrics-table tr:last-child td {
      border-bottom: none;
    }

    .refresh-info {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text-muted);
      font-size: 12px;
    }

    .spinner {
      width: 16px;
      height: 16px;
      border: 2px solid var(--border-color);
      border-top-color: var(--primary);
      border-radius: 50%;
      animation: spin 1s linear infinite;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    .loading {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 60px;
      color: var(--text-muted);
    }

    .loading .spinner {
      width: 32px;
      height: 32px;
      margin-bottom: 16px;
    }

    .error {
      background-color: rgba(239, 68, 68, 0.1);
      border: 1px solid var(--unhealthy);
      border-radius: 8px;
      padding: 16px;
      color: var(--unhealthy);
      margin-bottom: 24px;
    }

    .export-links {
      display: flex;
      gap: 16px;
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid var(--border-color);
    }

    .export-links a {
      color: var(--primary);
      text-decoration: none;
      font-size: 14px;
    }

    .export-links a:hover {
      text-decoration: underline;
    }
  </style>
</head>
<body>
  <div class="header">
    <h1>Compaction Dashboard</h1>
    <div style="display: flex; align-items: center; gap: 16px;">
      <div class="refresh-info">
        <div class="spinner" id="refresh-spinner" style="display: none;"></div>
        <span>Last updated: <span id="last-update">-</span></span>
      </div>
      <div class="status-badge" id="overall-status">
        <div class="status-dot"></div>
        <span>Loading...</span>
      </div>
    </div>
  </div>

  <div id="error-container"></div>

  <div class="grid" id="summary-cards">
    <div class="card">
      <div class="card-title">Windows Pending</div>
      <div class="card-value" id="total-pending">-</div>
      <div class="card-subtitle">Awaiting compaction</div>
    </div>
    <div class="card">
      <div class="card-title">Windows Processing</div>
      <div class="card-value" id="total-processing">-</div>
      <div class="card-subtitle">Currently compacting</div>
    </div>
    <div class="card">
      <div class="card-title">Files Pending</div>
      <div class="card-value" id="total-files">-</div>
      <div class="card-subtitle">Across all windows</div>
    </div>
    <div class="card">
      <div class="card-title">Oldest Window Age</div>
      <div class="card-value" id="oldest-age">-</div>
      <div class="card-subtitle">Hours since window closed</div>
    </div>
  </div>

  <div class="chart-container">
    <div class="chart-title">Windows Over Time</div>
    <div class="chart-wrapper">
      <canvas id="windows-chart"></canvas>
    </div>
  </div>

  <div class="namespace-grid" id="namespace-cards">
    <div class="loading">
      <div class="spinner"></div>
      <span>Loading namespace data...</span>
    </div>
  </div>

  <div class="export-links">
    <a href="${baseUrl}/compaction/metrics" target="_blank">Prometheus Metrics</a>
    <a href="${baseUrl}/compaction/metrics/json" target="_blank">JSON Time-Series</a>
    <a href="${baseUrl}/compaction/health?namespaces=${namespaces.join(',')}" target="_blank">Health API</a>
  </div>

  <script>
    const baseUrl = '${baseUrl}';
    const namespaces = ${namespacesJson};
    const refreshInterval = ${refreshInterval};

    // Chart configuration
    Chart.defaults.color = '#8892a0';
    Chart.defaults.borderColor = '#2a3a5c';

    let windowsChart = null;
    let metricsHistory = {};

    // Initialize chart
    function initChart() {
      const ctx = document.getElementById('windows-chart').getContext('2d');
      windowsChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: [],
          datasets: [
            {
              label: 'Pending',
              data: [],
              borderColor: '#3b82f6',
              backgroundColor: 'rgba(59, 130, 246, 0.1)',
              fill: true,
              tension: 0.4,
            },
            {
              label: 'Processing',
              data: [],
              borderColor: '#f59e0b',
              backgroundColor: 'rgba(245, 158, 11, 0.1)',
              fill: true,
              tension: 0.4,
            },
            {
              label: 'Stuck',
              data: [],
              borderColor: '#ef4444',
              backgroundColor: 'rgba(239, 68, 68, 0.1)',
              fill: true,
              tension: 0.4,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: {
            intersect: false,
            mode: 'index',
          },
          scales: {
            x: {
              grid: {
                display: false,
              },
            },
            y: {
              beginAtZero: true,
              grid: {
                color: '#2a3a5c',
              },
            },
          },
          plugins: {
            legend: {
              position: 'top',
            },
          },
        },
      });
    }

    // Update chart with new data
    function updateChart(metrics) {
      const now = new Date();
      const timeLabel = now.toLocaleTimeString();

      // Add new data point
      windowsChart.data.labels.push(timeLabel);
      windowsChart.data.datasets[0].data.push(metrics.total_pending || 0);
      windowsChart.data.datasets[1].data.push(metrics.total_processing || 0);
      windowsChart.data.datasets[2].data.push(metrics.total_stuck || 0);

      // Keep last 60 data points
      if (windowsChart.data.labels.length > 60) {
        windowsChart.data.labels.shift();
        windowsChart.data.datasets.forEach(ds => ds.data.shift());
      }

      windowsChart.update('none');
    }

    // Fetch metrics from API
    async function fetchMetrics() {
      const spinner = document.getElementById('refresh-spinner');
      spinner.style.display = 'block';

      try {
        // Fetch status for each namespace
        const results = await Promise.all(
          namespaces.map(async (ns) => {
            const response = await fetch(\`\${baseUrl}/compaction/status?namespace=\${ns}\`);
            if (!response.ok) {
              throw new Error(\`Failed to fetch status for \${ns}\`);
            }
            return response.json();
          })
        );

        // Aggregate metrics
        const aggregated = {
          total_pending: 0,
          total_processing: 0,
          total_dispatched: 0,
          total_files: 0,
          total_bytes: 0,
          total_stuck: 0,
          oldest_age: 0,
          by_namespace: {},
        };

        results.forEach((data, i) => {
          const ns = namespaces[i];
          aggregated.by_namespace[ns] = data;

          // Count window states
          let pending = 0, processing = 0, dispatched = 0, stuck = 0;
          (data.windows || []).forEach(w => {
            if (w.processingStatus.state === 'pending') pending++;
            else if (w.processingStatus.state === 'processing') {
              processing++;
              // Check if stuck (> 5 min in processing)
              if (Date.now() - w.processingStatus.startedAt > 5 * 60 * 1000) {
                stuck++;
              }
            }
            else if (w.processingStatus.state === 'dispatched') dispatched++;
          });

          aggregated.total_pending += pending;
          aggregated.total_processing += processing;
          aggregated.total_dispatched += dispatched;
          aggregated.total_stuck += stuck;
          aggregated.total_files += data.totalPendingFiles || 0;

          if (data.oldestWindowAge > aggregated.oldest_age) {
            aggregated.oldest_age = data.oldestWindowAge;
          }
        });

        updateDashboard(aggregated);
        updateChart(aggregated);

        document.getElementById('error-container').innerHTML = '';
      } catch (error) {
        console.error('Failed to fetch metrics:', error);
        document.getElementById('error-container').innerHTML = \`
          <div class="error">
            Error fetching metrics: \${error.message}
          </div>
        \`;
      } finally {
        spinner.style.display = 'none';
        document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
      }
    }

    // Update dashboard UI
    function updateDashboard(metrics) {
      // Update summary cards
      document.getElementById('total-pending').textContent = metrics.total_pending;
      document.getElementById('total-processing').textContent = metrics.total_processing;
      document.getElementById('total-files').textContent = metrics.total_files;

      // Format oldest age
      const ageHours = (metrics.oldest_age / (1000 * 60 * 60)).toFixed(1);
      document.getElementById('oldest-age').textContent = ageHours + 'h';

      // Color code based on thresholds
      const pendingEl = document.getElementById('total-pending');
      pendingEl.className = 'card-value';
      if (metrics.total_pending >= 50) pendingEl.classList.add('unhealthy');
      else if (metrics.total_pending >= 10) pendingEl.classList.add('degraded');
      else pendingEl.classList.add('healthy');

      const ageEl = document.getElementById('oldest-age');
      ageEl.className = 'card-value';
      if (parseFloat(ageHours) >= 6) ageEl.classList.add('unhealthy');
      else if (parseFloat(ageHours) >= 2) ageEl.classList.add('degraded');
      else ageEl.classList.add('healthy');

      // Update overall status
      const statusEl = document.getElementById('overall-status');
      const statusDot = statusEl.querySelector('.status-dot');
      const statusText = statusEl.querySelector('span');

      let status = 'healthy';
      if (metrics.total_stuck > 0 || metrics.total_pending >= 50 || parseFloat(ageHours) >= 6) {
        status = 'unhealthy';
      } else if (metrics.total_pending >= 10 || parseFloat(ageHours) >= 2) {
        status = 'degraded';
      }

      statusEl.className = 'status-badge ' + status;
      statusDot.className = 'status-dot ' + status;
      statusText.textContent = status.charAt(0).toUpperCase() + status.slice(1);

      // Update namespace cards
      const namespaceContainer = document.getElementById('namespace-cards');
      namespaceContainer.innerHTML = '';

      Object.entries(metrics.by_namespace).forEach(([ns, data]) => {
        // Count window states for this namespace
        let pending = 0, processing = 0, dispatched = 0;
        (data.windows || []).forEach(w => {
          if (w.processingStatus.state === 'pending') pending++;
          else if (w.processingStatus.state === 'processing') processing++;
          else if (w.processingStatus.state === 'dispatched') dispatched++;
        });

        const nsAgeHours = (data.oldestWindowAge / (1000 * 60 * 60)).toFixed(1);
        let nsStatus = 'healthy';
        if (data.windowsStuckInProcessing > 0 || pending >= 50 || parseFloat(nsAgeHours) >= 6) {
          nsStatus = 'unhealthy';
        } else if (pending >= 10 || parseFloat(nsAgeHours) >= 2) {
          nsStatus = 'degraded';
        }

        const card = document.createElement('div');
        card.className = 'namespace-card';
        card.innerHTML = \`
          <div class="namespace-header">
            <span class="namespace-name">\${ns}</span>
            <div class="status-badge \${nsStatus}">
              <div class="status-dot \${nsStatus}"></div>
              <span>\${nsStatus}</span>
            </div>
          </div>
          <table class="metrics-table">
            <tr>
              <td>Windows Pending</td>
              <td>\${pending}</td>
            </tr>
            <tr>
              <td>Windows Processing</td>
              <td>\${processing}</td>
            </tr>
            <tr>
              <td>Windows Dispatched</td>
              <td>\${dispatched}</td>
            </tr>
            <tr>
              <td>Total Files</td>
              <td>\${data.totalPendingFiles || 0}</td>
            </tr>
            <tr>
              <td>Oldest Window Age</td>
              <td>\${nsAgeHours}h</td>
            </tr>
            <tr>
              <td>Known Writers</td>
              <td>\${(data.knownWriters || []).length}</td>
            </tr>
            <tr>
              <td>Active Writers</td>
              <td>\${(data.activeWriters || []).length}</td>
            </tr>
          </table>
        \`;
        namespaceContainer.appendChild(card);
      });
    }

    // Initialize
    initChart();
    fetchMetrics();
    setInterval(fetchMetrics, refreshInterval);
  </script>
</body>
</html>`;
}
