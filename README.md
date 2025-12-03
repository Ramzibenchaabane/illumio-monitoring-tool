# Illumio Monitoring Tool

A Python-based monitoring tool for tracking Illumio deployment progress by reconciling data between Illumio PCE and ServiceNow CMDB.

## Features

- **Async API Integration**: High-performance async connectors for both Illumio PCE and ServiceNow CMDB
- **Large-Scale Support**: Designed to handle 100,000+ servers with parallel API requests
- **Data Reconciliation**: Automatic matching of workloads to CMDB servers by hostname
- **Excel Extractions**: Detailed data exports with multiple worksheets and formatting
- **PDF Reports**: Professional reports with Accenture branding and charts
- **Resilient Execution**: Continues with Illumio-only analysis if ServiceNow is unavailable

## Installation

### Prerequisites

- Python 3.9 or higher
- Access to Illumio PCE API
- Access to ServiceNow API (optional)

### Setup

1. Clone or download the project:
```bash
cd illumio-monitoring-tool
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure the tool:
```bash
cp config/config.yaml config/config.yaml.local
# Edit config/config.yaml.local with your settings
```

5. Set environment variables for API credentials:
```bash
export ILLUMIO_API_USER="api_xxxxx"
export ILLUMIO_API_SECRET="your_secret_here"
export SNOW_API_USER="your_username"
export SNOW_API_KEY="your_api_key"
```

## Configuration

Edit `config/config.yaml` to configure:

### Illumio PCE Settings
```yaml
illumio:
  pce_url: "https://pce.company.com"
  org_id: "1"
  api_user: "${ILLUMIO_API_USER}"
  api_secret: "${ILLUMIO_API_SECRET}"
  port: 8443
  page_size: 500
  max_concurrent_requests: 15
```

### ServiceNow Settings
```yaml
servicenow:
  instance_url: "https://company.service-now.com"
  api_user: "${SNOW_API_USER}"
  api_key: "${SNOW_API_KEY}"
  table: "cmdb_ci_server"
  page_size: 10000
```

### Filtering
```yaml
filtering:
  operating_entity_contains: "ACCENTURE"  # Filter CMDB by Operating Entity
```

## Usage

### Basic Execution
```bash
cd src
python main.py
```

### With Custom Config
```bash
python main.py --config /path/to/config.yaml
```

### Scheduled Execution (cron)
```bash
# Add to crontab for daily execution at 6 AM
0 6 * * * /path/to/venv/bin/python /path/to/src/main.py >> /var/log/illumio_monitor.log 2>&1
```

## Output Files

### Excel Extractions (`outputs/extracts/DD-MM-YYYY/`)

| File | Description |
|------|-------------|
| `illumio_workloads_DD-MM-YYYY.xlsx` | All workloads from Illumio PCE |
| `servicenow_servers_DD-MM-YYYY.xlsx` | All servers from CMDB (filtered) |
| `reconciliation_full_DD-MM-YYYY.xlsx` | Complete reconciliation data |
| `gap_not_deployed_DD-MM-YYYY.xlsx` | Servers without Illumio coverage |
| `shadow_it_DD-MM-YYYY.xlsx` | Workloads not in CMDB |
| `health_issues_DD-MM-YYYY.xlsx` | Offline and suspended agents |

### PDF Reports (`outputs/reports/DD-MM-YYYY/`)

| Report | Description |
|--------|-------------|
| `executive_summary_DD-MM-YYYY.pdf` | One-page summary with KPIs |
| `deployment_dashboard_DD-MM-YYYY.pdf` | Deployment progress charts |
| `agent_health_DD-MM-YYYY.pdf` | VEN status and health metrics |
| `gap_analysis_DD-MM-YYYY.pdf` | Detailed gap analysis |

## Reconciliation Logic

### Matching
Servers are matched by **normalized hostname** (uppercase, first segment before dot):
- `server01.domain.com` → `SERVER01`
- `SERVER-01` → `SERVER-01`

### Status Categories

| Status | Description |
|--------|-------------|
| `deployed_active` | In CMDB, VEN active and online |
| `deployed_offline` | In CMDB, VEN installed but offline |
| `deployed_suspended` | In CMDB, VEN suspended |
| `deployed_uninstalled` | In CMDB, VEN previously installed |
| `not_deployed` | In CMDB, not in Illumio |
| `not_in_cmdb` | In Illumio, not in CMDB (Shadow IT) |

## Logging

Logs are written to `logs/illumio_monitor.log` with automatic rotation.

Log levels can be configured: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

## Error Handling

- **Retry Logic**: Failed API requests are retried 3 times with exponential backoff
- **Rate Limiting**: Automatic pause and retry on HTTP 429
- **Graceful Degradation**: If ServiceNow is unavailable, Illumio-only analysis continues

## Project Structure

```
illumio-monitoring-tool/
├── config/
│   └── config.yaml          # Configuration file
├── src/
│   ├── connectors/          # API connectors
│   │   ├── base_connector.py
│   │   ├── illumio_connector.py
│   │   └── servicenow_connector.py
│   ├── processors/          # Data processing
│   │   ├── normalizer.py
│   │   └── reconciliation.py
│   ├── exporters/           # Output generation
│   │   ├── excel_exporter.py
│   │   └── pdf_report_generator.py
│   ├── utils/               # Utilities
│   │   ├── config_loader.py
│   │   └── logger.py
│   └── main.py              # Entry point
├── outputs/
│   ├── extracts/            # Excel files
│   └── reports/             # PDF files
├── logs/                    # Log files
├── assets/                  # Logo and branding
├── requirements.txt
└── README.md
```

## Customization

### Adding Custom Labels
The tool automatically extracts all Illumio labels, including custom ones. They appear as `label_<key>` columns.

### Modifying Reports
Edit `src/exporters/pdf_report_generator.py` to customize:
- Chart styles and colors
- Report sections
- Branding elements

### Adding Custom Fields
ServiceNow custom fields (prefixed `u_`) are automatically extracted and included in exports.

## Troubleshooting

### Connection Errors
- Verify API credentials and URLs
- Check network connectivity
- Ensure API user has required permissions

### Memory Issues
- Reduce `page_size` in configuration
- Reduce `max_concurrent_requests`
- Run on a machine with more RAM

### Missing Data
- Check Operating Entity filter matches expected servers
- Verify CMDB table name is correct
- Check Illumio org_id

## Support

For issues related to:
- **Illumio API**: Refer to Illumio documentation
- **ServiceNow API**: Refer to ServiceNow documentation
- **This tool**: Contact your internal development team

## License

Internal use only. Confidential.
