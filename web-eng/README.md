# 🌐 Web Scraper → Google Sheets Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

A professional Python web scraping pipeline that collects data from websites and automatically exports it to Google Sheets with beautiful formatting. Built in 5 stages as a learning project.

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  SCRAPE  │ →  │  CLEAN   │ →  │  DEDUP   │ →  │ DataFrame│ →  │  EXPORT  │
│ 5 pages  │    │ validate │    │ by link  │    │  Pandas  │    │  Sheets  │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
```

---

## ✨ Features

| Feature | Description |
|---|---|
| **Web Scraping** | requests + BeautifulSoup4 with retry mechanism & User-Agent rotation |
| **Data Processing** | Text cleaning, price validation, deduplication via Pandas |
| **Google Sheets** | Auto-export with bold headers, auto-width columns, zebra striping |
| **CLI Interface** | Full argparse CLI with 15+ options |
| **Progress Bars** | tqdm-powered colored progress bars for each pipeline step |
| **Cron Scheduling** | Built-in `--cron-help` with copy-paste instructions |
| **Multiple Formats** | Export to CSV, JSON, XLSX, or Google Sheets |

---

## 📁 Project Structure

```
📁 web-scraper-to-sheets/
├── scraper.py               ← Core web scraping module
├── data_processor.py        ← Data cleaning & validation
├── sheets_export.py         ← Google Sheets integration
├── main.py                  ← CLI orchestrator (entry point)
├── requirements.txt         ← Python dependencies
├── credentials.json.example ← Google API key template
├── README.md                ← This file
└── index.html               ← Project dashboard (for portfolio)
```

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
# Clone the repository
git clone https://github.com/yourusername/web-scraper-to-sheets.git
cd web-scraper-to-sheets

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate        # Linux/macOS
# venv\Scripts\activate         # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Scraper (no Google Sheets)

```bash
# Default: scrape 5 pages, print results
python main.py

# Scrape 3 pages, save to CSV
python main.py --pages 3 --output data.csv

# Scrape and save to JSON
python main.py --output results.json

# Scrape and save to Excel
python main.py --output results.xlsx
```

### 3. Run with Google Sheets Export

```bash
# First, set up Google API credentials (see below)

# Export to a NEW Google Sheet
python main.py --export

# Export to an EXISTING Google Sheet
python main.py --export --sheet-id 1AbC_dEf_GhI_jKl_MnO_pQr_StU_vWx

# Custom sheet title
python main.py --export --sheet-title "Weekly Quotes Report"

# Full pipeline: scrape 10 pages, save CSV + export to Sheets
python main.py --pages 10 --output data.csv --export
```

---

## 📋 CLI Reference

```
Usage: python main.py [OPTIONS]

Scraping Options:
  --url URL              Base URL to scrape (default: https://quotes.toscrape.com)
  --pages N              Number of pages to scrape (default: 5, max: 100)

Output Options:
  --output, -o FILE      Save results to file (.csv, .json, .xlsx)

Google Sheets Options:
  --export               Export data to Google Sheets
  --sheet-id ID          ID of an existing Google Sheet to update
  --sheet-title TITLE    Title for a new Google Sheet
  --credentials FILE     Path to credentials.json (default: ./credentials.json)

General Options:
  --verbose, -v          Enable DEBUG logging
  --log-file FILE        Write logs to file (e.g., scraper.log)
  --cron-help            Show cron scheduling instructions
  --dry-run              Preview what would happen without executing
```

### Usage Examples

```bash
# ── Basic ──────────────────────────────────────────────────
python main.py                          # Quick scrape, 5 pages
python main.py --pages 3                # Scrape only 3 pages
python main.py --url https://example.com  # Custom URL

# ── Save to File ──────────────────────────────────────────
python main.py -o quotes.csv            # CSV output
python main.py -o quotes.json           # JSON output
python main.py -o quotes.xlsx           # Excel output

# ── Google Sheets ─────────────────────────────────────────
python main.py --export                 # New sheet (auto-named)
python main.py --export --sheet-id 1abc # Update existing sheet
python main.py --export --sheet-title "My Report"

# ── Debugging ─────────────────────────────────────────────
python main.py --dry-run                # Preview only
python main.py --verbose                # Show DEBUG messages
python main.py --log-file scraper.log   # Log to file

# ── Full Pipeline ─────────────────────────────────────────
python main.py --pages 10 --export -o data.csv --verbose
```

---

## 🔑 Google Sheets Setup (Step-by-Step)

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click **"Select a project"** → **"New Project"**
3. Name it (e.g., `web-scraper-sheets`) → **Create**

### Step 2: Enable APIs

1. In the sidebar: **APIs & Services** → **Library**
2. Search and enable:
   - ✅ **Google Sheets API**
   - ✅ **Google Drive API**

### Step 3: Create Service Account

1. Go to **APIs & Services** → **Credentials**
2. Click **"Create Credentials"** → **"Service Account"**
3. Name it (e.g., `scraper-bot`) → **Create**
4. Skip the optional steps → **Done**

### Step 4: Download Credentials

1. Click on the service account you just created
2. Go to **Keys** tab → **Add Key** → **Create new key**
3. Choose **JSON** → **Create**
4. Save the downloaded file as `credentials.json` in the project folder

### Step 5: Verify

```bash
# Check that the file exists
ls credentials.json

# Test the connection
python main.py --export --dry-run
```

> **⚠️ Security:** Never commit `credentials.json` to Git! Add it to `.gitignore`.

---

## ⏰ Cron Scheduling (Automation)

### Linux/macOS

```bash
# View cron help from the script
python main.py --cron-help

# Open crontab editor
crontab -e

# Add one of these lines:

# ── Every day at 9:00 AM ────────────────────────────────
0 9 * * * cd /path/to/project && /usr/bin/python3 main.py --export >> cron.log 2>&1

# ── Every 6 hours ───────────────────────────────────────
0 */6 * * * cd /path/to/project && /usr/bin/python3 main.py --export >> cron.log 2>&1

# ── Every Monday at 8:00 AM ─────────────────────────────
0 8 * * 1 cd /path/to/project && /usr/bin/python3 main.py --pages 10 --export >> cron.log 2>&1

# Verify cron job
crontab -l
```

### Windows (Task Scheduler)

1. Open **Task Scheduler** (`taskschd.msc`)
2. **Create Basic Task** → name it `Web Scraper`
3. Set trigger: Daily / Weekly / etc.
4. Action: **Start a program**
   - Program: `python`
   - Arguments: `main.py --pages 5 --export`
   - Start in: `C:\path\to\project`
5. Click **Finish** → right-click → **Run** to test

---

## 🏗️ Architecture

### Module Overview

```
main.py                     ← Entry point, CLI, orchestration
  ├── scraper.py            ← HTTP requests, HTML parsing
  │     ├── fetch_page()    ← GET request with retry & User-Agent rotation
  │     └── parse_quote()   ← Extract title, description, price, link
  │
  ├── data_processor.py     ← Data cleaning pipeline
  │     ├── clean_text()    ← Remove Unicode artifacts, collapse whitespace
  │     ├── validate_price()← Convert "$12.99" / "free" → float
  │     ├── deduplicate()   ← Remove dupes by link (O(n) set-based)
  │     └── process_data()  ← Full pipeline → Pandas DataFrame
  │
  └── sheets_export.py      ← Google Sheets API integration
        ├── authenticate()  ← Service Account OAuth2
        ├── get_or_create() ← Open existing or create new spreadsheet
        ├── upload_data()   ← Batch write headers + rows + timestamp
        └── format_sheet()  ← Bold headers, auto-width, zebra stripes
```

### Data Flow

```
[Website HTML]
       ↓
  fetch_page()          # HTTP GET with retry (3 attempts)
       ↓
  parse_quote()         # BeautifulSoup → dict
       ↓
  clean_text()          # Remove Unicode quotes, whitespace
       ↓
  validate_price()      # "$12.99" → 12.99
       ↓
  deduplicate()         # Remove duplicates by link
       ↓
  pd.DataFrame()        # Structured table with types
       ↓
  ┌──────────────┐
  │  Export to:   │
  ├──────────────┤
  │  • CSV file   │
  │  • JSON file  │
  │  • XLSX file  │
  │  • Google     │
  │    Sheets     │
  └──────────────┘
```

---

## 🧪 Testing Individual Modules

Each module can be tested independently:

```bash
# Test scraper only
python scraper.py
# → Scrapes 5 pages, prints results to console

# Test data processor only
python data_processor.py
# → Runs with sample data, shows cleaning/validation results

# Test Google Sheets export only
python sheets_export.py
# → Requires credentials.json, creates a test spreadsheet
```

---

## 📊 Sample Output

### Terminal Output

```
 ╔══════════════════════════════════════════════════════════════╗
 ║          🌐  WEB SCRAPER → GOOGLE SHEETS EXPORTER  📊       ║
 ║          Scrape · Process · Export · Automate                 ║
 ╚══════════════════════════════════════════════════════════════╝

  Configuration:
    URL:          https://quotes.toscrape.com
    Pages:        5
    Output file:  data.csv
    Google Sheets: Yes

  ┌─────────────────────────────────────────────────────────┐
  │  STEP 1: WEB SCRAPING                                   │
  │  Target: https://quotes.toscrape.com                     │
  └─────────────────────────────────────────────────────────┘

  Scraping pages: ████████████████████████████ 5/5  quotes=50

  ✅ Scraped 50 items from 5 pages

  ┌─────────────────────────────────────────────────────────┐
  │  STEP 2: DATA PROCESSING                                │
  │  Cleaning, validating, deduplicating                     │
  └─────────────────────────────────────────────────────────┘

  Processing data: ████████████████████████████ 5/5

  ✅ Processed 42 unique items
      Price range:     $5.25 – $35.80
      Unique authors:  36

  ╔══════════════════════════════════════════════════════════╗
  ║                   PIPELINE COMPLETE ✅                   ║
  ╠══════════════════════════════════════════════════════════╣
  ║  Started at       : 2025-06-15 14:30:00                 ║
  ║  URL              : https://quotes.toscrape.com         ║
  ║  Pages scraped    : 5                                   ║
  ║  Raw items        : 50                                  ║
  ║  Processed rows   : 42                                  ║
  ║  Saved to file    : data.csv                            ║
  ║  Google Sheet     : https://docs.google.com/spread...   ║
  ║  Total time       : 12.3 seconds                        ║
  ╚══════════════════════════════════════════════════════════╝
```

### CSV Output

| title | description | price | date | link | tags |
|---|---|---|---|---|---|
| Albert Einstein | The world as we have created it... | 24.57 | 2025-06-15 | /author/Albert-Einstein | change, thinking |
| J.K. Rowling | It is our choices, Harry... | 17.32 | 2025-06-15 | /author/J-K-Rowling | abilities, choices |
| Jane Austen | The person, be it gentleman... | 22.18 | 2025-06-15 | /author/Jane-Austen | books, humor |

### Google Sheets Result

The exported spreadsheet includes:
- ✅ Bold white headers on dark blue background
- ✅ Frozen header row (always visible when scrolling)
- ✅ Auto-width columns based on content
- ✅ Price column formatted as currency ($#,##0.00)
- ✅ Alternating row colors (zebra striping)
- ✅ Timestamp row at the bottom with last update time
- ✅ Public link sharing (anyone with link can view)

---

## 🛡️ Error Handling

| Scenario | Behavior |
|---|---|
| Network timeout | Retries 3 times with exponential backoff (2s, 4s, 6s) |
| HTTP 404 | Skips the page, continues to next |
| HTTP 429 (rate limit) | Waits and retries |
| Invalid HTML | Gracefully skips unparseable elements |
| No data scraped | Exits with clear error message |
| Missing credentials.json | Shows step-by-step setup instructions |
| Google API quota exceeded | Reports the error with retry suggestion |
| Ctrl+C interrupt | Graceful shutdown with partial results |

---

## 🔧 Customization

### Scraping a Different Website

Edit `scraper.py` → `parse_quote()` function to match the HTML structure of your target site:

```python
def parse_quote(element, base_url):
    """Customize this function for your target website."""
    return {
        "title":       element.find("h2", class_="product-title").text.strip(),
        "description": element.find("p", class_="description").text.strip(),
        "price":       element.find("span", class_="price").text.strip(),
        "date":        element.find("time")["datetime"],
        "link":        base_url + element.find("a")["href"],
    }
```

### Adding New Data Fields

1. Add the field in `scraper.py` → `parse_quote()`
2. Add it to `COLUMN_ORDER` in `data_processor.py`
3. The rest of the pipeline handles it automatically!

---

## 📜 License

This project is licensed under the MIT License — feel free to use it for learning, portfolio, or commercial projects.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing`)
5. Open a Pull Request

---

## 📌 Troubleshooting

### "ModuleNotFoundError: No module named 'requests'"
```bash
pip install -r requirements.txt
```

### "FileNotFoundError: credentials.json"
Follow the [Google Sheets Setup](#-google-sheets-setup-step-by-step) section above.

### "gspread.exceptions.APIError: 403"
- Make sure Google Sheets API is enabled in your Google Cloud project
- Make sure Google Drive API is enabled
- Check that the service account email has access to the spreadsheet

### "tqdm not found" warning
```bash
pip install tqdm
# The script works without tqdm, but progress bars won't be shown
```

### Cron job not running
1. Use full paths: `/usr/bin/python3` instead of `python3`
2. Add `cd /full/path/to/project &&` before the command
3. Check cron logs: `grep CRON /var/log/syslog`
4. Test the command manually first

---

<p align="center">
  Built with ❤️ as a 5-stage learning project<br/>
  <strong>Stage 1:</strong> Scraping · <strong>Stage 2:</strong> Processing · <strong>Stage 3:</strong> Google Sheets · <strong>Stage 4:</strong> CLI · <strong>Stage 5:</strong> Docs
</p>
