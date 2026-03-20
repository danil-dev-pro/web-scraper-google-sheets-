#!/usr/bin/env python3
"""
=============================================================================
 Web Scraper with Google Sheets Integration
 Stage 3: Google Sheets Export Module
=============================================================================
 Handles: authentication, spreadsheet creation, data upload,
          formatting (bold headers, auto-width), timestamps
=============================================================================
"""

import logging
import pandas as pd
from datetime import datetime

# Google Sheets libraries
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS AUTHENTICATION
# Uses a Service Account (credentials.json) to access Google Sheets API.
#
# How to get credentials:
#   1. Go to Google Cloud Console → https://console.cloud.google.com
#   2. Create a new project (or select an existing one)
#   3. Enable the "Google Sheets API" and "Google Drive API"
#   4. Go to "Credentials" → "Create credentials" → "Service Account"
#   5. Download the JSON key file and save it as "credentials.json"
#   6. Share your Google Sheet with the service account email
#      (the email looks like: your-bot@project-id.iam.gserviceaccount.com)
# ─────────────────────────────────────────────────────────────────────────────

# Required OAuth2 scopes for reading/writing Google Sheets and Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",    # Read/write sheets
    "https://www.googleapis.com/auth/drive",           # Create/manage files
]


def authenticate(credentials_file="credentials.json"):
    """
    Authenticates with Google API using a Service Account.

    Args:
        credentials_file (str): Path to the credentials JSON file.

    Returns:
        gspread.Client: Authenticated client for Google Sheets.

    Raises:
        FileNotFoundError: If the credentials file doesn't exist.
        Exception: If authentication fails for any reason.

    Example:
        >>> client = authenticate("credentials.json")
        >>> print(client.auth.service_account_email)
        'my-bot@my-project.iam.gserviceaccount.com'
    """
    logger.info(f"Authenticating with Google API...")
    logger.info(f"Credentials file: {credentials_file}")

    try:
        # Load credentials from the JSON file
        creds = Credentials.from_service_account_file(
            credentials_file,
            scopes=SCOPES,
        )

        # Create an authenticated gspread client
        client = gspread.authorize(creds)

        logger.info(f"✓ Authentication successful!")
        logger.info(f"  Service account: {creds.service_account_email}")
        return client

    except FileNotFoundError:
        logger.error(
            f"Credentials file not found: '{credentials_file}'\n"
            f"  → Download it from Google Cloud Console.\n"
            f"  → See README.md for instructions."
        )
        raise

    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# OPEN OR CREATE A SPREADSHEET
# If a spreadsheet ID is provided, opens the existing one.
# Otherwise, creates a brand new spreadsheet.
# ─────────────────────────────────────────────────────────────────────────────
def get_or_create_spreadsheet(client, spreadsheet_id=None, title=None):
    """
    Opens an existing spreadsheet or creates a new one.

    Args:
        client (gspread.Client):  Authenticated gspread client.
        spreadsheet_id (str):     ID of an existing spreadsheet (optional).
        title (str):              Title for a new spreadsheet (optional).

    Returns:
        gspread.Spreadsheet: The opened or created spreadsheet object.

    How to find a spreadsheet ID:
        Open your Google Sheet in a browser. The URL looks like:
        https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
        The SPREADSHEET_ID is the long string between /d/ and /edit.

    Examples:
        # Open existing spreadsheet:
        >>> sheet = get_or_create_spreadsheet(client, spreadsheet_id="1abc...")

        # Create new spreadsheet:
        >>> sheet = get_or_create_spreadsheet(client, title="My Scraper Data")
    """
    # ── Option 1: Open an existing spreadsheet by ID ────────────────────
    if spreadsheet_id:
        try:
            logger.info(f"Opening existing spreadsheet: {spreadsheet_id}")
            spreadsheet = client.open_by_key(spreadsheet_id)
            logger.info(f"✓ Opened: '{spreadsheet.title}'")
            return spreadsheet

        except gspread.SpreadsheetNotFound:
            logger.error(
                f"Spreadsheet not found: '{spreadsheet_id}'\n"
                f"  → Make sure the spreadsheet exists.\n"
                f"  → Share it with the service account email."
            )
            raise

        except gspread.exceptions.APIError as e:
            logger.error(f"API error opening spreadsheet: {e}")
            raise

    # ── Option 2: Create a new spreadsheet ──────────────────────────────
    if title is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        title = f"Scraped_Data_{timestamp}"

    logger.info(f"Creating new spreadsheet: '{title}'")
    spreadsheet = client.create(title)

    # Make the spreadsheet accessible (share with anyone who has the link)
    spreadsheet.share(None, perm_type="anyone", role="reader")

    logger.info(f"✓ Created new spreadsheet: '{title}'")
    logger.info(f"  ID:  {spreadsheet.id}")
    logger.info(f"  URL: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")

    return spreadsheet


# ─────────────────────────────────────────────────────────────────────────────
# UPLOAD DATA TO GOOGLE SHEETS
# Takes a Pandas DataFrame and writes it to the spreadsheet.
# Includes headers, data rows, and a timestamp row.
# ─────────────────────────────────────────────────────────────────────────────
def upload_data(spreadsheet, df, worksheet_name="Scraped Data"):
    """
    Uploads a Pandas DataFrame to a Google Sheets worksheet.

    Steps:
        1. Select (or create) a worksheet
        2. Clear any existing data
        3. Write column headers (row 1)
        4. Write all data rows (rows 2+)
        5. Add a timestamp row at the bottom

    Args:
        spreadsheet (gspread.Spreadsheet): The target spreadsheet.
        df (pandas.DataFrame):             The data to upload.
        worksheet_name (str):              Name of the worksheet tab.

    Returns:
        gspread.Worksheet: The worksheet where data was written.
    """
    if df.empty:
        logger.warning("DataFrame is empty — nothing to upload")
        return None

    logger.info(f"Uploading {len(df)} rows to worksheet '{worksheet_name}'...")

    # ── Step 1: Get or create the worksheet ─────────────────────────────
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        logger.info(f"  Found existing worksheet: '{worksheet_name}'")
    except gspread.WorksheetNotFound:
        # Create a new worksheet with enough rows and columns
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=len(df) + 10,     # Extra rows for timestamp + buffer
            cols=len(df.columns) + 1,
        )
        logger.info(f"  Created new worksheet: '{worksheet_name}'")

        # Delete the default "Sheet1" if it exists and we created a new one
        try:
            default_sheet = spreadsheet.worksheet("Sheet1")
            if default_sheet.id != worksheet.id:
                spreadsheet.del_worksheet(default_sheet)
                logger.info("  Removed default 'Sheet1'")
        except (gspread.WorksheetNotFound, gspread.exceptions.APIError):
            pass  # No default sheet to remove — that's fine

    # ── Step 2: Clear existing data ─────────────────────────────────────
    worksheet.clear()
    logger.info("  Cleared existing data")

    # ── Step 3: Prepare data for upload ─────────────────────────────────
    # Convert DataFrame to a list of lists (gspread format)
    headers = df.columns.tolist()

    # Convert all values to strings to avoid serialization issues
    data_rows = df.astype(str).values.tolist()

    # Combine headers + data rows
    all_rows = [headers] + data_rows

    # ── Step 4: Write all data at once (batch update = faster) ──────────
    worksheet.update(
        range_name=f"A1",
        values=all_rows,
    )
    logger.info(f"  ✓ Uploaded {len(data_rows)} data rows + 1 header row")

    # ── Step 5: Add timestamp row ───────────────────────────────────────
    timestamp_row = len(all_rows) + 2  # Leave one empty row
    timestamp_text = f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    worksheet.update_cell(timestamp_row, 1, timestamp_text)
    logger.info(f"  ✓ Added timestamp at row {timestamp_row}")

    return worksheet


# ─────────────────────────────────────────────────────────────────────────────
# FORMAT THE SPREADSHEET
# Applies visual formatting:
#   - Bold headers (row 1)
#   - Auto-width columns based on content length
#   - Freeze the header row
#   - Color the header row background
# ─────────────────────────────────────────────────────────────────────────────
def format_worksheet(worksheet, df):
    """
    Applies professional formatting to the worksheet.

    Formatting includes:
        1. Bold + colored header row
        2. Frozen header row (stays visible when scrolling)
        3. Auto-width columns (based on content length)
        4. Number format for price column
        5. Alternating row colors (zebra striping) — optional

    Args:
        worksheet (gspread.Worksheet): The worksheet to format.
        df (pandas.DataFrame):         The data (used for column widths).
    """
    if worksheet is None:
        return

    logger.info("Applying formatting...")

    # We need the spreadsheet ID and worksheet ID for batch updates
    spreadsheet_id = worksheet.spreadsheet.id
    sheet_id = worksheet.id

    # ── Build a list of formatting requests ─────────────────────────────
    requests_list = []

    # ── 1. Bold + colored header row ────────────────────────────────────
    # Make the first row (headers) bold with a dark blue background
    header_format_request = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,           # Only row 1 (0-indexed)
                "startColumnIndex": 0,
                "endColumnIndex": len(df.columns),
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.15,
                        "green": 0.30,
                        "blue": 0.53,       # Dark blue
                    },
                    "textFormat": {
                        "bold": True,
                        "fontSize": 11,
                        "foregroundColor": {
                            "red": 1.0,
                            "green": 1.0,
                            "blue": 1.0,     # White text
                        },
                    },
                    "horizontalAlignment": "CENTER",
                },
            },
            "fields": (
                "userEnteredFormat(backgroundColor,textFormat,"
                "horizontalAlignment)"
            ),
        }
    }
    requests_list.append(header_format_request)

    # ── 2. Freeze the header row ────────────────────────────────────────
    # The header row stays visible when you scroll down
    freeze_request = {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {
                    "frozenRowCount": 1,     # Freeze 1 row (the header)
                },
            },
            "fields": "gridProperties.frozenRowCount",
        }
    }
    requests_list.append(freeze_request)

    # ── 3. Auto-width columns ──────────────────────────────────────────
    # Calculate the optimal width for each column based on content
    for col_idx, col_name in enumerate(df.columns):
        # Calculate max content length in this column
        max_content_len = max(
            len(str(col_name)),                          # Header length
            df[col_name].astype(str).str.len().max(),    # Max data length
        )

        # Convert character count to pixel width (approximate)
        # Minimum: 80px, Maximum: 400px
        pixel_width = min(max(int(max_content_len * 8), 80), 400)

        resize_request = {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": col_idx,
                    "endIndex": col_idx + 1,
                },
                "properties": {
                    "pixelSize": pixel_width,
                },
                "fields": "pixelSize",
            }
        }
        requests_list.append(resize_request)

    # ── 4. Number format for the price column ──────────────────────────
    # Find the price column index
    price_col_idx = None
    for idx, col in enumerate(df.columns):
        if col.lower() == "price":
            price_col_idx = idx
            break

    if price_col_idx is not None:
        price_format_request = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,         # Skip header
                    "endRowIndex": len(df) + 1,
                    "startColumnIndex": price_col_idx,
                    "endColumnIndex": price_col_idx + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "$#,##0.00",
                        },
                    },
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
        requests_list.append(price_format_request)

    # ── 5. Alternating row colors (zebra striping) ─────────────────────
    # Light gray background for even rows — easier to read
    banding_request = {
        "addBanding": {
            "bandedRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": len(df) + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(df.columns),
                },
                "rowProperties": {
                    "headerColor": {
                        "red": 0.15,
                        "green": 0.30,
                        "blue": 0.53,
                    },
                    "firstBandColor": {
                        "red": 1.0,
                        "green": 1.0,
                        "blue": 1.0,       # White
                    },
                    "secondBandColor": {
                        "red": 0.94,
                        "green": 0.96,
                        "blue": 0.98,       # Light blue-gray
                    },
                },
            }
        }
    }
    requests_list.append(banding_request)

    # ── Execute all formatting requests in a single batch ───────────────
    try:
        worksheet.spreadsheet.batch_update({"requests": requests_list})
        logger.info("  ✓ Bold headers applied")
        logger.info("  ✓ Header row frozen")
        logger.info("  ✓ Column widths auto-adjusted")
        logger.info("  ✓ Price column formatted as currency")
        logger.info("  ✓ Alternating row colors applied")

    except gspread.exceptions.APIError as e:
        logger.warning(f"  Some formatting could not be applied: {e}")
        logger.info("  (Data was uploaded successfully — only styling failed)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXPORT FUNCTION
# High-level function that combines all steps:
# authenticate → open/create → upload → format
# This is the function you call from main.py
# ─────────────────────────────────────────────────────────────────────────────
def export_to_sheets(
    df,
    credentials_file="credentials.json",
    spreadsheet_id=None,
    spreadsheet_title=None,
    worksheet_name="Scraped Data",
):
    """
    Complete export pipeline: authenticate → open/create → upload → format.

    This is the main entry point for Google Sheets export.

    Args:
        df (pandas.DataFrame):     Data to export.
        credentials_file (str):    Path to Google API credentials JSON.
        spreadsheet_id (str):      ID of existing spreadsheet (optional).
        spreadsheet_title (str):   Title for new spreadsheet (optional).
        worksheet_name (str):      Name of the worksheet tab.

    Returns:
        dict: Export result with spreadsheet URL and metadata.

    Example:
        >>> from sheets_export import export_to_sheets
        >>> result = export_to_sheets(
        ...     df=my_dataframe,
        ...     credentials_file="credentials.json",
        ...     spreadsheet_title="My Data Export",
        ... )
        >>> print(result["url"])
        'https://docs.google.com/spreadsheets/d/abc123...'
    """
    logger.info("=" * 60)
    logger.info("Starting Google Sheets export...")
    logger.info("=" * 60)

    # ── Step 1: Authenticate ────────────────────────────────────────────
    client = authenticate(credentials_file)

    # ── Step 2: Open or create spreadsheet ──────────────────────────────
    spreadsheet = get_or_create_spreadsheet(
        client,
        spreadsheet_id=spreadsheet_id,
        title=spreadsheet_title,
    )

    # ── Step 3: Upload data ─────────────────────────────────────────────
    worksheet = upload_data(spreadsheet, df, worksheet_name)

    # ── Step 4: Apply formatting ────────────────────────────────────────
    if worksheet:
        format_worksheet(worksheet, df)

    # ── Build result ────────────────────────────────────────────────────
    result = {
        "success": True,
        "spreadsheet_id": spreadsheet.id,
        "spreadsheet_title": spreadsheet.title,
        "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}",
        "worksheet_name": worksheet_name,
        "rows_uploaded": len(df),
        "columns": df.columns.tolist(),
        "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    logger.info("=" * 60)
    logger.info("Google Sheets export complete!")
    logger.info(f"  Spreadsheet: {result['spreadsheet_title']}")
    logger.info(f"  URL:         {result['url']}")
    logger.info(f"  Rows:        {result['rows_uploaded']}")
    logger.info(f"  Worksheet:   {result['worksheet_name']}")
    logger.info("=" * 60)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE EXECUTION (DEMO)
# When run directly, demonstrates the export with sample data.
# Requires a valid credentials.json file.
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Set up logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print("\n" + "=" * 60)
    print("  STAGE 3: Google Sheets Export Demo")
    print("=" * 60 + "\n")

    # ── Option A: Use live scraped + processed data ─────────────────────
    try:
        from scraper import scrape_quotes
        from data_processor import process_data

        print("  Fetching live data (scraper → processor → sheets)...\n")
        raw_data = scrape_quotes(num_pages=2)
        df = process_data(raw_data)

    except ImportError:
        # ── Option B: Use sample DataFrame ──────────────────────────────
        print("  Using sample data (modules not available)...\n")
        df = pd.DataFrame({
            "title": ["Albert Einstein", "J.K. Rowling", "Steve Martin"],
            "description": [
                "The world as we have created it...",
                "It is our choices, Harry...",
                "A day without laughter is wasted.",
            ],
            "price": [15.99, 12.50, 0.00],
            "date": ["2025-01-15", "2025-01-15", "2025-01-15"],
            "link": [
                "https://quotes.toscrape.com/author/Albert-Einstein",
                "https://quotes.toscrape.com/author/J-K-Rowling",
                "https://quotes.toscrape.com/author/Steve-Martin",
            ],
            "tags": ["change, deep-thoughts", "abilities, choices", "humor"],
        })

    # ── Export to Google Sheets ─────────────────────────────────────────
    print(f"  DataFrame ready: {len(df)} rows × {len(df.columns)} columns\n")

    try:
        result = export_to_sheets(
            df=df,
            credentials_file="credentials.json",
            spreadsheet_title="Web Scraper Demo Export",
        )

        print(f"\n{'─' * 60}")
        print(f"  Export Result")
        print(f"{'─' * 60}")
        for key, value in result.items():
            print(f"  {key:20s}: {value}")
        print(f"{'─' * 60}\n")

    except FileNotFoundError:
        print("  ⚠ credentials.json not found!")
        print()
        print("  To use Google Sheets export, you need to:")
        print("  1. Go to https://console.cloud.google.com")
        print("  2. Create a project & enable Sheets API + Drive API")
        print("  3. Create a Service Account key (JSON)")
        print("  4. Save it as 'credentials.json' in this folder")
        print("  5. See README.md for detailed instructions")
        print()

    except Exception as e:
        print(f"  ✗ Export failed: {e}")
        print(f"  Check the logs above for details.\n")
