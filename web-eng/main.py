#!/usr/bin/env python3
"""
=============================================================================
 Web Scraper with Google Sheets Integration
 Stage 4: Main Orchestrator (CLI + Progress Bar + Automation)
=============================================================================
 Ties together all modules: scraper → processor → Google Sheets export.

 Usage examples:
   python main.py                                    # Default (5 pages, no export)
   python main.py --pages 3                          # Scrape 3 pages only
   python main.py --url https://quotes.toscrape.com  # Custom URL
   python main.py --export                           # Export to NEW Google Sheet
   python main.py --export --sheet-id 1abc...xyz     # Export to EXISTING sheet
   python main.py --output data.csv                  # Save to CSV file
   python main.py --pages 10 --export --output data.csv  # Full pipeline

 For cron scheduling, see the README.md or run:
   python main.py --cron-help
=============================================================================
"""

import argparse
import sys
import os
import time
import logging
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# Centralized logging configuration for the entire application.
# All modules (scraper, processor, sheets) use the same log format.
# ─────────────────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(verbose=False, log_file=None):
    """
    Configures logging for the entire application.

    Args:
        verbose (bool):   If True, show DEBUG messages; otherwise INFO only.
        log_file (str):   If provided, also write logs to this file.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    # Create handlers
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        handlers.append(file_handler)

    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=handlers,
        force=True,  # Override any previous logging config
    )


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# PROGRESS BAR WRAPPER
# Uses tqdm for beautiful progress bars in the terminal.
# If tqdm is not installed, falls back to simple print statements.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


def create_progress_bar(iterable, description="Processing", total=None):
    """
    Creates a tqdm progress bar, or a simple fallback if tqdm is not installed.

    Args:
        iterable:        The items to iterate over.
        description:     Text label shown next to the progress bar.
        total (int):     Total number of items (optional, for generators).

    Returns:
        An iterable with progress tracking.
    """
    if TQDM_AVAILABLE:
        return tqdm(
            iterable,
            desc=f"  {description}",
            total=total,
            bar_format="{l_bar}{bar:30}{r_bar}",
            colour="green",
            ncols=80,
        )
    else:
        # Fallback: simple counter (no fancy bar)
        items = list(iterable)
        total_items = total or len(items)
        for i, item in enumerate(items, 1):
            print(f"  {description}: {i}/{total_items}", end="\r", flush=True)
            yield item
        print()  # New line after progress
        return


# ─────────────────────────────────────────────────────────────────────────────
# MODIFIED SCRAPER WITH PROGRESS BAR
# Wraps the scraper's pagination with a visual progress indicator.
# ─────────────────────────────────────────────────────────────────────────────
def scrape_with_progress(base_url, num_pages):
    """
    Runs the scraper with a progress bar for each page.

    Instead of calling scrape_quotes() directly (which handles its own loop),
    we replicate the pagination logic here to add progress bar support.

    Args:
        base_url (str):   The base URL to scrape.
        num_pages (int):  Number of pages to scrape.

    Returns:
        list[dict]: All scraped items.
    """
    # Import the scraper module functions
    from scraper import fetch_page, parse_quote

    all_items = []
    pages = range(1, num_pages + 1)

    print()  # Blank line before progress bar
    logger.info(f"Starting scraper: {base_url} ({num_pages} pages)")
    print()

    # ── Scrape each page with progress tracking ─────────────────────────
    progress = create_progress_bar(pages, description="Scraping pages", total=num_pages)

    for page_num in progress:
        # Build the page URL
        if page_num == 1:
            url = base_url.rstrip("/") + "/"
        else:
            url = f"{base_url.rstrip('/')}/page/{page_num}/"

        # Fetch the page (with retry mechanism from scraper.py)
        soup = fetch_page(url)

        if soup is None:
            logger.warning(f"  Skipping page {page_num} — could not fetch")
            continue

        # Find all quote blocks on the page
        quote_elements = soup.find_all("div", class_="quote")

        if not quote_elements:
            logger.info(f"  No quotes on page {page_num} — end of content")
            break

        # Parse each quote
        page_items = []
        for element in quote_elements:
            parsed = parse_quote(element, base_url.rstrip("/"))
            if parsed:
                page_items.append(parsed)

        all_items.extend(page_items)

        # Update progress bar description with count
        if TQDM_AVAILABLE and hasattr(progress, "set_postfix"):
            progress.set_postfix(
                quotes=len(all_items),
                page_items=len(page_items),
            )

        # Polite delay between pages (don't hammer the server)
        if page_num < num_pages:
            import random
            delay = random.uniform(1.0, 3.0)
            time.sleep(delay)

    print()  # Blank line after progress bar
    logger.info(f"Scraping complete! Total items: {len(all_items)}")

    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE STEPS WITH PROGRESS
# Each major step gets its own progress indicator.
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(args):
    """
    Main pipeline: scrape → process → export.

    This function orchestrates the entire workflow based on
    command-line arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.

    Returns:
        dict: Summary of the pipeline run.
    """
    start_time = time.time()
    results = {
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "url": args.url,
        "pages": args.pages,
    }

    # ════════════════════════════════════════════════════════════════════
    # STEP 1: SCRAPING
    # ════════════════════════════════════════════════════════════════════
    print_step_header(1, "WEB SCRAPING", f"Target: {args.url}")

    raw_data = scrape_with_progress(args.url, args.pages)

    if not raw_data:
        logger.error("No data was scraped. Exiting.")
        print_error("Scraping returned no data. Check the URL and try again.")
        sys.exit(1)

    results["raw_items"] = len(raw_data)
    print_step_result(f"Scraped {len(raw_data)} items from {args.pages} pages")

    # ════════════════════════════════════════════════════════════════════
    # STEP 2: DATA PROCESSING
    # ════════════════════════════════════════════════════════════════════
    print_step_header(2, "DATA PROCESSING", "Cleaning, validating, deduplicating")

    from data_processor import process_data, get_data_summary

    # Show processing sub-steps with progress
    processing_steps = [
        "Cleaning text fields",
        "Validating prices",
        "Removing duplicates",
        "Creating DataFrame",
        "Sorting & finalizing",
    ]

    if TQDM_AVAILABLE:
        proc_progress = tqdm(
            processing_steps,
            desc="  Processing data",
            bar_format="{l_bar}{bar:30}{r_bar}",
            colour="blue",
            ncols=80,
        )
        for step_name in proc_progress:
            proc_progress.set_postfix(step=step_name)
            time.sleep(0.3)  # Brief pause for visual feedback
        print()

    df = process_data(raw_data)

    if df.empty:
        logger.error("Data processing returned empty DataFrame. Exiting.")
        print_error("No data after processing. Check the scraping results.")
        sys.exit(1)

    # Show summary
    summary = get_data_summary(df)
    results["processed_rows"] = len(df)
    results["unique_authors"] = summary.get("unique_authors", 0)
    results["price_range"] = f"${summary.get('price_min', 0):.2f} – ${summary.get('price_max', 0):.2f}"

    print_step_result(f"Processed {len(df)} unique items")
    print(f"      Price range:     {results['price_range']}")
    print(f"      Unique authors:  {results['unique_authors']}")

    # ════════════════════════════════════════════════════════════════════
    # STEP 3: SAVE TO CSV (if --output is specified)
    # ════════════════════════════════════════════════════════════════════
    if args.output:
        print_step_header(3, "SAVE TO FILE", f"Output: {args.output}")

        try:
            # Determine format from file extension
            if args.output.endswith(".csv"):
                df.to_csv(args.output, index=False, encoding="utf-8-sig")
            elif args.output.endswith(".json"):
                df.to_json(args.output, orient="records", indent=2, force_ascii=False)
            elif args.output.endswith(".xlsx"):
                df.to_excel(args.output, index=False, engine="openpyxl")
            else:
                # Default to CSV
                df.to_csv(args.output, index=False, encoding="utf-8-sig")

            file_size = os.path.getsize(args.output)
            results["output_file"] = args.output
            results["file_size"] = f"{file_size:,} bytes"
            print_step_result(f"Saved to {args.output} ({file_size:,} bytes)")

        except Exception as e:
            logger.error(f"Failed to save file: {e}")
            print_error(f"Could not save to {args.output}: {e}")

    # ════════════════════════════════════════════════════════════════════
    # STEP 4: EXPORT TO GOOGLE SHEETS (if --export is specified)
    # ════════════════════════════════════════════════════════════════════
    if args.export:
        step_num = 4 if args.output else 3
        print_step_header(step_num, "GOOGLE SHEETS EXPORT", "Uploading to Google Sheets")

        # Check for credentials file
        creds_file = args.credentials
        if not os.path.exists(creds_file):
            print_error(
                f"Credentials file not found: '{creds_file}'\n"
                f"      To set up Google Sheets:\n"
                f"      1. Go to https://console.cloud.google.com\n"
                f"      2. Enable Google Sheets API + Google Drive API\n"
                f"      3. Create a Service Account → download JSON key\n"
                f"      4. Save it as 'credentials.json' (or use --credentials path)\n"
                f"      5. See README.md for detailed instructions"
            )
            results["sheets_export"] = "SKIPPED (no credentials)"
        else:
            try:
                from sheets_export import export_to_sheets

                # Show export progress
                export_steps = [
                    "Authenticating with Google",
                    "Opening/creating spreadsheet",
                    "Uploading data rows",
                    "Applying formatting",
                    "Adding timestamp",
                ]

                if TQDM_AVAILABLE:
                    exp_progress = tqdm(
                        export_steps,
                        desc="  Exporting to Sheets",
                        bar_format="{l_bar}{bar:30}{r_bar}",
                        colour="yellow",
                        ncols=80,
                    )
                    for step_name in exp_progress:
                        exp_progress.set_postfix(step=step_name)
                        time.sleep(0.2)  # Brief visual feedback
                    print()

                # Perform the actual export
                export_result = export_to_sheets(
                    df=df,
                    credentials_file=creds_file,
                    spreadsheet_id=args.sheet_id,
                    spreadsheet_title=args.sheet_title,
                )

                results["sheets_url"] = export_result.get("url", "N/A")
                results["sheets_title"] = export_result.get("spreadsheet_title", "N/A")
                results["sheets_rows"] = export_result.get("rows_uploaded", 0)

                print_step_result(f"Exported to Google Sheets!")
                print(f"      URL:   {results['sheets_url']}")
                print(f"      Title: {results['sheets_title']}")
                print(f"      Rows:  {results['sheets_rows']}")

            except ImportError:
                print_error(
                    "Google Sheets libraries not installed.\n"
                    "      Run: pip install gspread google-auth"
                )
                results["sheets_export"] = "FAILED (missing libraries)"

            except Exception as e:
                logger.error(f"Google Sheets export failed: {e}")
                print_error(f"Export failed: {e}")
                results["sheets_export"] = f"FAILED ({e})"

    # ════════════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ════════════════════════════════════════════════════════════════════
    elapsed = time.time() - start_time
    results["elapsed_time"] = f"{elapsed:.1f} seconds"

    print_final_summary(results, elapsed)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# PRETTY CONSOLE OUTPUT HELPERS
# Functions to display nice formatted output in the terminal.
# ─────────────────────────────────────────────────────────────────────────────
def print_banner():
    """Prints the application banner at startup."""
    banner = """
 ╔══════════════════════════════════════════════════════════════╗
 ║                                                              ║
 ║          🌐  WEB SCRAPER → GOOGLE SHEETS EXPORTER  📊       ║
 ║                                                              ║
 ║          Scrape · Process · Export · Automate                 ║
 ║                                                              ║
 ╚══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_step_header(step_num, title, subtitle=""):
    """Prints a step header."""
    print()
    print(f"  ┌─────────────────────────────────────────────────────────┐")
    print(f"  │  STEP {step_num}: {title:<49s}│")
    if subtitle:
        print(f"  │  {subtitle:<55s}│")
    print(f"  └─────────────────────────────────────────────────────────┘")
    print()


def print_step_result(message):
    """Prints a step completion message."""
    print(f"\n  ✅ {message}")


def print_error(message):
    """Prints an error message."""
    print(f"\n  ❌ ERROR: {message}")


def print_final_summary(results, elapsed):
    """Prints the final pipeline summary."""
    print()
    print(f"  ╔══════════════════════════════════════════════════════════╗")
    print(f"  ║                   PIPELINE COMPLETE ✅                   ║")
    print(f"  ╠══════════════════════════════════════════════════════════╣")

    summary_items = [
        ("Started at", results.get("start_time", "N/A")),
        ("URL", results.get("url", "N/A")),
        ("Pages scraped", str(results.get("pages", 0))),
        ("Raw items", str(results.get("raw_items", 0))),
        ("Processed rows", str(results.get("processed_rows", 0))),
        ("Unique authors", str(results.get("unique_authors", 0))),
        ("Price range", results.get("price_range", "N/A")),
    ]

    if "output_file" in results:
        summary_items.append(("Saved to file", results["output_file"]))
        summary_items.append(("File size", results.get("file_size", "N/A")))

    if "sheets_url" in results:
        summary_items.append(("Google Sheet", results["sheets_url"]))

    summary_items.append(("Total time", f"{elapsed:.1f} seconds"))

    for label, value in summary_items:
        # Truncate long URLs to fit the box
        display_value = value if len(value) <= 38 else value[:35] + "..."
        print(f"  ║  {label:<18s}: {display_value:<37s}║")

    print(f"  ╚══════════════════════════════════════════════════════════╝")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# CRON SCHEDULING HELP
# Prints detailed instructions for setting up automated scheduling.
# ─────────────────────────────────────────────────────────────────────────────
def print_cron_help():
    """Prints detailed cron scheduling instructions."""
    script_path = os.path.abspath(__file__)
    project_dir = os.path.dirname(script_path)

    help_text = f"""
 ╔══════════════════════════════════════════════════════════════╗
 ║              ⏰  CRON SCHEDULING INSTRUCTIONS                ║
 ╚══════════════════════════════════════════════════════════════╝

  Cron allows you to run this scraper automatically on a schedule.
  Below are step-by-step instructions for Linux/macOS.

  ──────────────────────────────────────────────────────────────

  STEP 1: Open your crontab editor
  ─────────────────────────────────
    $ crontab -e

  STEP 2: Add a cron job line
  ─────────────────────────────────
  Format:  MIN HOUR DAY MONTH WEEKDAY command

  Examples:

    # ── Every day at 9:00 AM ────────────────────────────────
    0 9 * * * cd {project_dir} && /usr/bin/python3 main.py --pages 5 --export --output data.csv >> cron.log 2>&1

    # ── Every 6 hours ───────────────────────────────────────
    0 */6 * * * cd {project_dir} && /usr/bin/python3 main.py --export >> cron.log 2>&1

    # ── Every Monday at 8:00 AM ─────────────────────────────
    0 8 * * 1 cd {project_dir} && /usr/bin/python3 main.py --pages 10 --export >> cron.log 2>&1

    # ── Every 30 minutes (frequent monitoring) ──────────────
    */30 * * * * cd {project_dir} && /usr/bin/python3 main.py --pages 2 --output latest.csv >> cron.log 2>&1

  STEP 3: Save and exit
  ─────────────────────────────────
    In nano: Ctrl+O → Enter → Ctrl+X
    In vim:  :wq

  STEP 4: Verify your cron job
  ─────────────────────────────────
    $ crontab -l

  ──────────────────────────────────────────────────────────────

  IMPORTANT NOTES:

    1. Use FULL PATHS in cron (e.g., /usr/bin/python3, not python3)
       Find your Python path with: $ which python3

    2. The 'cd {project_dir}' is needed so the script can
       find credentials.json and other files.

    3. The '>> cron.log 2>&1' part redirects output to a log file
       so you can check for errors later.

    4. Make sure credentials.json is in the project directory.

    5. Test your command manually first:
       $ cd {project_dir} && python3 main.py --pages 2

  ──────────────────────────────────────────────────────────────

  WINDOWS TASK SCHEDULER:

    1. Open Task Scheduler (taskschd.msc)
    2. Create Basic Task → name it "Web Scraper"
    3. Set trigger (daily, weekly, etc.)
    4. Action: Start a program
       Program: python
       Arguments: main.py --pages 5 --export
       Start in: {project_dir}
    5. Finish and test with "Run" button.

  ──────────────────────────────────────────────────────────────
"""
    print(help_text)


# ─────────────────────────────────────────────────────────────────────────────
# COMMAND-LINE ARGUMENT PARSER
# Defines all CLI arguments with help text and defaults.
# ─────────────────────────────────────────────────────────────────────────────
def parse_arguments():
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "🌐 Web Scraper with Google Sheets Integration\n"
            "   Scrapes data from websites and exports to Google Sheets."
        ),
        epilog=(
            "Examples:\n"
            "  python main.py                                 # Default scrape (5 pages)\n"
            "  python main.py --pages 3                       # Scrape 3 pages\n"
            "  python main.py --output data.csv               # Save to CSV\n"
            "  python main.py --export                        # Export to Google Sheets\n"
            "  python main.py --export --sheet-id 1abc...     # Export to existing sheet\n"
            "  python main.py --pages 10 --export --output data.csv  # Full pipeline\n"
            "  python main.py --cron-help                     # Scheduling instructions\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── Scraping arguments ──────────────────────────────────────────────
    scraping_group = parser.add_argument_group("Scraping Options")

    scraping_group.add_argument(
        "--url",
        type=str,
        default="https://quotes.toscrape.com",
        help="Base URL to scrape (default: quotes.toscrape.com)",
    )

    scraping_group.add_argument(
        "--pages",
        type=int,
        default=5,
        help="Number of pages to scrape (default: 5)",
    )

    # ── Output arguments ────────────────────────────────────────────────
    output_group = parser.add_argument_group("Output Options")

    output_group.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Save results to file (supports .csv, .json, .xlsx)",
    )

    # ── Google Sheets arguments ─────────────────────────────────────────
    sheets_group = parser.add_argument_group("Google Sheets Options")

    sheets_group.add_argument(
        "--export",
        action="store_true",
        help="Export data to Google Sheets",
    )

    sheets_group.add_argument(
        "--sheet-id",
        type=str,
        default=None,
        help="ID of an existing Google Sheet to update",
    )

    sheets_group.add_argument(
        "--sheet-title",
        type=str,
        default=None,
        help="Title for a new Google Sheet (default: auto-generated)",
    )

    sheets_group.add_argument(
        "--credentials",
        type=str,
        default="credentials.json",
        help="Path to Google API credentials file (default: credentials.json)",
    )

    # ── General arguments ───────────────────────────────────────────────
    general_group = parser.add_argument_group("General Options")

    general_group.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    general_group.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Save logs to file (e.g., scraper.log)",
    )

    general_group.add_argument(
        "--cron-help",
        action="store_true",
        help="Show cron scheduling instructions and exit",
    )

    general_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without actually scraping",
    )

    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# DRY RUN MODE
# Shows what the script would do without actually running.
# Useful for testing cron jobs and verifying arguments.
# ─────────────────────────────────────────────────────────────────────────────
def dry_run(args):
    """Shows what the pipeline would do without executing it."""
    print()
    print("  ── DRY RUN MODE (nothing will be executed) ─────────────")
    print()
    print(f"  URL to scrape:     {args.url}")
    print(f"  Pages to scrape:   {args.pages}")
    print(f"  Save to file:      {args.output or '(none)'}")
    print(f"  Export to Sheets:  {'Yes' if args.export else 'No'}")
    if args.export:
        print(f"  Sheet ID:          {args.sheet_id or '(will create new)'}")
        print(f"  Sheet title:       {args.sheet_title or '(auto-generated)'}")
        print(f"  Credentials file:  {args.credentials}")
        creds_exists = os.path.exists(args.credentials)
        print(f"  Credentials found: {'✅ Yes' if creds_exists else '❌ No'}")
    print(f"  Verbose logging:   {'Yes' if args.verbose else 'No'}")
    print(f"  Log file:          {args.log_file or '(none)'}")
    print(f"  tqdm installed:    {'✅ Yes' if TQDM_AVAILABLE else '❌ No (install: pip install tqdm)'}")
    print()
    print("  To run for real, remove the --dry-run flag.")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# The main function that runs when you execute: python main.py
# ─────────────────────────────────────────────────────────────────────────────
def main():
    """
    Application entry point.

    Workflow:
        1. Parse command-line arguments
        2. Set up logging
        3. Handle special modes (--cron-help, --dry-run)
        4. Run the main pipeline (scrape → process → export)
        5. Display final results
    """
    # ── Parse command-line arguments ────────────────────────────────────
    args = parse_arguments()

    # ── Handle special modes ────────────────────────────────────────────
    if args.cron_help:
        print_cron_help()
        sys.exit(0)

    # ── Set up logging ──────────────────────────────────────────────────
    setup_logging(verbose=args.verbose, log_file=args.log_file)

    # ── Print banner ────────────────────────────────────────────────────
    print_banner()

    # ── Show configuration ──────────────────────────────────────────────
    print(f"  Configuration:")
    print(f"    URL:          {args.url}")
    print(f"    Pages:        {args.pages}")
    print(f"    Output file:  {args.output or '(none)'}")
    print(f"    Google Sheets: {'Yes' if args.export else 'No'}")
    if args.export and args.sheet_id:
        print(f"    Sheet ID:     {args.sheet_id}")
    print(f"    Verbose:      {'Yes' if args.verbose else 'No'}")
    if not TQDM_AVAILABLE:
        print(f"    ⚠ tqdm not found — install it for progress bars: pip install tqdm")
    print()

    # ── Dry run mode ────────────────────────────────────────────────────
    if args.dry_run:
        dry_run(args)
        sys.exit(0)

    # ── Validate arguments ──────────────────────────────────────────────
    if args.pages < 1:
        print_error("Number of pages must be at least 1")
        sys.exit(1)

    if args.pages > 100:
        print_error("Maximum 100 pages (be respectful to the server!)")
        sys.exit(1)

    # ── Run the pipeline ────────────────────────────────────────────────
    try:
        results = run_pipeline(args)

    except KeyboardInterrupt:
        print("\n\n  ⚠ Interrupted by user (Ctrl+C). Exiting...\n")
        sys.exit(130)

    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        print_error(f"Unexpected error: {e}")
        print(f"      Run with --verbose for more details.")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
