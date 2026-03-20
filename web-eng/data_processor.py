#!/usr/bin/env python3
"""
=============================================================================
 Web Scraper with Google Sheets Integration
 Stage 2: Data Processing Module
=============================================================================
 Handles: text cleaning, price validation, deduplication, Pandas DataFrame
=============================================================================
"""

import re
import logging
import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# TEXT CLEANING
# Removes extra whitespace, special characters, and unicode artifacts
# that are common when scraping web pages.
# ─────────────────────────────────────────────────────────────────────────────
def clean_text(text):
    """
    Cleans a text string from scraping artifacts.

    Steps:
        1. Strip leading/trailing whitespace
        2. Remove special Unicode quotes (\u201c \u201d) and other symbols
        3. Collapse multiple spaces into one
        4. Remove control characters (tabs, newlines inside text)

    Args:
        text (str): Raw text from the scraper.

    Returns:
        str: Cleaned text.

    Example:
        >>> clean_text("  \\u201cHello   World\\u201d\\n  ")
        'Hello World'
    """
    if not isinstance(text, str):
        return ""

    # Step 1: Strip leading/trailing whitespace
    text = text.strip()

    # Step 2: Remove Unicode quotes and special symbols
    # These characters often appear in scraped text:
    #   \u201c = "  (left double quotation mark)
    #   \u201d = "  (right double quotation mark)
    #   \u2018 = '  (left single quotation mark)
    #   \u2019 = '  (right single quotation mark)
    #   \u2014 = —  (em dash)
    text = text.replace("\u201c", "").replace("\u201d", "")
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    text = text.replace("\u2014", " - ")

    # Step 3: Replace control characters (newlines, tabs) with spaces
    text = re.sub(r"[\n\r\t]+", " ", text)

    # Step 4: Collapse multiple spaces into a single space
    text = re.sub(r"\s{2,}", " ", text)

    # Step 5: Final strip
    text = text.strip()

    return text


# ─────────────────────────────────────────────────────────────────────────────
# PRICE VALIDATION
# Converts price strings/numbers to proper float values.
# Handles cases like: "$12.99", "12,99 EUR", "free", None, etc.
# ─────────────────────────────────────────────────────────────────────────────
def validate_price(price):
    """
    Validates and converts a price to a proper float number.

    Handles various formats:
        - Float/int: 12.99, 10 → returned as-is (rounded to 2 decimals)
        - String with currency: "$12.99", "12.99 USD" → extracts the number
        - Comma decimal: "12,99" → converts to 12.99
        - Invalid/missing: None, "", "free" → returns 0.0

    Args:
        price: The price value (can be str, int, float, or None).

    Returns:
        float: The validated price, rounded to 2 decimal places.

    Examples:
        >>> validate_price(12.99)
        12.99
        >>> validate_price("$12.99")
        12.99
        >>> validate_price("free")
        0.0
        >>> validate_price(None)
        0.0
    """
    # Case 1: Already a number
    if isinstance(price, (int, float)):
        return round(float(price), 2)

    # Case 2: None or empty
    if price is None or str(price).strip() == "":
        return 0.0

    # Case 3: String — try to extract the numeric part
    price_str = str(price).strip()

    # Replace comma with dot (European format: 12,99 → 12.99)
    price_str = price_str.replace(",", ".")

    # Extract the first number found in the string
    # This regex finds patterns like: 12.99, 12, .99, 1234.56
    match = re.search(r"(\d+\.?\d*)", price_str)

    if match:
        return round(float(match.group(1)), 2)

    # Case 4: Could not extract a number
    logger.warning(f"Could not parse price: '{price}' — defaulting to 0.0")
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPLICATION
# Removes duplicate entries based on a specific key (default: link).
# Keeps the first occurrence and discards later duplicates.
# ─────────────────────────────────────────────────────────────────────────────
def deduplicate(data, key="link"):
    """
    Removes duplicate items from a list of dictionaries.

    Uses a specific key (e.g., 'link') to determine uniqueness.
    The first occurrence is kept; subsequent duplicates are removed.

    Args:
        data (list[dict]): List of scraped items.
        key (str):         The dictionary key to check for duplicates.

    Returns:
        list[dict]: Deduplicated list.

    Example:
        >>> items = [
        ...     {"link": "/a", "title": "First"},
        ...     {"link": "/b", "title": "Second"},
        ...     {"link": "/a", "title": "Duplicate of First"},
        ... ]
        >>> deduplicate(items, key="link")
        [{"link": "/a", "title": "First"}, {"link": "/b", "title": "Second"}]
    """
    if not data:
        return []

    seen = set()         # Tracks keys we've already encountered
    unique_items = []    # Stores only unique items

    duplicates_count = 0

    for item in data:
        # Get the value of the deduplication key
        item_key = item.get(key, "")

        if item_key not in seen:
            seen.add(item_key)
            unique_items.append(item)
        else:
            duplicates_count += 1

    if duplicates_count > 0:
        logger.info(f"Deduplication: removed {duplicates_count} duplicate(s) by '{key}'")
    else:
        logger.info(f"Deduplication: no duplicates found (checked by '{key}')")

    return unique_items


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PROCESSING PIPELINE
# Combines all processing steps into a single function.
# Takes raw scraped data → returns a clean Pandas DataFrame.
# ─────────────────────────────────────────────────────────────────────────────
def process_data(raw_data):
    """
    Full data processing pipeline.

    Steps:
        1. Validate input (check if data exists)
        2. Clean text fields (title, description)
        3. Validate prices (convert to proper floats)
        4. Remove duplicates (by link)
        5. Create a Pandas DataFrame
        6. Sort by title
        7. Reset index (clean row numbers)
        8. Add processing metadata

    Args:
        raw_data (list[dict]): Raw scraped data from the scraper.

    Returns:
        pandas.DataFrame: Cleaned and structured data.
        Returns an empty DataFrame if input is invalid.
    """
    logger.info("=" * 60)
    logger.info("Starting data processing pipeline...")
    logger.info("=" * 60)

    # ── Step 1: Validate input ──────────────────────────────────────────
    if not raw_data:
        logger.warning("No data to process — received empty list")
        return pd.DataFrame(columns=[
            "title", "description", "price", "date", "link", "tags"
        ])

    logger.info(f"Input: {len(raw_data)} raw items")

    # ── Step 2: Clean text fields ───────────────────────────────────────
    logger.info("Step 2/6: Cleaning text fields...")
    for item in raw_data:
        item["title"] = clean_text(item.get("title", ""))
        item["description"] = clean_text(item.get("description", ""))
        item["tags"] = clean_text(item.get("tags", ""))
        item["link"] = item.get("link", "").strip()  # links just need strip
        item["date"] = item.get("date", "").strip()

    logger.info("  ✓ Text cleaning complete")

    # ── Step 3: Validate prices ─────────────────────────────────────────
    logger.info("Step 3/6: Validating prices...")
    for item in raw_data:
        item["price"] = validate_price(item.get("price"))

    logger.info("  ✓ Price validation complete")

    # ── Step 4: Deduplicate ─────────────────────────────────────────────
    logger.info("Step 4/6: Removing duplicates...")
    clean_data = deduplicate(raw_data, key="link")
    logger.info(f"  ✓ After dedup: {len(clean_data)} items")

    # ── Step 5: Create Pandas DataFrame ─────────────────────────────────
    logger.info("Step 5/6: Creating Pandas DataFrame...")

    # Define the column order we want
    columns = ["title", "description", "price", "date", "link", "tags"]

    df = pd.DataFrame(clean_data, columns=columns)

    # Ensure proper data types
    df["price"] = df["price"].astype(float)
    df["title"] = df["title"].astype(str)
    df["description"] = df["description"].astype(str)

    logger.info(f"  ✓ DataFrame created: {df.shape[0]} rows × {df.shape[1]} columns")

    # ── Step 6: Sort and finalize ───────────────────────────────────────
    logger.info("Step 6/6: Sorting and finalizing...")

    # Sort by title (author name) alphabetically
    df = df.sort_values(by="title", ascending=True)

    # Reset index to have clean row numbers (0, 1, 2, ...)
    df = df.reset_index(drop=True)

    logger.info("  ✓ Sorted by title (A → Z)")

    # ── Summary ─────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Data processing complete!")
    logger.info(f"  Total rows:      {len(df)}")
    logger.info(f"  Columns:         {', '.join(df.columns.tolist())}")
    logger.info(f"  Price range:     ${df['price'].min():.2f} — ${df['price'].max():.2f}")
    logger.info(f"  Average price:   ${df['price'].mean():.2f}")
    logger.info(f"  Unique authors:  {df['title'].nunique()}")
    logger.info("=" * 60)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# DATA SUMMARY STATISTICS
# Generates a quick overview of the processed data.
# Useful for logging and verification.
# ─────────────────────────────────────────────────────────────────────────────
def get_data_summary(df):
    """
    Generates summary statistics for the processed DataFrame.

    Args:
        df (pandas.DataFrame): The processed data.

    Returns:
        dict: Summary statistics.
    """
    if df.empty:
        return {"status": "empty", "total_rows": 0}

    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "unique_authors": df["title"].nunique(),
        "price_min": round(df["price"].min(), 2),
        "price_max": round(df["price"].max(), 2),
        "price_avg": round(df["price"].mean(), 2),
        "price_total": round(df["price"].sum(), 2),
        "avg_description_length": round(df["description"].str.len().mean(), 0),
        "date_range": f"{df['date'].min()} to {df['date'].max()}",
        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE EXECUTION
# When run directly, this module performs a demo using sample data
# to demonstrate all processing features.
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Set up logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print("\n" + "=" * 60)
    print("  STAGE 2: Data Processing Demo")
    print("=" * 60 + "\n")

    # ── Option A: Use live data from Stage 1 scraper ────────────────────
    try:
        from scraper import scrape_quotes

        print("  Fetching live data from scraper (Stage 1)...\n")
        raw_data = scrape_quotes(num_pages=2)  # Only 2 pages for quick demo

    except ImportError:
        # ── Option B: Use sample data if scraper is not available ────────
        print("  Using sample data (scraper not available)...\n")
        raw_data = [
            {
                "title": "  Albert Einstein  ",
                "description": "\u201cThe world as we have created it...\u201d",
                "price": "$15.99",
                "date": "2025-01-15",
                "link": "https://quotes.toscrape.com/author/Albert-Einstein",
                "tags": "change, deep-thoughts",
            },
            {
                "title": "J.K. Rowling",
                "description": "  It is our choices,\n  Harry...  ",
                "price": "12,50 EUR",
                "date": "2025-01-15",
                "link": "https://quotes.toscrape.com/author/J-K-Rowling",
                "tags": "abilities, choices",
            },
            {
                "title": "Albert Einstein",
                "description": "Another quote by Einstein...",
                "price": None,
                "date": "2025-01-15",
                "link": "https://quotes.toscrape.com/author/Albert-Einstein",  # DUPLICATE
                "tags": "science",
            },
            {
                "title": "Steve Martin",
                "description": "\u201cA day without laughter is wasted.\u201d",
                "price": "free",
                "date": "2025-01-15",
                "link": "https://quotes.toscrape.com/author/Steve-Martin",
                "tags": "humor",
            },
        ]

    # ── Process the data ────────────────────────────────────────────────
    df = process_data(raw_data)

    # ── Display results ─────────────────────────────────────────────────
    if not df.empty:
        print(f"\n{'─' * 60}")
        print(f"  Processed DataFrame Preview")
        print(f"{'─' * 60}\n")

        # Show DataFrame info
        print("  DataFrame Info:")
        print(f"  Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        print(f"  Data types:")
        for col in df.columns:
            print(f"    {col:15s} → {df[col].dtype}")

        # Show first few rows
        print(f"\n  First rows:")
        print(df.to_string(index=True, max_colwidth=50))

        # Show summary
        print(f"\n{'─' * 60}")
        print(f"  Data Summary")
        print(f"{'─' * 60}")
        summary = get_data_summary(df)
        for key, value in summary.items():
            print(f"  {key:25s}: {value}")

        print(f"{'─' * 60}\n")
    else:
        print("  No data to display.\n")
