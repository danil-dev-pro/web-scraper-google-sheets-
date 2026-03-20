#!/usr/bin/env python3
"""
=============================================================================
 Web Scraper with Google Sheets Integration
 Stage 1: Core Web Scraping Module
=============================================================================
 Scrapes quotes from quotes.toscrape.com (demo site)
 Features: pagination, retry mechanism, User-Agent rotation
=============================================================================
"""

import requests
from bs4 import BeautifulSoup
import random
import time
import logging
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# We use logging instead of print() — it's a professional practice.
# Logs show timestamps and severity levels (INFO, WARNING, ERROR).
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# USER-AGENT ROTATION
# Websites can block bots. We rotate User-Agent headers to look like
# different real browsers. This reduces the chance of being blocked.
# ─────────────────────────────────────────────────────────────────────────────
USER_AGENTS = [
    # Chrome on Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    # Firefox on macOS
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Firefox/126.0"
    ),
    # Safari on macOS
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    ),
    # Chrome on Linux
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Edge on Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
    ),
]


def get_random_headers():
    """
    Returns HTTP headers with a random User-Agent.
    Called before each request to rotate the browser identity.
    """
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETRY MECHANISM
# Network requests can fail (timeout, server error, etc.).
# We retry up to `max_retries` times with increasing delay between attempts.
# This pattern is called "exponential backoff".
# ─────────────────────────────────────────────────────────────────────────────
def fetch_page(url, max_retries=3, base_delay=2):
    """
    Fetches a web page with retry logic.

    Args:
        url (str):         The URL to fetch.
        max_retries (int): How many times to retry on failure (default: 3).
        base_delay (int):  Base delay in seconds between retries (default: 2).
                           Actual delay = base_delay * attempt_number.

    Returns:
        BeautifulSoup object if successful, None if all retries failed.
    """
    for attempt in range(1, max_retries + 1):
        try:
            headers = get_random_headers()
            logger.info(
                f"Fetching: {url} (attempt {attempt}/{max_retries})"
            )

            response = requests.get(url, headers=headers, timeout=15)

            # Raise an exception for HTTP errors (4xx, 5xx)
            response.raise_for_status()

            # Parse the HTML content
            soup = BeautifulSoup(response.text, "html.parser")
            logger.info(f"Successfully fetched: {url}")
            return soup

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt} for {url}")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error {e.response.status_code} on attempt {attempt}")
            # Don't retry on 404 — the page doesn't exist
            if e.response.status_code == 404:
                logger.error(f"Page not found: {url}")
                return None

        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error on attempt {attempt} for {url}")

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on attempt {attempt}: {e}")

        # Wait before retrying (exponential backoff)
        if attempt < max_retries:
            delay = base_delay * attempt
            logger.info(f"Waiting {delay} seconds before retry...")
            time.sleep(delay)

    logger.error(f"All {max_retries} attempts failed for {url}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# QUOTE PARSER
# Extracts structured data from a single quote element on the page.
# We map the quote fields to our target schema:
#   - title  → author name
#   - description → quote text
#   - price → (simulated) random price tag for demo purposes
#   - date  → (simulated) current date, since the demo site has no dates
#   - link  → link to the author's page
# ─────────────────────────────────────────────────────────────────────────────
def parse_quote(quote_element, base_url="https://quotes.toscrape.com"):
    """
    Extracts data from a single quote HTML element.

    Args:
        quote_element: BeautifulSoup Tag object representing one quote.
        base_url (str): Base URL for constructing absolute links.

    Returns:
        dict with keys: title, description, price, date, link
        Returns None if parsing fails.
    """
    try:
        # ── Extract the quote text ──────────────────────────────────────
        text_element = quote_element.find("span", class_="text")
        description = text_element.get_text(strip=True) if text_element else ""

        # ── Extract the author (used as "title") ────────────────────────
        author_element = quote_element.find("small", class_="author")
        title = author_element.get_text(strip=True) if author_element else "Unknown"

        # ── Extract tags (used to simulate a "price") ──────────────────
        # The demo site doesn't have prices, so we generate a demo price
        # based on the number of tags (just for demonstration).
        tags = quote_element.find_all("a", class_="tag")
        tag_list = [tag.get_text(strip=True) for tag in tags]
        # Simulated price: $5.00 per tag (for demo purposes)
        price = round(len(tag_list) * 5.00 + random.uniform(0.99, 9.99), 2)

        # ── Extract the author link ─────────────────────────────────────
        author_link = quote_element.find("a")
        if author_link and author_link.get("href"):
            link = base_url + author_link["href"]
        else:
            link = base_url

        # ── Date (simulated — the demo site has no dates) ───────────────
        date = datetime.now().strftime("%Y-%m-%d")

        # ── Build the result dictionary ─────────────────────────────────
        return {
            "title": title,
            "description": description,
            "price": price,
            "date": date,
            "link": link,
            "tags": ", ".join(tag_list),  # bonus field
        }

    except Exception as e:
        logger.error(f"Error parsing quote element: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPER FUNCTION
# Orchestrates the full scraping process:
# 1. Iterates through pages (pagination)
# 2. Fetches each page with retry mechanism
# 3. Parses all quotes on each page
# 4. Returns a list of all parsed items
# ─────────────────────────────────────────────────────────────────────────────
def scrape_quotes(base_url="https://quotes.toscrape.com", num_pages=5):
    """
    Scrapes quotes from multiple pages.

    Args:
        base_url (str):   The base URL of the website.
        num_pages (int):  How many pages to scrape (default: 5).

    Returns:
        list of dicts — each dict represents one scraped quote.
    """
    all_quotes = []

    logger.info("=" * 60)
    logger.info(f"Starting scraper: {base_url}")
    logger.info(f"Pages to scrape: {num_pages}")
    logger.info("=" * 60)

    for page_num in range(1, num_pages + 1):
        # ── Build the page URL ──────────────────────────────────────────
        if page_num == 1:
            url = base_url + "/"
        else:
            url = f"{base_url}/page/{page_num}/"

        # ── Fetch the page (with retries) ───────────────────────────────
        soup = fetch_page(url)

        if soup is None:
            logger.warning(f"Skipping page {page_num} — could not fetch")
            continue

        # ── Find all quote blocks on the page ──────────────────────────
        quote_elements = soup.find_all("div", class_="quote")

        if not quote_elements:
            logger.info(f"No quotes found on page {page_num} — end of content")
            break

        logger.info(f"Page {page_num}: found {len(quote_elements)} quotes")

        # ── Parse each quote ────────────────────────────────────────────
        for element in quote_elements:
            parsed = parse_quote(element, base_url)
            if parsed:
                all_quotes.append(parsed)

        # ── Polite delay between pages ──────────────────────────────────
        # Always add a delay between requests to avoid overloading the server.
        # This is good scraping etiquette!
        if page_num < num_pages:
            delay = random.uniform(1.0, 3.0)
            logger.info(f"Waiting {delay:.1f}s before next page...")
            time.sleep(delay)

    logger.info("=" * 60)
    logger.info(f"Scraping complete! Total quotes collected: {len(all_quotes)}")
    logger.info("=" * 60)

    return all_quotes


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE EXECUTION
# When this file is run directly (not imported), it performs a demo scrape
# and prints the results to the console.
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  STAGE 1: Web Scraping Demo")
    print("  Target: quotes.toscrape.com")
    print("=" * 60 + "\n")

    # Run the scraper
    quotes = scrape_quotes(
        base_url="https://quotes.toscrape.com",
        num_pages=5,
    )

    # Display results
    if quotes:
        print(f"\n{'─' * 60}")
        print(f"  Results Preview (first 5 of {len(quotes)} quotes)")
        print(f"{'─' * 60}\n")

        for i, quote in enumerate(quotes[:5], 1):
            print(f"  #{i}")
            print(f"  Title (Author): {quote['title']}")
            print(f"  Description:    {quote['description'][:80]}...")
            print(f"  Price (demo):   ${quote['price']}")
            print(f"  Date:           {quote['date']}")
            print(f"  Link:           {quote['link']}")
            print(f"  Tags:           {quote['tags']}")
            print()

        print(f"{'─' * 60}")
        print(f"  Total scraped: {len(quotes)} quotes from 5 pages")
        print(f"{'─' * 60}\n")
    else:
        print("  No quotes were scraped. Check the logs above for errors.\n")
