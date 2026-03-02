from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import os
import re
from datetime import datetime

# -------------------- Dynamic output pathing --------------------
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
BASE_OUTPUT_ROOT = "/mnt/code/data_updates"
# ---------------------------------------------------------------

def extract_posted_date(url: str):
    """
    Try to extract a 'posted' date from the filename portion of the URL.
    Supports patterns like:
      - 01-23-2026 / 1-23-2026
      - 12182020
      - 08.03.2020
      - 07092021
      - 1072025 (best-effort, ambiguous -> handled as fallback)
    Returns a datetime.date or None.
    """
    fname = url.split("/")[-1].lower()

    # Strip query fragments if any
    fname = fname.split("?")[0].split("#")[0]

    patterns = [
        (r'(?P<m>\d{1,2})-(?P<d>\d{1,2})-(?P<y>20\d{2})', "%m-%d-%Y"),
        (r'(?P<m>\d{1,2})_(?P<d>\d{1,2})_(?P<y>20\d{2})', "%m_%d_%Y"),
        (r'(?P<m>\d{1,2})\.(?P<d>\d{1,2})\.(?P<y>20\d{2})', "%m.%d.%Y"),
        (r'(?P<m>\d{2})(?P<d>\d{2})(?P<y>20\d{2})', "%m%d%Y"),  # mmddyyyy
        (r'(?P<m>\d{2})(?P<d>\d{2})(?P<y>20\d{2})', "%m%d%Y"),
    ]

    for pat, fmt in patterns:
        m = re.search(pat, fname)
        if m:
            try:
                # Build a normalized string that matches fmt
                if fmt in ("%m-%d-%Y", "%m_%d_%Y", "%m.%d.%Y"):
                    s = f"{m.group('m')}{fmt[2]}{m.group('d')}{fmt[5]}{m.group('y')}"  # reuse separators
                    # The above is a little hacky; simpler: use the original match
                    # We'll just parse the matched substring:
                    return datetime.strptime(m.group(0), fmt).date()
                else:
                    return datetime.strptime(m.group(0), fmt).date()
            except Exception:
                pass

    return None

def extract_cy_year(url: str, text: str):
    """
    Extract CY year (e.g., cy2025) or any 20xx year as fallback.
    Returns string year or None.
    """
    combined = f"{text} {url}".lower()
    m = re.search(r'cy(20\d{2})', combined)
    if m:
        return m.group(1)
    m = re.search(r'(20\d{2})', combined)
    if m:
        return m.group(1)
    return None

# Selenium setup
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--incognito')

driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'), options=options)

url = 'https://www.cms.gov/marketplace/resources/regulations-guidance'
driver.get(url)

try:
    WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), 'Technical Details (XLSX)')]"))
    )

    xlsx_links = driver.find_elements(By.XPATH, "//a[contains(text(), 'Technical Details (XLSX)')]")
    xlsx_urls_and_text = [
        (link.get_attribute('href').strip(), link.text.strip())
        for link in xlsx_links
        if link.get_attribute('href')
    ]

    if not xlsx_urls_and_text:
        print("No XLSX links found on the page.")
        raise SystemExit(0)

    # Monitoring output: print all links seen
    print(f"Found {len(xlsx_urls_and_text)} XLSX links.")
    for href, text in xlsx_urls_and_text:
        print(f"Text: {text}, URL: {href}")

    # Choose "latest" deterministically:
    # 1) Prefer the most recent posted date parsed from filename
    # 2) If no dates, fall back to highest CY year / year present
    enriched = []
    for href, text in xlsx_urls_and_text:
        posted_date = extract_posted_date(href)
        cy_year = extract_cy_year(href, text)
        enriched.append((href, text, posted_date, cy_year))

    dated = [row for row in enriched if row[2] is not None]
    if dated:
        dated.sort(key=lambda r: r[2])
        sorted_latest_url, latest_text, latest_posted_date, latest_cy_year = dated[-1]
    else:
        # fallback: choose highest year string if available, else last URL lexicographically
        def year_key(r):
            try:
                return int(r[3]) if r[3] else -1
            except Exception:
                return -1

        enriched.sort(key=lambda r: (year_key(r), r[0]))
        sorted_latest_url, latest_text, latest_posted_date, latest_cy_year = enriched[-1]

    print(f"\nLatest file URL: {sorted_latest_url}")
    print(f"Latest link text: {latest_text}")

    # Folder year should be the CY year (preferred), else any year we can find
    latest_year = latest_cy_year or extract_cy_year(sorted_latest_url, latest_text)
    if not latest_year:
        raise ValueError("Could not determine year for output folder from the latest link.")

    print(f"Detected year: {latest_year}")

    output_dir = os.path.join(BASE_OUTPUT_ROOT, SCRIPT_NAME, latest_year)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")

    response = requests.get(sorted_latest_url)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download the latest file. HTTP {response.status_code}")

    file_name = sorted_latest_url.split("/")[-1].split("?")[0].split("#")[0]
    output_path = os.path.join(output_dir, file_name)

    with open(output_path, 'wb') as f:
        f.write(response.content)

    print(f"Downloaded file to: {output_path}")

except Exception as e:
    print(f"Error: {e}")

finally:
    driver.quit()
