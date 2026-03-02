import os
import logging
import requests
import re
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

############# Added for dynamic pathing for downloaded data  to remain the data being downloaded and the year
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
BASE_OUTPUT_ROOT = "/mnt/code/data_updates"
################################################################

# -------------------- Configuration --------------------
CHROMEDRIVER_PATH = '/usr/local/bin/chromedriver'
BASE_URL = 'https://www.cms.gov/medicare/payment/medicare-advantage-rates-statistics/risk-adjustment'
# OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".")
# os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_DIR = "/mnt/code/data_updates/downloads"


# -------------------- Logging Setup --------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------- Selenium Setup --------------------
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--incognito')

driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)

try:
    logger.info("Navigating to CMS Risk Adjustment base page...")
    driver.get(BASE_URL)
    WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
    links = driver.find_elements(By.TAG_NAME, "a")

    # Step 1: Find subpages with year in URL
    year_pattern = re.compile(r'/20\d{2}-model-software')
    year_links = {}
    for link in links:
        href = link.get_attribute('href')
        if href:
            match = year_pattern.search(href)
            if match:
                year_str = match.group() # e.g., '/2026-model-software'
                year = re.search(r'20\d{2}', year_str).group() # Extract just the year
                year_links[year] = href

    if not year_links:
        raise Exception("No year-specific model software pages found.")

    latest_year = sorted(year_links.keys())[-1]
    latest_model_url = year_links[latest_year]
    icd_url = latest_model_url + "/icd-10-mappings"

################# Added New File Path ############
    OUTPUT_DIR = os.path.join(
    BASE_OUTPUT_ROOT,
    SCRIPT_NAME,
    latest_year
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logger.info(f"Output directory set to: {OUTPUT_DIR}")
##################################################################

    logger.info(f"Latest year detected: {latest_year}")
    logger.info(f"Model Software URL: {latest_model_url}")
    logger.info(f"ICD-10 Mappings URL: {icd_url}")

    def collect_download_links(page_url):
        driver.get(page_url)
        WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
        page_links = driver.find_elements(By.TAG_NAME, "a")
        downloads = []
        for link in page_links:
            href = link.get_attribute('href')
            text = link.text.strip().lower()
            if href and any(href.endswith(ext) for ext in ['.zip', '.xls', '.xlsx']):
                downloads.append((text, href))
        return downloads

    # Step 2: Collect downloads from both pages
    model_downloads = collect_download_links(latest_model_url)
    icd_downloads = collect_download_links(icd_url)

    all_downloads = model_downloads + icd_downloads
    logger.info(f"Found {len(all_downloads)} total downloadable files.")

    # Step 3: Download and extract
    for label, file_url in all_downloads:
        logger.info(f"Downloading {label} from {file_url}")
        response = requests.get(file_url)
        if response.status_code == 200:
            file_name = file_url.split("/")[-1]
            output_path = os.path.join(OUTPUT_DIR, file_name)
            with open(output_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"{label} saved to {output_path}")

            # Extract ZIP files
            if file_name.endswith(".zip"):
                import zipfile
                try:
                    with zipfile.ZipFile(output_path, 'r') as zip_ref:
                        extract_dir = os.path.join(OUTPUT_DIR, file_name.replace(".zip", ""))
                        os.makedirs(extract_dir, exist_ok=True)
                        zip_ref.extractall(extract_dir)
                    logger.info(f"Extracted {file_name} to {extract_dir}")
                except zipfile.BadZipFile:
                    logger.warning(f"Failed to unzip {file_name}: Bad zip file.")
        else:
            logger.warning(f"Failed to download {label} from {file_url}")

finally:
    driver.quit()
    logger.info("Process completed.")
