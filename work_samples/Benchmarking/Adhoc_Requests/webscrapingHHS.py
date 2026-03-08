#%%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

# Set up the Selenium WebDriver (Chrome in this case)
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run headless if you don't want the browser window to appear
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# URL of the CMS page to scrape
url = 'https://www.cms.gov/marketplace/resources/regulations-guidance'

# Open the page with Selenium
driver.get(url)

# Wait for the page to load fully (adjust time if needed)
try:
    # Wait for the "Technical Details (XLSX)" text to be present on the page
    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(text(), 'Technical Details (XLSX)')]")))

    # Find all links by their visible text 'Technical Details (XLSX)'
    xlsx_links = driver.find_elements(By.XPATH, "//a[contains(text(), 'Technical Details (XLSX)')]")

    # Extract the URLs and visible text from the links
    xlsx_urls_and_text = [(link.get_attribute('href').strip(), link.text.strip()) for link in xlsx_links]  # Extract URL and text

    # Print the list of all URLs
    if xlsx_urls_and_text:
        print(f"Found {len(xlsx_urls_and_text)} XLSX links.")
        for url, text in xlsx_urls_and_text:
            print(f"Text: {text}, URL: {url}")

        # Sort the links based on their visible text lexicographically
        xlsx_urls_and_text.sort(key=lambda x: x[1])  # Sort by the visible text

        # Get the most recent file after sorting by visible text
        sorted_latest_url = xlsx_urls_and_text[-1][0]  # Get URL of the last item in the sorted list
        print(f"\nLatest file URL (sorted lexicographically by text): {sorted_latest_url}")

        # Download the most recent file
        response = requests.get(sorted_latest_url)
        if response.status_code == 200:
            file_name = sorted_latest_url.split("/")[-1]
            with open(file_name, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded the latest file: {file_name}")
        else:
            print("Failed to download the latest file.")

    else:
        print("No XLSX links found on the page.")

except Exception as e:
    print(f"Error: {e}")

# Close the driver
driver.quit()

# %%
