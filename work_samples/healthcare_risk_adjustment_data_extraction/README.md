## README: Selenium Data Refresh Scripts for Claim Severity Estimator Inputs
#### Overview

These Selenium-based automation scripts were created to reduce the turnaround time for external data refreshes from roughly two weeks to two days by automating the retrieval of CMS/HHS source files used in downstream model input preparation. They support a more reliable and repeatable refresh process for the Claim Severity Estimator and related scoring workflows used by FNX, Post-Pay, and IDR.

The scripts focus on downloading the latest publicly available source files from CMS pages, organizing them into year-based directories, and, where applicable, extracting ZIP contents for downstream processing. The implementation uses Selenium for dynamic page navigation and link discovery, plus requests for direct file download. 

hhs_data

 

cms_data

Because UnitedHealthcare is also using the API in its own operational processes, keeping these source files current is increasingly important. Ownership was placed under CORE AI / Data Science so maintenance is handled consistently and centrally.

#### Purpose

These scripts were built to automate the most time-consuming part of the refresh cycle:

locating the latest CMS/HHS source files,

downloading them into a structured folder hierarchy,

standardizing where refreshed files land,

reducing manual effort and missed updates,

accelerating comparison of refreshed files against prior benchmark sets such as the October baseline.

At this stage, the model input files have already been compiled. The current use of these scripts supports comparison of newly retrieved source files against the October set to determine whether updates are needed for the Claim Severity Estimator.

#### Scripts Included
hhs_data.py

Downloads the latest “Technical Details (XLSX)” file from the CMS Marketplace regulations and guidance page.

Key behaviors:

opens the CMS Marketplace regulations/guidance page with Selenium,

finds all links containing “Technical Details (XLSX)”,

determines the latest file using:

parsed posted date from the file name when available,

otherwise the highest CY year or detectable year,

creates a year-specific output directory under /mnt/code/data_updates/<script_name>/<year>/,

downloads the selected XLSX file into that folder. 

hhs_data

cms_data.py

Downloads the latest CMS risk adjustment model software files and related ICD-10 mapping files.

Key behaviors:

opens the CMS Medicare Advantage risk adjustment page,

identifies year-specific model software pages from URLs matching /20xx-model-software,

selects the latest year available,

visits both:

the model software page,

the associated ICD-10 mappings page,

downloads all .zip, .xls, and .xlsx files found,

extracts ZIP files into subfolders,

stores outputs under /mnt/code/data_updates/<script_name>/<year>/. 

cms_data

#### Why This Matters

This automation improves refresh speed and operational resilience for teams that depend on scoring outputs every day.

#### Business impact

Reduced refresh cycle from ~2 weeks to ~2 days

Lower manual effort and fewer missed file updates

Faster validation against the October comparison set

More consistent central maintenance under CORE AI / Data Science

Better support for internal and external consumers, including UnitedHealthcare

#### Downstream users

FNX

Post-Pay

IDR

CORE AI / Data Science

UnitedHealthcare API consumers

#### Technical Stack

Python 3

Selenium

Chrome / ChromeDriver

requests

zipfile

logging

regular expressions for year/date detection

Both scripts run Chrome in headless mode and assume ChromeDriver is available at:

/usr/local/bin/chromedriver

This path is explicitly referenced in both scripts. 

hhs_data

 

cms_data

#### Directory Structure

Both scripts use dynamic output pathing based on script name and detected year.

#### Root output location
/mnt/code/data_updates
#### Example output layout
/mnt/code/data_updates/
├── hhs_data/
│   └── 2026/
│       └── <downloaded technical details file>.xlsx
└── cms_data/
    └── 2026/
        ├── <model software file>.zip
        ├── <model software file extracted>/
        ├── <icd mapping file>.xlsx
        └── ...

The year-based foldering is built directly into both scripts. 

hhs_data

 

cms_data

#### Prerequisites

Install required Python packages:

pip install selenium requests

System requirements:

Google Chrome installed

ChromeDriver installed and compatible with the local Chrome version

access to the target CMS public pages

write permission to /mnt/code/data_updates

#### How to Run
#### Run the marketplace technical details refresh
python hhs_data.py
#### Run the CMS risk adjustment/model software refresh
python cms_data.py
#### What Each Script Produces
hhs_data.py

Produces:

the latest detected Marketplace Technical Details XLSX

saved into a year-based folder

Selection logic:

first tries to parse a posted date from the file name,

if no usable date is found, falls back to CY year or detected year in the link text/URL. 

hhs_data

cms_data.py

Produces:

all downloadable .zip, .xls, and .xlsx files found on:

the latest model software page,

the related ICD-10 mappings page

extracted ZIP contents in subfolders under the same year directory. 

cms_data

#### Suggested Refresh Workflow

Run hhs_data.py

Run cms_data.py

Confirm the latest year folder was created

Review downloaded files for completeness

Compare refreshed source files against the October set

Determine whether source-level changes require updates to:

model inputs,

transformation logic,

the Claim Severity Estimator

This keeps the external data refresh step fast and repeatable while preserving analyst review for actual model-impact decisions.

#### Logging and Monitoring
hhs_data.py

Prints:

all discovered Technical Details XLSX links,

the selected latest file URL,

detected year,

output directory,

final download path,

any runtime errors. 

hhs_data

cms_data.py

Uses Python logging to record:

navigation progress,

detected latest year,

URLs visited,

files downloaded,

ZIP extraction status,

warnings for failed downloads or bad ZIP files. 

cms_data

#### Maintenance Notes

These scripts depend on the current structure of CMS public web pages, so maintenance may be needed if:

page URLs change,

anchor text changes,

downloadable file types change,

the CMS site structure is reorganized.

#### Recommended maintenance checks

verify ChromeDriver compatibility regularly,

confirm the page still exposes the expected links,

validate that the newest year is being selected correctly,

spot-check extracted files before comparison runs,

keep output directories organized by year to simplify audits and rollback.

#### Known Assumptions

ChromeDriver exists at /usr/local/bin/chromedriver

CMS pages remain publicly accessible and structurally similar

files are identified correctly by extension or anchor text

year detection in URLs remains consistent for model software pages

downloaded files are the authoritative external inputs for comparison work

#### Operational Context

These scripts were originally developed to automate pulling and staging the data required for model input refreshes. With the compiled model input files already prepared, the current focus is on comparing refreshed data against the October source set to assess whether any updates are required for the Claim Severity Estimator.

Because these scores are consumed daily by FNX, Post-Pay, and IDR, and because UnitedHealthcare now also uses the API in its own workflows, keeping the source files current is a production support need rather than a one-time data pull exercise.
