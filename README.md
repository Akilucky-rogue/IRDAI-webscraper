
IRDAI Agent Locator
Production Scraper v2
Complete Usage Manual

Comprehensive guide for extracting all insurance agent data
from the IRDAI Agency Portal across India

April 2026
 
 
1. Overview	4
Key Features	4
2. Critical API Limitation	5
3. Prerequisites	6
System Requirements	6
Python Dependencies	6
Required Files	6
4. Quick Start Guide	7
Step 1: Install Dependencies	7
Step 2: Explore Available States	7
Step 3: Run a Test	7
Step 4: Scrape a State	7
Step 5: Full India Scrape	7
5. State-by-State Scraping	8
Scrape a Single State	8
Scrape Multiple States	8
Scrape a Specific District	8
Combine with Type Filters	8
Resume a State Scrape	8
Merge State Output	8
Output Folder Structure (Single State)	8
6. CLI Reference	9
7. Output Files	10
Raw CSVs (data/raw/ or data/STATE/raw/)	10
Master CSV	10
Column Reference	10
Truncated Queries Log	10
Progress File	10
Master Parquet	10
8. Insurance Types & Insurer IDs	11
9. States & PIN Code Coverage	12
10. API Endpoints Reference	13
11. Troubleshooting	14
No PIN codes matched the given filters	14
High number of failed requests	14
Many truncated queries	14
Scrape interrupted / connection lost	14
Disk space concerns	14
Import errors (httpx, pandas)	14
12. Performance Estimates	15
13. File Structure	16
14. Recommended Workflow for Full India Extraction	17
15. Audit & Verification Log	18
Data Verification (PIN 400002, Mumbai)	18
Broader Validation (20 Delhi PINs)	18
API Cap Verification	18
State Filter Verification (Goa)	18
Bug Fixes Applied	18

 
1. Overview
The IRDAI Agent Locator Scraper is a Python-based tool that extracts insurance agent data from the IRDAI Agency Portal (agencyportal.irdai.gov.in). It systematically queries every PIN code in India against every insurance company under each insurance type (General, Life, Health) to build a complete database of registered insurance agents.
The scraper supports state-by-state extraction, allowing you to pull data for a single state, a specific district, or the entire country. Output is organized into clean CSV and Parquet files, with full resume capability for interrupted runs.
Key Features
•	State-by-state and district-level scraping with --state and --district filters
•	19,586 unique PIN codes covering all 37 Indian states and union territories
•	66 insurance companies across 3 types (General, Life, Health)
•	Async architecture with 20 concurrent requests for fast extraction
•	Resume capability: pick up exactly where you left off after interruption
•	Truncation detection: flags queries that hit the API 15-result cap
•	Automatic deduplication and master file generation (CSV + Parquet)
•	State-wise output folders for organized data management
 
2. Critical API Limitation
WARNING: The IRDAI API hard-caps results at 15 per query, regardless of requested page size. The server ignores the rp (rows per page) parameter and always returns a maximum of 15 agent records per request. Pagination beyond page 1 with large page sizes returns empty results.
This limitation was discovered through systematic testing:
•	rp=9999 (requesting 9,999 per page): returns only 15 agents
•	rp=15 (requesting 15 per page): returns 15 agents
•	rp=5 with pagination across 3 pages: returns 5+5+5 = 15 total
•	page=2 with rp=9999: returns 0 agents
The scraper solves this by querying at the PIN code level. Since most PIN + insurer combinations have fewer than 15 agents, the data is complete. The minority of queries that hit the 15-result cap are flagged in truncated_queries.csv for review.
 
3. Prerequisites
System Requirements
•	Python 3.9 or higher
•	pip (Python package manager)
•	Stable internet connection
•	Sufficient disk space (approximately 500 MB–1 GB for the full India scrape)
Python Dependencies
pip install httpx pandas pyarrow
Package	Version	Purpose
httpx	>=0.25	Async HTTP client for API requests
pandas	>=2.0	Data merging, deduplication, analysis
pyarrow	>=14.0	Parquet file output (optional)
Required Files
•	irdai_scraper.py — The main scraper script
•	pins_master.csv — Master list of 19,586 Indian PIN codes with state and district columns
 
4. Quick Start Guide
Follow these steps to start scraping:
Step 1: Install Dependencies
pip install httpx pandas pyarrow
Step 2: Explore Available States
python irdai_scraper.py --list-states
This prints all 37 states/UTs with their districts and PIN counts, so you can plan your scrape.
Step 3: Run a Test
python irdai_scraper.py --test
Runs a quick test with 5 PINs, 1 insurance type, and 1 insurer to verify connectivity.
Step 4: Scrape a State
python irdai_scraper.py --state "GOA"
Scrapes all 89 Goa PINs across all 3 insurance types and all 66 insurers. Output goes to data/GOA/.
Step 5: Full India Scrape
python irdai_scraper.py --resume
Scrapes all 19,586 PINs across all types and insurers. Use --resume to safely restart if interrupted. Estimated time: 10–20 hours.
 
5. State-by-State Scraping
The scraper supports pulling data for specific states and districts. This is the recommended approach for large-scale extraction — scrape one state at a time for manageable output and easy resume.
Scrape a Single State
python irdai_scraper.py --state "MAHARASHTRA"
Scrapes all 1,600 Maharashtra PINs. Output is saved to data/MAHARASHTRA/ with its own progress tracker, so you can resume independently.
Scrape Multiple States
python irdai_scraper.py --state "GOA" "DELHI" "CHANDIGARH"
Note: When scraping multiple states, output goes to the shared data/ directory rather than a state-specific folder.
Scrape a Specific District
python irdai_scraper.py --state "KARNATAKA" --district "BENGALURU URBAN"
Filters PINs to only those in the specified district within the state.
Combine with Type Filters
python irdai_scraper.py --state "DELHI" --types 1 2
Scrapes only General (1) and Life (2) insurance for Delhi PINs.
Resume a State Scrape
python irdai_scraper.py --state "MAHARASHTRA" --resume
Picks up exactly where the Maharashtra scrape left off. Each single-state scrape has its own progress.json inside data/STATE_NAME/.
Merge State Output
python irdai_scraper.py --state "MAHARASHTRA" --merge-only
Re-merges the raw CSVs into the master file without re-scraping. Useful after manual edits.
Output Folder Structure (Single State)
data/MAHARASHTRA/
  raw/
    agents_general.csv
    agents_life.csv
    agents_health.csv
  agents_MAHARASHTRA_master.csv
  agents_MAHARASHTRA_master.parquet
  progress.json
  truncated_queries.csv
 
6. CLI Reference
Complete list of all command-line arguments:
Flag	Default	Description
--state	(none)	State name(s) to filter PINs. Case-insensitive. E.g. --state "MAHARASHTRA"
--district	(none)	District name(s) to filter PINs. Best used with --state for precision.
--list-states	false	List all states, districts, and PIN counts, then exit.
--types	all	Insurance type IDs: 1=General, 2=Life, 3=Health
--insurers	all	Specific insurer user IDs to scrape
--pins-file	pins_master.csv	Path to the PIN codes CSV file
--max-pins	(none)	Limit number of PINs to scrape (for testing)
--max-insurers	(none)	Limit insurers per type (for testing)
--concurrency	20	Max parallel HTTP requests
--batch-size	50	PINs processed per batch before saving progress
--resume	false	Skip previously completed PIN+Type combinations
--merge-only	false	Only merge existing raw CSVs into master file
--test	false	Quick test: 5 PINs, 1 type, 1 insurer
 
7. Output Files
Raw CSVs (data/raw/ or data/STATE/raw/)
One CSV per insurance type (agents_general.csv, agents_life.csv, agents_health.csv). These are append-only during scraping and may contain duplicates. Used as intermediate files.
Master CSV
The deduplicated, merged output: irdai_agents_master.csv (full scrape) or agents_STATE_master.csv (state scrape). This is the primary deliverable.
Column Reference
Column	Description
AgentID_Internal	IRDAI internal database ID
AgentName	Full name of the agent (Mr./Mrs./Ms. prefix)
LicenseNo	License number issued by IRDAI
IRDA_URN	Unique Registration Number from IRDAI
Agent_ID	Composite agent identifier
InsuranceType	General, Life, or Health
Insurer	Full name of the insurance company
DP_ID	Distribution point ID
State	State name (from API response)
District	District name (from API response)
PINCode	6-digit Indian PIN code
ValidFrom	License validity start date
ValidTo	License validity end date
AbsorbedAgent	Whether the agent was absorbed (Yes/No)
PhoneNo	Agent phone number
MobileNo	Agent mobile number
Truncated Queries Log
truncated_queries.csv lists all queries that returned exactly 15 results (the API cap). These may have more agents than captured. Review this file to assess data completeness.
Progress File
progress.json tracks completed PIN+Type combinations for the --resume feature. Delete this file to force a fresh scrape.
Master Parquet
Same data as the master CSV but in Apache Parquet format. Ideal for loading into Pandas, Spark, or any data warehouse. About 60% smaller file size.
 
8. Insurance Types & Insurer IDs
The scraper automatically fetches the latest insurance types and insurer lists from the IRDAI API at each run. Below are the types and approximate insurer counts as of April 2026:
Type ID	Name	Insurers	Example Companies
1	General Insurance	30	Bajaj, ICICI, New India
2	Life Insurance	27	LIC, HDFC, SBI Life
3	Health Insurance	9	Star Health, Niva Bupa
To scrape only specific types, use the --types flag with type IDs:
python irdai_scraper.py --types 1        # General only
python irdai_scraper.py --types 2 3      # Life + Health
 
9. States & PIN Code Coverage
The scraper covers all 37 Indian states and union territories. Use --list-states to see the full list with district breakdowns. Here are the major states by PIN count:
State	PINs	Est. Time
Tamil Nadu	2,049	~2 hours
Uttar Pradesh	1,668	~1.5 hours
Maharashtra	1,600	~1.5 hours
Kerala	1,428	~1.3 hours
Karnataka	1,359	~1.2 hours
Andhra Pradesh	1,262	~1.1 hours
West Bengal	1,131	~1 hour
Gujarat	1,010	~55 min
Rajasthan	1,017	~55 min
Delhi	103	~6 min
Goa	89	~5 min
Chandigarh	23	~2 min
Times assume: 20 concurrent requests (~6–7 queries/second), all 3 insurance types, and all 66 insurers. Actual time varies with network speed and API response time.
 
10. API Endpoints Reference
The scraper uses the following IRDAI API endpoints. These are documented here for transparency and troubleshooting.
Endpoint	URL
GetInsurerType	/_WebService/General/DataLoader.asmx/GetInsurerType
GetInsurer	/_WebService/General/DataLoader.asmx/GetInsurer
GetState	/_WebService/General/DataLoader.asmx/GetState
GetDistrict	/_WebService/General/DataLoader.asmx/GetDistrict
LocateAgent	/_WebService/PublicAccess/AgentLocator.asmx/LocateAgent
Base URL: https://agencyportal.irdai.gov.in
The customquery parameter format for LocateAgent is: Name,LicenseNo,AgentID,InsuranceType,InsurerID,StateID,DistrictID,PINCode. Fields are comma-separated, with empty fields left blank.
 
11. Troubleshooting
No PIN codes matched the given filters
The state or district name you entered does not match any entry in pins_master.csv. Names are case-insensitive but must match exactly. Run --list-states to see all valid names.
High number of failed requests
The IRDAI server may be under load or temporarily unavailable. The scraper retries each request up to 4 times with exponential backoff (2s, 4s, 8s, 16s). If failures persist, reduce concurrency:
python irdai_scraper.py --state "MAHARASHTRA" --concurrency 10
Many truncated queries
Truncated queries indicate a PIN + insurer combination has more than 15 agents. This is uncommon at the PIN level but can happen in dense urban areas. Review truncated_queries.csv to identify affected areas. There is no way to retrieve more than 15 results per query from this API.
Scrape interrupted / connection lost
Simply re-run with --resume. The scraper tracks completed PIN+Type combinations and skips them:
python irdai_scraper.py --state "MAHARASHTRA" --resume
Disk space concerns
Raw CSVs accumulate during scraping. A full India scrape produces roughly 300–800 MB of raw data. The master CSV after deduplication is typically 30–50% smaller. Use --merge-only after scraping to regenerate the master file if needed.
Import errors (httpx, pandas)
pip install httpx pandas pyarrow
 
12. Performance Estimates
The scraper processes approximately 6–7 API queries per second at the default concurrency of 20. Each PIN code requires up to 66 queries (one per insurer across the selected types).
Scope	PINs	Queries	Est. Time
Single district	20–100	1.3K–6.6K	3–15 min
Small state (Goa)	89	5,874	~15 min
Medium state (Delhi)	103	6,798	~17 min
Large state (Maharashtra)	1,600	105,600	~4.5 hours
Full India	19,586	1,292,676	~50 hours
Times assume: 20 concurrent requests, all 3 insurance types, all 66 insurers, average server response of ~150ms. Real time varies with network conditions and server load. Scraping state by state with --resume is recommended for the full India extract.
 
13. File Structure
project/
  irdai_scraper.py          # Main scraper script
  pins_master.csv           # Input: 19,586 Indian PIN codes
  irdai_scraper.log         # Scraper log file
  data/                     # Output directory (full scrape)
    raw/                    # Intermediate per-type CSVs
      agents_general.csv
      agents_life.csv
      agents_health.csv
    irdai_agents_master.csv     # Deduplicated master output
    irdai_agents_master.parquet  # Same data, Parquet format
    progress.json               # Resume tracking
    truncated_queries.csv        # Queries that hit 15-cap
  data/MAHARASHTRA/         # State-specific output
    raw/
      agents_general.csv
      agents_life.csv
      agents_health.csv
    agents_MAHARASHTRA_master.csv
    agents_MAHARASHTRA_master.parquet
    progress.json
    truncated_queries.csv
 
14. Recommended Workflow for Full India Extraction
For extracting all agent data across India, the recommended approach is to scrape state by state. This gives you manageable output files, easy resume per state, and lets you verify results incrementally.
1.	Install dependencies: pip install httpx pandas pyarrow
2.	Run --list-states to review all 37 states and their PIN counts
3.	Start with a small state for validation: python irdai_scraper.py --state "GOA"
4.	Verify the output in data/GOA/agents_GOA_master.csv
5.	Proceed state by state, largest states last, always using --resume
6.	Check truncated_queries.csv in each state folder for completeness
7.	After all states are done, optionally combine all master CSVs into one national file using pandas
Example loop for scraping all states (bash):
for STATE in "GOA" "DELHI" "CHANDIGARH" "PUNJAB" "HARYANA" "KERALA" "KARNATAKA" "MAHARASHTRA" "TAMIL NADU" "UTTAR PRADESH"; do
  echo "Scraping $STATE..."
  python irdai_scraper.py --state "$STATE" --resume
done
 
15. Audit & Verification Log
This section documents the verification steps performed to ensure scraper accuracy.
Data Verification (PIN 400002, Mumbai)
Cross-referenced scraper output against the IRDAI portal web interface for PIN 400002, Bajaj General Insurance. The scraper returned exactly 7 agents matching the portal display: AMAR SUNKERSETT, DEEPAK KAPADIA, NILESH JAIN, RAKESH H. JAIN, Sandeep Ghisalal Nahar, Sanjay Sonthalia, and Ruchi Labhshankar Joshi. Perfect match.
Broader Validation (20 Delhi PINs)
Scraped 20 Delhi PIN codes across all 66 insurers (1,320 queries). Results: 3,775 unique agents found, 0 failures, 157 truncated queries. Zero null values in key fields (AgentName, LicenseNo, IRDA_URN, State, District, PINCode).
API Cap Verification
Systematically tested the API with varying page sizes (rp=5, 15, 100, 9999) and page numbers. Confirmed the server enforces a hard cap of 15 results per query regardless of parameters.
State Filter Verification (Goa)
Tested --state "GOA" with 10 PINs and all General insurers. Results: 124 unique agents found across North Goa and South Goa. All State, District, and PINCode fields populated correctly. Output correctly saved to data/GOA/.
Bug Fixes Applied
•	Fixed: Failed HTTP requests were incorrectly counted as empty (0 agents) instead of failed. Now tracked separately.
•	Fixed: Truncation warning message referenced wrong file path when using state filters.
•	Fixed: State/district filter comparison was rebuilt per-row (165K rows). Now pre-computed as sets.
•	Added: Warning when --district is used without --state (could match across multiple states).
