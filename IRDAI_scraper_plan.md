# IRDAI Agent Locator — Full Async Production Pipeline

## 1. Objective

Build a **high-throughput, production-grade async scraper** to extract:

> All insurance agents across India

Segmented by:
- PIN Code (primary key)
- Insurance Type
- Insurer

---

## 2. System Nature

IRDAI portal is:
- ASP.NET WebForms backend
- Session-based
- Multi-dimensional filtering
- No direct bulk export

➡️ Requires **query orchestration**

---

## 3. Core APIs

### 3.1 Metadata APIs

#### Insurance Types
```
POST /_WebService/General/DataLoader.asmx/GetInsurerType
```

#### Insurers
```
POST /_WebService/General/DataLoader.asmx/GetInsurer
Body: { InsuranceType }
```

#### States (optional)
```
POST /_WebService/General/DataLoader.asmx/GetState
```

#### Districts (optional)
```
POST /_WebService/General/DataLoader.asmx/GetDistrict
Body: { StateID }
```

---

### 3.2 Main Data API

```
POST /_WebService/PublicAccess/AgentLocator.asmx/LocateAgent
```

---

## 4. Core Parameter (Critical)

All filters combine into:

```
customquery
```

Format:
```
,,,InsuranceType,Insurer,State,District,PIN
```

We optimize to:

```
,,,InsuranceType,Insurer,,,PIN
```

---

## 5. Final Extraction Strategy

### Primary Key = PIN

```
FOR each PIN
    FOR each InsuranceType
        FOR each Insurer
            CALL LocateAgent
```

---

## 6. Pagination

Parameters:

```
page=1
rp=9999
```

Loop:

```
page++ until empty
```

---

## 7. Async Architecture

```
PIN List
   ↓
Async Queue
   ↓
(Type × Insurer)
   ↓
LocateAgent API
   ↓
Parse
   ↓
Store
```

---

## 8. Project Structure

```
irdai-scraper/
  ├── src/
  │   ├── config.py
  │   ├── metadata.py
  │   ├── async_fetch.py
  │   ├── parser.py
  │   ├── pipeline.py
  │   └── merge.py
  ├── data/
  │   ├── raw/
  │   └── master.parquet
  ├── pins.csv
  └── requirements.txt
```

---

## 9. Configuration

```python
BASE_URL = "https://agencyportal.irdai.gov.in/_WebService/PublicAccess/AgentLocator.asmx/LocateAgent"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://agencyportal.irdai.gov.in/PublicAccess/AgentLocator.aspx",
    "Content-Type": "application/x-www-form-urlencoded"
}

SEM_LIMIT = 20
TIMEOUT = 30
RETRIES = 3
```

---

## 10. Metadata Loader

```python
# src/metadata.py

import httpx

BASE = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx"

async def get_types(client):
    r = await client.post(f"{BASE}/GetInsurerType", content="{}")
    return r.text

async def get_insurers(client, type_id):
    r = await client.post(
        f"{BASE}/GetInsurer",
        content=f"{{InsuranceType:'{type_id}'}}"
    )
    return r.text
```

---

## 11. Async Fetcher

```python
# src/async_fetch.py

import asyncio
import httpx
import os

OUT = "data/raw"
os.makedirs(OUT, exist_ok=True)

async def fetch(client, sem, pin, t, insurer):
    async with sem:
        payload = (
            f"page=1&rp=9999&sortname=AgentName&sortorder=asc"
            f"&customquery=,,,{t},{insurer},,,{pin}"
        )

        r = await client.post(
            "https://agencyportal.irdai.gov.in/_WebService/PublicAccess/AgentLocator.asmx/LocateAgent",
            content=payload
        )

        if len(r.text) > 1000:
            fname = f"{OUT}/{pin}_{t}_{insurer}.xml"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(r.text)
            return fname
        return None
```

---

## 12. Runner

```python
# src/pipeline.py

import asyncio
import httpx
from async_fetch import fetch

async def run(pins, types, insurers_map):
    sem = asyncio.Semaphore(20)

    async with httpx.AsyncClient(timeout=30) as client:
        tasks = []

        for pin in pins:
            for t in types:
                for insurer in insurers_map[t]:
                    tasks.append(fetch(client, sem, pin, t, insurer))

        await asyncio.gather(*tasks)
```

---

## 13. Parsing

```python
# src/parser.py

from bs4 import BeautifulSoup
import pandas as pd


def parse_file(path):
    with open(path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "xml")

    rows = []
    for tr in soup.find_all("tr"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    return pd.DataFrame(rows)
```

---

## 14. Merge

```python
# src/merge.py

import os
import pandas as pd

files = os.listdir("data/raw")

dfs = []
for f in files:
    df = pd.read_xml(f"data/raw/{f}")
    dfs.append(df)

final = pd.concat(dfs, ignore_index=True)

final = final.drop_duplicates(subset=["IRDA URN", "License No"])

final.to_parquet("data/master.parquet", index=False)
```

---

## 15. Requirements

```
httpx
asyncio
pandas
beautifulsoup4
lxml
pyarrow
```

---

## 16. Execution

```bash
python src/pipeline.py
python src/merge.py
```

---

## 17. Performance

| Mode | Time |
|------|------|
| Sequential | Hours |
| Async | 10–30 min |

---

## 18. Key Optimizations

- Use PIN as primary key
- Skip empty responses
- Deduplicate aggressively
- Keep concurrency moderate

---

## 19. Final Summary

This pipeline:
- Fully bypasses UI
- Uses direct WebService calls
- Scales across India
- Produces structured dataset

---

**End of IRDAI Document**

