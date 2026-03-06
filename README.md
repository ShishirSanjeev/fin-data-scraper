# fin_data_scraper

ETL pipeline for extracting, transforming, validating, and uploading NSE financial data.

## Structure

```
fin_data_scraper/
├── run_pipeline.py          # Entry point - orchestrates the full ETL pipeline
├── config.json              # API credentials and company tickers
├── requirements.txt
│
├── pipeline/                # Core ETL modules
│   ├── data_extractor.py    # Pulls financial data via yfinance, web search, or GPT-4
│   ├── data_transformer.py  # Reshapes raw data into structured CSV format
│   ├── data_validator.py    # Validates metrics, types, ranges, and balance sheet consistency
│   └── data_uploader.py     # Uploads output via REST API or SFTP
│
├── scripts/
│   └── sector_mapping.py    # Maps NSE industries to sectors using the sector CSV
│
├── utils/
│   └── logger.py            # Centralised logging (console + optional file handler)
│
└── data/
    ├── core-watchlist.csv          # 532 NSE stocks with 57 financial metrics
    ├── Sector mapping - Sheet1.csv # 21 sectors with sub-sector labels
    └── NSE500_with_Sector.csv      # core-watchlist with sector column appended
```

## Components

### run_pipeline.py
Orchestrates extract → transform → validate → upload for each company in the config.
Can be driven by a JSON config file or CLI flags.

### pipeline/data_extractor.py
Three extraction methods:
- `yfinance` — pulls income statement, balance sheet, and cash flow via the yfinance API
- `search` — uses Selenium to find financial statement URLs, then scrapes the page
- `gpt` — parses unstructured financial text using GPT-4

### pipeline/data_transformer.py
Takes raw extraction output and produces a normalised DataFrame with consistent metric names and quarter/year labels. Handles both structured (yfinance dict) and unstructured (GPT text) inputs.

### pipeline/data_validator.py
`FinancialDataValidator` checks:
- Missing values in critical metrics
- Correct data types
- Values within expected ranges (e.g. EPS, gross margin, current ratio)
- Balance sheet consistency (assets = liabilities + equity)

Returns a dict with `passed` (bool), `errors`, and `warnings`.

### pipeline/data_uploader.py
- `APIUploader` — POSTs CSV data to a REST endpoint with API key auth
- `SFTPUploader` — uploads file to a remote path via SFTP (password or key-based)

### scripts/sector_mapping.py
Reads `core-watchlist.csv` and `Sector mapping - Sheet1.csv`, matches each industry to a sector using substring matching, and writes the result to `NSE500_with_Sector.csv`.

### utils/logger.py
`get_logger(name, log_file=None)` returns a logger with a console handler and an optional file handler. Used by all pipeline modules.

## Usage

### Run the full pipeline

```bash
python run_pipeline.py
```

Defaults to yfinance extraction and API upload for the three companies in `DEFAULT_CONFIG`.

### Run with a config file

```bash
python run_pipeline.py --config config.json
```

### Run for a single company

```bash
python run_pipeline.py --company "HDFC Bank Ltd" --extraction yfinance --upload api
```

### Run sector mapping

```bash
python scripts/sector_mapping.py
```

## Config

`config.json` keys:

| Key | Description |
|-----|-------------|
| `companies` | Dict of `"Company Name": "NSE:TICKER"` |
| `extraction_method` | `yfinance`, `search`, or `gpt` |
| `upload_method` | `api` or `sftp` |
| `quarter` / `quarter_label` / `year` | Period labels for output files |

Credentials are read from environment variables: `WEBSITE_API_URL`, `WEBSITE_API_KEY`, `OPENAI_API_KEY`, `SFTP_HOST`, `SFTP_USER`, `SFTP_PASSWORD`, `SFTP_KEY_PATH`.

## Dependencies

```bash
pip install -r requirements.txt
```
