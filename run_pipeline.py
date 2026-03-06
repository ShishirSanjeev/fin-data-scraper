import argparse
import json
import os
from datetime import datetime

from pipeline.data_extractor import get_financial_data_yfinance, search_financial_statements, extract_financials_gpt4
from pipeline.data_transformer import transform_financial_data
from pipeline.data_validator import FinancialDataValidator
from pipeline.data_uploader import APIUploader, SFTPUploader
from utils.logger import get_logger

logger = get_logger("pipeline", "etl_pipeline.log")

DEFAULT_CONFIG = {
    "companies": {
        "HDFC Bank Ltd": "NSE:HDFCBANK",
        "Granules India Ltd": "NSE:GRANULES",
        "Tata Consultancy Services Ltd": "NSE:TCS",
    },
    "extraction_method": "yfinance",
    "upload_method": "api",
    "quarter": "1 2024",
    "quarter_label": "Q1",
    "year": 2024,
    "api_url": os.environ.get("WEBSITE_API_URL", ""),
    "api_key": os.environ.get("WEBSITE_API_KEY", ""),
    "openai_api_key": os.environ.get("OPENAI_API_KEY", ""),
    "sftp_host": os.environ.get("SFTP_HOST", ""),
    "sftp_user": os.environ.get("SFTP_USER", ""),
    "sftp_password": os.environ.get("SFTP_PASSWORD", ""),
    "sftp_key_path": os.environ.get("SFTP_KEY_PATH", ""),
    "sftp_remote_dir": "/www/data/financial",
}


def extract(method, company, ticker=None, quarter=None, api_key=None):
    logger.info(f"Extracting [{method}] for {company}")
    if method == "yfinance":
        return get_financial_data_yfinance(ticker)
    elif method == "search":
        return search_financial_statements(company, quarter)
    elif method == "gpt":
        raise ValueError("GPT extraction requires text input - use data_extractor.py directly")


def transform_and_save(data, company, quarter, year):
    output_file = f"{company.replace(' ', '_')}_{quarter}_{year}_financials.csv"
    df = transform_financial_data(data, company, quarter, year)
    df.to_csv(output_file, index=False)
    logger.info(f"Saved {output_file}")
    return output_file


def validate(csv_path):
    return FinancialDataValidator(csv_path).validate()


def upload(method, csv_path, cfg):
    if method == "api":
        return APIUploader(cfg["api_url"], cfg["api_key"]).upload(csv_path)
    elif method == "sftp":
        remote = f"{cfg.get('sftp_remote_dir', '/uploads')}/{os.path.basename(csv_path)}"
        return SFTPUploader(
            cfg["sftp_host"], cfg["sftp_user"],
            cfg.get("sftp_password"), cfg.get("sftp_key_path")
        ).upload(csv_path, remote)


def run_pipeline(companies, extraction_method, upload_method, cfg):
    for company, ticker in companies.items():
        logger.info(f"Processing {company} ({ticker})")

        try:
            raw = extract(extraction_method, company, ticker=ticker,
                          quarter=cfg.get("quarter"), api_key=cfg.get("openai_api_key"))
        except Exception as e:
            logger.error(f"Extraction failed for {company}: {e}")
            continue

        try:
            csv_path = transform_and_save(raw, company, cfg.get("quarter_label", "Q1"),
                                          cfg.get("year", datetime.now().year))
        except Exception as e:
            logger.error(f"Transformation failed for {company}: {e}")
            continue

        results = validate(csv_path)
        if not results["passed"]:
            logger.error(f"Validation failed for {company}, skipping upload")
            for err in results["errors"]:
                logger.error(f"  {err}")
            continue

        ok = upload(upload_method, csv_path, cfg)
        if ok:
            logger.info(f"Uploaded {company} data")
        else:
            logger.error(f"Upload failed for {company}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run financial ETL pipeline")
    parser.add_argument("--config", help="Path to JSON config file")
    parser.add_argument("--company", help="Single company name to process")
    parser.add_argument("--extraction", choices=["search", "yfinance", "gpt"])
    parser.add_argument("--upload", choices=["api", "sftp"])
    args = parser.parse_args()

    cfg = DEFAULT_CONFIG.copy()

    if args.config:
        with open(args.config, "r") as f:
            cfg.update(json.load(f))

    if args.extraction:
        cfg["extraction_method"] = args.extraction
    if args.upload:
        cfg["upload_method"] = args.upload

    if args.company:
        if args.company not in cfg["companies"]:
            print(f"Company '{args.company}' not found in config")
            exit(1)
        cfg["companies"] = {args.company: cfg["companies"][args.company]}

    run_pipeline(cfg["companies"], cfg["extraction_method"], cfg["upload_method"], cfg)
