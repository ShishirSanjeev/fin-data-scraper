import argparse
import json
import re

import pandas as pd

METRICS = [
    "Revenue", "Cost of Revenue", "Gross Profit", "Operating Expenses",
    "Operating Income", "Net Income", "EPS", "Diluted EPS",
    "Total Assets", "Current Assets", "Cash and Cash Equivalents",
    "Total Liabilities", "Current Liabilities", "Total Equity",
    "Operating Cash Flow", "Capital Expenditures", "Free Cash Flow",
]

UNSTRUCTURED_PATTERNS = {
    "Revenue": r"Revenue[:\s]+([\$\d,\.]+)[\s\n]",
    "Net Income": r"Net Income[:\s]+([\$\d,\.]+)[\s\n]",
    "EPS": r"Earnings Per Share[:\s]+([\$\d,\.]+)[\s\n]",
}


def parse_unstructured_data(text_data):
    data = {}
    for metric, pattern in UNSTRUCTURED_PATTERNS.items():
        match = re.search(pattern, text_data)
        if match:
            value_str = match.group(1).replace("$", "").replace(",", "")
            try:
                data[metric] = float(value_str)
            except ValueError:
                data[metric] = value_str
    return data


def transform_financial_data(raw_data, company_name, quarter, year):
    try:
        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
    except json.JSONDecodeError:
        data = parse_unstructured_data(raw_data)

    rows = [
        {"Company": company_name, "Quarter": quarter, "Year": year, "Metric": m, "Value": data[m]}
        for m in METRICS if m in data
    ]
    return pd.DataFrame(rows, columns=["Company", "Quarter", "Year", "Metric", "Value"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform financial data")
    parser.add_argument("--input", required=True)
    parser.add_argument("--company", required=True)
    parser.add_argument("--quarter", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    with open(args.input, "r") as f:
        raw_data = f.read()

    df = transform_financial_data(raw_data, args.company, args.quarter, args.year)
    df.to_csv(args.output, index=False)
    print(f"Saved to {args.output}")
