import argparse
import json

import pandas as pd

from utils.logger import get_logger

logger = get_logger("validator", "data_validation.log")

CRITICAL_METRICS = ["Revenue", "Net Income", "EPS", "Total Assets", "Total Liabilities"]

VALUE_RANGES = {
    "EPS": (-1000, 1000),
    "Gross Margin": (0, 1),
    "Current Ratio": (0, 10),
}


class FinancialDataValidator:
    def __init__(self, data_filepath):
        self.df = pd.read_csv(data_filepath)
        self.results = {"passed": True, "errors": [], "warnings": []}

    def _error(self, msg):
        self.results["passed"] = False
        self.results["errors"].append(msg)
        logger.error(msg)

    def _warn(self, msg):
        self.results["warnings"].append(msg)
        logger.warning(msg)

    def check_missing_values(self):
        for metric in CRITICAL_METRICS:
            missing = self.df[self.df["Metric"] == metric]["Value"].isnull().sum()
            if missing > 0:
                self._error(f"Missing values for {metric}: {missing} instance(s)")

    def check_data_types(self):
        try:
            non_numeric = pd.to_numeric(self.df["Value"], errors="coerce").isnull().sum()
            if non_numeric > 0:
                self._error(f"Non-numeric values found: {non_numeric} instance(s)")
        except Exception as e:
            self._error(f"Data type check failed: {e}")

    def check_value_ranges(self):
        for metric, (lo, hi) in VALUE_RANGES.items():
            subset = self.df[self.df["Metric"] == metric]
            if subset.empty:
                continue
            values = pd.to_numeric(subset["Value"], errors="coerce")
            out_of_range = ((values < lo) | (values > hi)).sum()
            if out_of_range > 0:
                self._warn(f"{metric}: {out_of_range} value(s) outside expected range [{lo}, {hi}]")

    def check_balance_sheet(self):
        pivot = self.df.pivot_table(
            index=["Company", "Quarter", "Year"],
            columns="Metric",
            values="Value",
            aggfunc="first",
        ).reset_index()

        needed = ["Total Assets", "Total Liabilities", "Total Equity"]
        if not all(c in pivot.columns for c in needed):
            return

        diff = abs(pivot["Total Assets"] - (pivot["Total Liabilities"] + pivot["Total Equity"]))
        inconsistent = (diff > pivot["Total Assets"] * 0.001).sum()
        if inconsistent > 0:
            self._warn(f"Balance sheet equation inconsistent in {inconsistent} instance(s)")

    def validate(self):
        self.check_missing_values()
        self.check_data_types()
        self.check_value_ranges()
        self.check_balance_sheet()
        return self.results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate financial data")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()

    validator = FinancialDataValidator(args.input)
    results = validator.validate()

    if results["passed"]:
        print(f"Validation passed with {len(results['warnings'])} warning(s)")
    else:
        print(f"Validation failed with {len(results['errors'])} error(s)")
        for err in results["errors"]:
            print(f"  {err}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
