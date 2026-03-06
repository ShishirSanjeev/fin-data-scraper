import argparse
import json

import openai
import yfinance as yf
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def search_financial_statements(company_name, year_quarter):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    query = f"{company_name} financial statements Q{year_quarter} consolidated"
    driver.get(f"https://www.google.com/search?q={query}")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "search")))

    links = driver.find_elements(By.CSS_SELECTOR, "div.g a")
    results = [{"title": l.text, "url": l.get_attribute("href")} for l in links[:5]]
    driver.quit()
    return results


def get_financial_data_yfinance(ticker, period="quarterly"):
    stock = yf.Ticker(ticker)
    if period == "quarterly":
        return {
            "income_statement": stock.quarterly_income_stmt,
            "balance_sheet": stock.quarterly_balance_sheet,
            "cash_flow": stock.quarterly_cashflow,
        }
    return {
        "income_statement": stock.income_stmt,
        "balance_sheet": stock.balance_sheet,
        "cash_flow": stock.cashflow,
    }


def extract_financials_gpt4(text_data, openai_api_key):
    openai.api_key = openai_api_key
    metrics = "Revenue, Cost of Revenue, Gross Profit, Operating Income, Net Income, EPS, Total Assets, Total Liabilities, Equity"
    prompt = (
        f"Extract the following financial metrics from this quarterly financial statement text. "
        f"Return the data in JSON format with the metric name as key and value as a number:\n\n"
        f"Metrics to extract: {metrics}\n\n"
        f"Financial statement text:\n{text_data[:4000]}"
    )
    response = openai.ChatCompletion.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "You are a financial data extraction assistant that outputs only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        max_tokens=1500,
    )
    return response.choices[0].message.content


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract financial data")
    parser.add_argument("--method", choices=["search", "yfinance", "gpt"], required=True)
    parser.add_argument("--company", required=True)
    parser.add_argument("--ticker")
    parser.add_argument("--quarter")
    parser.add_argument("--api_key")
    parser.add_argument("--output")
    args = parser.parse_args()

    if args.method == "search":
        if not args.quarter:
            parser.error("--quarter is required for search method")
        results = search_financial_statements(args.company, args.quarter)
        print(json.dumps(results, indent=2))

    elif args.method == "yfinance":
        if not args.ticker:
            parser.error("--ticker is required for yfinance method")
        data = get_financial_data_yfinance(args.ticker)
        output = {k: v.to_json() for k, v in data.items()}
        if args.output:
            with open(args.output, "w") as f:
                json.dump(output, f)
        else:
            print(json.dumps(output, indent=2))

    elif args.method == "gpt":
        if not args.api_key:
            parser.error("--api_key is required for gpt method")
        print("GPT extraction requires text data input from a file")
