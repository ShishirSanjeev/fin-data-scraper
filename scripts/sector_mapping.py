import pandas as pd


def load_sector_map(sector_mapping_path):
    df = pd.read_csv(sector_mapping_path)
    return df


def build_industry_sector_map(nse_df, sector_map):
    industry_sector = {}
    for industry in nse_df["Industry"].dropna().unique():
        for _, row in sector_map.iterrows():
            if str(row["Sub-Sector"]).lower() in industry.lower():
                industry_sector[industry] = row["Sector"]
                break
    return pd.DataFrame(
        list(industry_sector.items()), columns=["Industry", "Sector"]
    )


def merge_sector_to_watchlist(watchlist_path, sector_mapping_path, output_path):
    nse_df = pd.read_csv(watchlist_path)
    sector_map = load_sector_map(sector_mapping_path)
    industry_sector = build_industry_sector_map(nse_df, sector_map)
    result = pd.merge(nse_df, industry_sector, on="Industry", how="left")
    result.to_csv(output_path, index=False)
    return result


if __name__ == "__main__":
    merge_sector_to_watchlist(
        "data/core-watchlist.csv",
        "data/Sector mapping - Sheet1.csv",
        "data/NSE500_with_Sector.csv",
    )
