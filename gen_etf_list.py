import mstarpy
import pandas as pd
import time
from mstarpy import search_field, search_filter
from pandas.api.types import CategoricalDtype
import yfinance as yf
import logging
from tqdm import tqdm

# 1. Remove the row limit (shows all rows)
pd.set_option('display.max_rows', None)

# 2. Remove the column limit (shows all columns)
pd.set_option('display.max_columns', None)

# 3. Widen the console output so text doesn't wrap to the next line
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)


# This tells the logger to only show 'CRITICAL' errors
# It will hide all the 'WARNING' and 'ERROR' messages about delisted tickers.
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

def get_major_etfs():
    top_issuers = [
        # --- The Big Three & Parent Companies ---
        "Vanguard", 
        "iShares", "BlackRock",   
        "SPDR", "State Street",
        
        # --- Trillion & Multi-Billion Dollar Heavyweights ---
        "Invesco", 
        "Schwab", "Charles Schwab",
        "Fidelity",
        "JPMorgan", "J.P. Morgan",
        "First Trust",
        "Dimensional", "DFA",
        "Capital Group",
        "Morgan Stanley",
        
        # --- Major Specialists (Thematic, Smart Beta, Commodities) ---
        "WisdomTree",
        "VanEck",
        "Global X",
        "PIMCO",
        "ARK", "ARK Invest",
        "KraneShares",
        
        # --- Leveraged & Inverse Specialists ---
        "ProShares",
        "Direxion",
        "Leverage Shares",
        "GraniteShares",
        
        # --- Established Wall Street & Mutual Fund Giants ---
        "Goldman Sachs",
        "Franklin Templeton",
        "T. Rowe Price",
        "Janus Henderson",
        "DWS", "Xtrackers",
        "Nuveen",
        "American Century",
        "Northern Trust", "FlexShares",
        "Natixis",
        "UBS",
        
        # --- Niche, Income, Buffer & Options-Focused ---
        "Amplify",
        "Pacer", "Pacer ETFs",
        "Innovator",
        "Simplify",
        "YieldMax",
        "Defiance",
        "ALPS",
        "Roundhill"
    ]
    
    fields = [
        "name",
        "fundStarRating",
        "medalistRating", 
        "sustainabilityRating",
        "morningstarRiskRating"
    ]
    
    all_funds_data = []
    
    # print("Beginning multi-term search. Please wait...\n")
    
    for term in tqdm(top_issuers, unit="issuer"):
        # print(f"Scraping top-rated ETFs for: {term}...")
        
        try:
            # We search for each term one by one
            results = mstarpy.screener_universe(
                term=term, 
                field=fields, 
                pageSize=300, # Grabs up to 100 top funds per issuer
                filters={
                    "fundStarRating": (">=", 4), # Only grabbing the best 4 & 5 star funds
                    "exchange": ["ARCX", "XNAS", "XNYS", "BATS"] # Only grabbing ETFs listed on major US exchanges
                } 
            )
            
            if results:
                # Flattening the nested data
                for item in results:
                    row = {}
                    row["Ticker"] = item.get("ticker") or item.get("meta", {}).get("ticker", "N/A")
                    row["Issuer"] = term # Adding a custom column so you know which search found it
                    
                    raw_fields = item.get("fields", {})
                    for col_name, col_data in raw_fields.items():
                        if isinstance(col_data, dict):
                            row[col_name] = col_data.get("value")
                        else:
                            row[col_name] = col_data
                            
                    all_funds_data.append(row)
            
            # Pause for 1 second between searches so Morningstar doesn't block us for spamming
            time.sleep(2) 
            
        except Exception as e:
            print(f"An error occurred while searching for {term}: {e}")

    # --- FORMATTING THE MASTER TABLE ---
    if not all_funds_data:
        print("No data found across any terms.")
        return None

    df = pd.DataFrame(all_funds_data)
    
    df = df.rename(columns={
        "name": "Name",
        "fundStarRating": "Stars",
        "medalistRating": "Medal",
        "sustainabilityRating": "ESG Globes",
        "morningstarRiskRating": "Risk"
    })
    
    # Organize columns nicely
    cols = ["Issuer", "Ticker", "Name", "Stars", "Risk", "Medal", "ESG Globes"]
    df = df[[c for c in cols if c in df.columns]]
    
    # Drop any duplicates just in case two issuers share a weird fund name
    df = df.drop_duplicates(subset=["Ticker"])
    # df = df[df["Medal"].isin(["Neutral", "Bronze", "Silver", "Gold"])] 
    # df = df[df["ESG Globes"].isin([3., 4., 5.])] 
    # df = df[df["Risk"].isin(['Average','Below Average','Low'])]
    
    return df

# Run the master script
master_df = get_major_etfs()
# print("\nData fetching complete! A total of {} ETFs found.".format(len(master_df)))

master_df = master_df[~master_df["Ticker"].str.contains(r'\d', na=False, regex=True)]

def calculate_flow(data, num_rows):
    """A helper function to do the Buy/Sell math on any slice of data."""
    if data.empty or len(data) < 2:
        return None  # We use None so Pandas recognizes it as missing data (NaN)
        
    # Take only the exact number of rows we want (e.g., last 10 bars)
    df_slice = data.tail(num_rows).copy()
    
    # Calculate price change
    df_slice['Price_Change'] = df_slice['Close'].diff()
    
    # Sum up the volumes
    buy_vol = df_slice[df_slice['Price_Change'] > 0]['Volume'].sum()
    sell_vol = df_slice[df_slice['Price_Change'] < 0]['Volume'].sum()
    total_vol = buy_vol + sell_vol

    if total_vol == 0:
        return 50.0  # Dead even
        
    return round((buy_vol / total_vol) * 100, 1)


def get_multi_timeframe_pct(df):
    print("\nSnapping multi-timeframe volume flows...")
    
    # Lists to hold our 4 new columns
    flow_10m = []
    flow_30m = []
    flow_1h = []
    flow_2h = []
    flow_6h = []
    flow_12h = []

    for ticker in tqdm(df['Ticker'], desc="Fetching Tickers", unit="ticker"):
        try:
            # We create a base ticker object to pull from
            tkr = yf.Ticker(ticker)
            
            # 1. Last 10 mins
            d_1m = tkr.history(period="1d", interval="1m")
            flow_10m.append(calculate_flow(d_1m, 10))
            
            # 2. Last 30 mins
            flow_30m.append(calculate_flow(d_1m, 30))

            # 3. Last 1 hour
            flow_1h.append(calculate_flow(d_1m, 60))

            # 4. Last 2 hours
            flow_2h.append(calculate_flow(d_1m, 120))

            # 5. Last 6 hours
            flow_6h.append(calculate_flow(d_1m, 360))

            # 6. Last 12 hours
            flow_12h.append(calculate_flow(d_1m, 720))
            

            
        except Exception:
            # If the ticker completely fails, fill with None across the board
            flow_10m.append(None)
            flow_30m.append(None)
            flow_1h.append(None)
            flow_2h.append(None)
            flow_6h.append(None)
            flow_12h.append(None)

    # Append all 4 lists as new columns in your master dataframe
    df['12h Flow'] = flow_12h
    df['6h Flow'] = flow_6h
    df['2h Flow'] = flow_2h
    df['1h Flow'] = flow_1h
    df['30m Flow'] = flow_30m
    df['10m Flow'] = flow_10m


    
    return df

# --- Usage ---
master_df = get_multi_timeframe_pct(master_df)

# Print it! (You might want to export to CSV to see it clearly, as it is very wide now)
# print(master_df.to_string(index=False))

def drop_na_and_convert_pct(col,df):
    df = df[df[col] != "N/A"]
    # df[col] = pd.to_numeric(
    #     df[col].str.replace('%', '').str.replace(' Buy', ''), 
    #     errors='coerce'
    # )

for col in ['10m Flow', '30m Flow', '1h Flow', '2h Flow', '6h Flow', '12h Flow']:
    drop_na_and_convert_pct(col, master_df)

# 1. Teach Pandas the correct order for Medals (Best to Worst)
medal_hierarchy = CategoricalDtype(
    categories=["Gold", "Silver", "Bronze", "Neutral", "Negative"], 
    ordered=True
)

# 2. Teach Pandas the correct order for Risk (Safest to Riskiest)
risk_hierarchy = CategoricalDtype(
    categories=["Low", "Below Average", "Average", "Above Average", "High"], 
    ordered=True
)

# 3. Apply these new rules to your dataframe's columns
master_df["Medal"] = master_df["Medal"].astype(medal_hierarchy)
master_df["Risk"] = master_df["Risk"].astype(risk_hierarchy)

# 4. Now perform the master sort!
# We use a list of True/False to tell Pandas exactly which direction to sort each column.
master_df = master_df.sort_values(
    by=['12h Flow', '6h Flow', '2h Flow', '1h Flow', '30m Flow', '10m Flow', "Stars", "Risk", "Medal",  "ESG Globes"],
    ascending=[
        False,
        False,
        False,
        False,
        False,
        False,
        False, # Stars: False = 5 to 1 (Best first)
        True,  # Risk: True = Follows our Safest-to-Riskiest list above
        True,  # Medal: True = Follows our custom Best-to-Worst list above
        False  # ESG Globes: False = 5 to 1 (Best first)
    ],
    na_position='last' # Pushes any funds with missing data (NaN) to the very bottom
)

master_df = master_df.dropna(subset=['12h Flow'])

# Drop rows where Risk is 'Above Average' or 'High'
master_df = master_df[~master_df['Risk'].isin(['Above Average', 'High'])]

# Drop rows where Medal is 'Negative'
master_df = master_df[master_df['Medal'] != 'Negative']

# Drop rows where ESG Globes is 1
master_df = master_df[master_df['ESG Globes'] != 1]

master_df = master_df.reset_index(drop=True)
master_df.index = master_df.index + 1

# Print the beautifully sorted dataset!
# print(master_df.to_string(index=True))

master_df.to_csv('etf_data.csv', index=False)