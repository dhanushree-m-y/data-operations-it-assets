import pandas as pd

# ---------------------- Step 1: Load CSV ----------------------
input_file = "it_asset_inventory_cleaned.csv"
output_file = "it_asset_inventory_cleaned2.csv" 

df = pd.read_csv(input_file)

# ---------------------- Step 2: Remove Duplicates ----------------------
if 'hostname' in df.columns:
    df = df.drop_duplicates(subset=['hostname'], keep='first')

# ---------------------- Step 3: Trim Extra Spaces ----------------------
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# ---------------------- Step 4: Replace Blanks or NaN with 'unknown' ----------------------
df = df.fillna("unknown")

# Replace empty strings ("") or spaces-only cells with "unknown"
df = df.replace(r'^\s*$', 'unknown', regex=True)

# ---------------------- Step 5: Convert All Text to Lowercase ----------------------
df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)

# ---------------------- Step 6: Standardize Date Format ----------------------
date_col = "operating_system_installation_date"
if date_col in df.columns:
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
    df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")
    df[date_col] = df[date_col].fillna("unknown")

# ---------------------- Step 7: Save Cleaned File ----------------------
df.to_csv(output_file, index=False)
print(f"Cleaned data saved as '{output_file}'")
