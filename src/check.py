import pandas as pd

fp = r"data/raw/EPD_SNOMED_202511.csv"  # change to your actual filename
df = pd.read_csv(fp, nrows=200_000, dtype=str)

vals = (df["BNF_CHAPTER_PLUS_CODE"]
        .dropna()
        .astype(str)
        .str.strip()
        .value_counts()
        .head(50))

print(vals)
