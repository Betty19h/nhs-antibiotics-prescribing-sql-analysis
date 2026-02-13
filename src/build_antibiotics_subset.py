from pathlib import Path
import pandas as pd
from tqdm import tqdm
import csv

raw_folder = Path("data/raw")
out_file = Path("data/processed/epd_antibiotics_3m.csv")

cols_to_keep = [
    "YEAR_MONTH",
    "REGIONAL_OFFICE_NAME", "REGIONAL_OFFICE_CODE",
    "ICB_NAME", "ICB_CODE",
    "PCO_NAME", "PCO_CODE",
    "PRACTICE_NAME", "PRACTICE_CODE",
    "POSTCODE",
    "BNF_CHEMICAL_SUBSTANCE",
    "BNF_PRESENTATION_NAME",
    "BNF_CHAPTER_PLUS_CODE",
    "QUANTITY", "ITEMS", "NIC", "ACTUAL_COST",
    "SNOMED_CODE"
]


def clean_cols(cols):
    return [c.strip().upper() for c in cols]


def is_antibiotic_row(row):
    # first restrict to infections chapter
    chapter = str(row.get("BNF_CHAPTER_PLUS_CODE", ""))
    if not chapter.startswith("05"):
        return False

    # then check drug name looks like an antibacterial
    name = (
        str(row.get("BNF_PRESENTATION_NAME", "")) + " " +
        str(row.get("BNF_CHEMICAL_SUBSTANCE", ""))
    ).lower()

    antibiotic_words = [
        "cillin", "cycline", "mycin", "floxacin",
        "cef", "ceph", "penem",
        "azith", "clarith",
        "trimethoprim", "metronidazole"
    ]

    return any(w in name for w in antibiotic_words)


def main():
    out_file.parent.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(raw_folder.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)[:3]

    wrote_header = False

    for fp in csv_files:
        print(f"\nProcessing {fp.name}")

        header = pd.read_csv(fp, nrows=0)
        header.columns = clean_cols(header.columns)

        keep = [c for c in cols_to_keep if c in header.columns]
        print("Keeping:", keep)

        for chunk in tqdm(pd.read_csv(fp, chunksize=200_000, dtype=str), desc=fp.name):
            chunk.columns = clean_cols(chunk.columns)
            chunk = chunk[keep]

            chunk = chunk[chunk.apply(is_antibiotic_row, axis=1)]

            if chunk.empty:
                continue

            chunk.to_csv(
                out_file,
                mode="a",
                index=False,
                header=not wrote_header,
                quoting=csv.QUOTE_MINIMAL
            )
            wrote_header = True

    print("\nFinished — antibiotics dataset saved to:")
    print(out_file)


if __name__ == "__main__":
    main()
