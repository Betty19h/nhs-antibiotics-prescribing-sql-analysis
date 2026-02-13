# Download the latest 3 months of NHSBSA EPD + SNOMED prescribing data
# The dataset is on the NHSBSA open data portal (CKAN).
# We:
# 1) ask the portal for the dataset file list
# 2) sort files by date (newest first)
# 3) pick the newest 3 (prefer ZIP if available)
# 4) download them into data/raw
# 5) save a small manifest so we know what we downloaded

from pathlib import Path
import json

import requests
from dateutil.parser import isoparse
from tqdm import tqdm

dataset_id = "english-prescribing-dataset-epd-with-snomed-code"
base_url = "https://opendata.nhsbsa.net"
package_show_url = f"{base_url}/api/3/action/package_show"


def get_file_list():
    # get the dataset metadata (including all downloadable files)
    r = requests.get(package_show_url, params={"id": dataset_id}, timeout=60)
    r.raise_for_status()

    data = r.json()
    if not data.get("success"):
        raise RuntimeError("The portal API returned success=false")

    return data["result"]["resources"]


def get_resource_date(resource):
    # figure out the best date field we can use for sorting newest -> oldest
    for key in ("created", "last_modified", "metadata_modified"):
        if resource.get(key):
            return isoparse(resource[key])
    return isoparse("1970-01-01T00:00:00Z")


def pick_latest(resources, n=3):
    # sort newest first
    resources = sorted(resources, key=get_resource_date, reverse=True)

    chosen = []

    # first try: grab ZIP files (usually smaller and quicker)
    for res in resources:
        name = (res.get("name") or "").lower()
        fmt = (res.get("format") or "").lower()

        # keep only resources that look like the EPD SNOMED monthly files
        if "epd" not in name or "snomed" not in name:
            continue

        if fmt == "zip":
            chosen.append(res)

        if len(chosen) >= n:
            return chosen

    # fallback: if we didn't get enough ZIP files, take other formats
    for res in resources:
        name = (res.get("name") or "").lower()

        if "epd" not in name or "snomed" not in name:
            continue

        if res not in chosen:
            chosen.append(res)

        if len(chosen) >= n:
            return chosen

    return chosen


def download(url, save_as):
    # download in chunks so it doesn’t try to load a huge file into memory
    save_as.parent.mkdir(parents=True, exist_ok=True)

    # if the file already exists and isn't empty, skip it
    if save_as.exists() and save_as.stat().st_size > 0:
        print(f"Skip (already exists): {save_as.name}")
        return

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))

        with open(save_as, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=save_as.name,
        ) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))


def main(months_to_download=3):
    resources = get_file_list()
    chosen = pick_latest(resources, n=months_to_download)

    raw_folder = Path("data/raw")
    manifest = []

    for res in chosen:
        url = res.get("url")
        if not url:
            continue

        # use the resource name + format for a filename
        name = (res.get("name") or "resource").replace("/", "-")
        fmt = (res.get("format") or "file").lower()

        save_as = raw_folder / f"{name}.{fmt}"
        download(url, save_as)

        manifest.append(
            {
                "name": res.get("name"),
                "format": res.get("format"),
                "created": res.get("created"),
                "last_modified": res.get("last_modified"),
                "url": url,
                "saved_to": str(save_as),
            }
        )

    # save a record of what we downloaded (useful if something goes wrong later)
    Path("data").mkdir(exist_ok=True)
    Path("data/manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"\nDone. Downloaded {len(manifest)} files.")
    print("Saved manifest to data/manifest.json")


if __name__ == "__main__":
    main(3)
