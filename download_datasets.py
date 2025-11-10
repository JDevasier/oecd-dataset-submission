import requests
import time
import os
import xml.etree.ElementTree as ET
from slugify import slugify


DATAFLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/all"
DATA_URL_TEMPLATE = "https://sdmx.oecd.org/public/rest/data/{agencyID},{id},{version}?dimensionAtObservation=AllDimensions&format=csvfilewithlabels"

OUTPUT_DIR = "./datasets/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_datasets():
    resp = requests.get(DATAFLOW_URL)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    ns = {
        "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
        "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"
    }
    datasets = []
    for df in root.findall(".//structure:Dataflow", ns):
        id_ = df.attrib.get("id")
        agencyID = df.attrib.get("agencyID")
        version = df.attrib.get("version")
        if id_ and agencyID and version:
            datasets.append({"id": id_, "agencyID": agencyID, "version": version})
    return datasets

def download_dataset(ds):
    url = DATA_URL_TEMPLATE.format(**ds)
    print(f"Downloading dataset {ds['id']} from {url}")

    out_path = os.path.join(OUTPUT_DIR, slugify(ds['id'], separator="_")+".csv")
    # Check if the file already exists
    if os.path.exists(out_path):
        print(f"File {out_path} already exists. Skipping download.")
        return True

    print(f"Downloading {ds['id']} ...")
    resp = requests.get(url)
    if resp.status_code == 429:
        print(f"Rate limit exceeded for {ds['id']}. Retrying after 180 seconds...")
        time.sleep(180)
        return download_dataset(ds)

    if resp.status_code == 200 and resp.text.strip():
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        print(f"Saved {ds['id']} to {out_path}")
    else:
        print(f"Failed to download {ds['id']} (status {resp.status_code})")
        return False
    
    return True

def main():
    datasets = get_datasets()
    print(f"Found {len(datasets)} datasets.")
    for ds in datasets:
        try:
            out_path = os.path.join(OUTPUT_DIR, slugify(ds['id'], separator="_")+".csv")
            if os.path.exists(out_path):
                print(f"File {out_path} already exists. Skipping download.")
                continue
            
            success = download_dataset(ds)

            if success:
                time.sleep(180)
        except Exception as e:
            print(f"Error downloading {ds['id']}: {e}")

if __name__ == "__main__":
    main()
