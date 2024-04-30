"""Create project-tags to be added to the catalog

Do this once with tags alone, to avoid re-extracting all datasets.
TODO: refactor and include in load_tabby.

"""

from argparse import ArgumentParser
import json
from pathlib import Path

from datalad_catalog.schema_utils import get_metadata_item
from datalad_tabby.io import load_tabby

from utils import mint_dataset_id

parser = ArgumentParser()
parser.add_argument("superds", type=Path, help="Superdataset location")
parser.add_argument("outfile", type=Path, help="Output metadata file")
args = parser.parse_args()

metadata_items = []
tabby_files = (args.superds / ".datalad" / "tabby").rglob("dataset*tsv")
for tabby in tabby_files:
    # todo: handle encoding
    record = load_tabby(
        tabby,
        cpaths=[Path(__file__).parent / "conventions"],
    )

    # dataset ID and version
    dataset_id = mint_dataset_id(record.get("name"), record.get("crc-project"))
    dataset_version = record.get("version")

    # project names and keywords
    if isinstance(record["crc-project"], str):
        projects = [record["crc-project"]]
    else:
        projects = record["crc-project"]
    keywords = record.get("keywords", [])
    new_keywords = []

    for project in projects:
        if project.upper() not in keywords:  # case sensitive is ok
            new_keywords.append(project.upper())

    if len(new_keywords) == 0:
        print("Nothing to add for", tabby)

    metadata_items.append(
        get_metadata_item(
            item_type="dataset",
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            source_name="manual_addition",
            source_version="0.1.0",
        )
        | {"keywords": new_keywords}
    )

with args.outfile.open("w") as json_file:
    for item in metadata_items:
        json.dump(item, json_file)
        json_file.write("\n")
