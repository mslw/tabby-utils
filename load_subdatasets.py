from argparse import ArgumentParser
from pathlib import Path
from pprint import pprint

from datalad.api import catalog_add
from datalad_catalog.schema_utils import get_metadata_item
from datalad_next.datasets import Dataset
from datalad_tabby.io import load_tabby

from utils import mint_dataset_id


def get_tabby_subdataset_path(tabby_file_path, ds_root_path):
    """Get path of subdataset described by tabby

    Note: this is currently tuned to a single dir layout, and reports
    tabby file's parent dir as the location of described subdataset.
    If the tabby collection is located in .datalad/tabby, reports
    relative to that directory instead.

    """
    relpath = tabby_file_path.parent.relative_to(ds_root_path)
    if relpath.match(".datalad/tabby/*"):
        return relpath.relative_to(".datalad/tabby/")
    return relpath


def list_tabby_ds_files(ds, anywhere=False):
    """List dataset*.tsv tabby files

    By default, used glob to report .datalad/tabby contents. If
    searching anywhere is requested, uses ls-tree instead.

    """
    if not anywhere:
        return list(ds.pathobj.glob(".datalad/tabby/**/*dataset*.tsv"))
    else:
        return [
            ds.pathobj.joinpath(p)
            for p in ds.repo.call_git_items_(["ls-files", "*dataset@tby*.tsv"])
        ]


def subdataset_item(ds, tabby_path):
    """Report subdataset path, id, version"""

    # path is derived from tabby location
    ds_path = get_tabby_subdataset_path(tabby_path, ds.pathobj)

    # id & version are derived from tabby content
    record = load_tabby(
        tabby_path,
        cpaths=[Path(__file__).parent / "conventions"],
    )

    ds_id = mint_dataset_id(
        ds_name=record["name"],
        project=record["crc-project"],
    )
    ds_version = record["version"]

    return {"dataset_path": ds_path, "dataset_id": ds_id, "version": ds_version}


parser = ArgumentParser()
parser.add_argument("ds_path", type=Path)
parser.add_argument("--catalog", type=Path, help="Catalog to add to")
parser.add_argument("--tabby-anywhere", action="store_true", help="Search outside .datalad/tabby")
args = parser.parse_args()

# Search the dataset and create subdataset metadata dicts
ds = Dataset(args.ds_path)

subdatasets = []
for tabby_path in list_tabby_ds_files(ds, anywhere=args.tabby_anywhere):
    subdatasets.append(subdataset_item(ds, tabby_path))

# Early exit if nothing to do
if len(subdatasets) == 0:
    print("No subdatasets found")
    exit()

# Create a catalog metadata item and print it
dataset_item = get_metadata_item(
    item_type="dataset",
    dataset_id=ds.id,
    dataset_version=ds.repo.get_hexsha(),
    source_name="tabby",
    source_version="0.1.0",
)
dataset_item["subdatasets"] = subdatasets
pprint(dataset_item)

# Add to catalog if requested
if args.catalog is not None:
    catalog_add(
        catalog=catalog_dir,
        metadata=json.dumps(dataset_item),
        config_file=catalog_dir / "config.json",
    )
