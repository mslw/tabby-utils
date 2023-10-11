from argparse import ArgumentParser
from pathlib import Path
from pprint import pprint

from datalad_catalog.schema_utils import get_metadata_item
from datalad_next.datasets import Dataset
from datalad_tabby.io import load_tabby

from utils import mint_dataset_id

parser = ArgumentParser()
parser.add_argument("ds_path", type=Path)
parser.add_argument("--tabby-anywhere", action="store_true")
args = parser.parse_args()


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


ds = Dataset(args.ds_path)

dataset_item = get_metadata_item(
    item_type="dataset",
    dataset_id=ds.id,
    dataset_version=ds.repo.get_hexsha(),
    source_name="tabby",
    source_version="0.1.0",
)

subdatasets = []
for tabby_path in list_tabby_ds_files(ds, anywhere=args.tabby_anywhere):
    subdatasets.append(subdataset_item(ds, tabby_path))

dataset_item["subdatasets"] = subdatasets

pprint(dataset_item)
