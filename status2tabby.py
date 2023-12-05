import argparse
import csv
from pathlib import Path, PurePath

from datalad_next.datasets import Dataset


def transform_result(res):
    """Transform status result to get required information

    Transformed result contains relative path, size in bytes, and
    optionally md5sum. Keys match the sfb1451 tabby specification.

    """
    transformed = {
        "path": PurePath(res["path"]).relative_to(res["parentds"]).as_posix(),
        "size[bytes]": res["bytesize"],
    }
    if res.get("key", "").startswith("MD5"):
        # report MD5 checksum if contained in annex key
        # https://git-annex.branchable.com/internals/key_format/
        transformed["checksum[md5]"] = res["key"].rsplit("--")[-1].split(".")[0]

    return transformed


def is_file(res):
    """Check if result type is file"""
    return res["type"] == "file"


parser = argparse.ArgumentParser()
parser.add_argument("dataset", type=Path, help="Dataset for which files will be listed")
parser.add_argument("outfile", type=Path, help="Name of tsv file to write")
args = parser.parse_args()

ds = Dataset(args.dataset)
results = ds.status(
    annex="basic",
    result_renderer="disabled",
    result_filter=is_file,
    result_xfm=transform_result,
    return_type="generator",
)

with args.outfile.open("w", encoding="utf-8", newline="") as csvfile:
    fieldnames = ["path", "size[bytes]", "checksum[md5]"]
    writer = csv.DictWriter(csvfile, delimiter="\t", fieldnames=fieldnames)
    writer.writeheader()
    for res in results:
        writer.writerow(res)
