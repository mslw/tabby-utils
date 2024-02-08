import argparse
import csv
from pathlib import Path

from datalad.api import ls_file_collection


def transform_result(res):
    """Transform status result to get required information

    Transformed result contais relative path, size in bytes, and
    optionally md5sum. Keys match the sfb1451 tabby specification.

    """
    transformed = {
        "path[POSIX]": res["item"].relative_to(res["collection"]).as_posix(),
        "size[bytes]": res["annexsize"]
        if res["annexsize"] is not None
        else res["size"],
        "checksum[md5]": res.get("hash-md5"),  # won't be computed for annexed files
    }
    if res["type"] == "annexed file" and res["annexkey"].startswith("MD5"):
        # report MD5 checksum if contained in annex key
        # https://git-annex.branchable.com/internals/key_format/
        transformed["checksum[md5]"] = res["annexkey"].rsplit("--")[-1].split(".")[0]

    return transformed


def is_file(res):
    """Check if result type is file"""
    return res["type"] in ("file", "annexed file")


parser = argparse.ArgumentParser()
parser.add_argument("dataset", type=Path, help="Dataset for which files will be listed")
parser.add_argument("collection_type", help="Type of file collection")
parser.add_argument("outfile", type=Path, help="Name of tsv file to write")
args = parser.parse_args()

results = ls_file_collection(
    type=args.collection_type,
    collection=args.dataset,
    hash="md5",
    result_renderer="disabled",
    result_filter=is_file,
    result_xfm=transform_result,
    return_type="generator",
)

with args.outfile.open("w", encoding="utf-8", newline="") as csvfile:
    fieldnames = ["path[POSIX]", "size[bytes]", "checksum[md5]"]
    writer = csv.DictWriter(csvfile, delimiter="\t", fieldnames=fieldnames)
    writer.writeheader()
    for res in results:
        writer.writerow(res)
