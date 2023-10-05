"""Create a tabby file list from a git worktree"""
import argparse
import csv
from pathlib import Path
from datalad.api import ls_file_collection

parser = argparse.ArgumentParser()
parser.add_argument("dataset", type=Path)
parser.add_argument("outfile", type=Path)
args = parser.parse_args()

res = ls_file_collection(
    type="gitworktree",
    collection=args.dataset,
    hash="md5",
    result_renderer="disabled",
    return_type="generator",
)


with args.outfile.open("w", newline="") as csvfile:
    writer = csv.writer(csvfile, delimiter="\t")
    writer.writerow(["path[POSIX]", "size[bytes]", "checksum[md5]"])

    dictwriter = csv.DictWriter(
        csvfile,
        fieldnames=["item", "size", "hash-md5"],
        extrasaction="ignore",
        delimiter="\t",
    )

    for item in res:
        if item.get("status") == "ok" and item.get("type") == "file":
            dictwriter.writerow(item)
