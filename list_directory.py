import argparse
import csv
from pathlib import Path
import sys

from datalad.api import ls_file_collection

parser = argparse.ArgumentParser()
parser.add_argument("rootdir", type=Path, help="Top-level directory for which files will be listed")
parser.add_argument("--output", type=Path, help="TSV file to write to")
args = parser.parse_args()


def lsfc(dirpath):
    """Call ls_file_collection recursively on directories.

    Yields files.

    """
    for res in ls_file_collection(
        type="directory",
        collection=dirpath,
        hash="md5",
        result_renderer="disabled",
        return_type="generator",
    ):
        if res["type"] == "file":
            yield res
        elif res["type"] == "directory":
            yield from lsfc(res["item"])

header = ["path[POSIX]", "size[bytes]", "checksum[md5]"]

if args.output is not None:
    # write output to a tsv file
    with args.output.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="\t")
        writer.writerow(header)
        for x in lsfc(args.rootdir):
            writer.writerow(
                [x["item"].relative_to(args.rootdir), x["size"], x.get("hash-md5")]
            )
else:
    # write output to stdout
    writer = csv.writer(sys.stdout, delimiter="\t")
    writer.writerow(header)
    for x in lsfc(args.rootdir):
        writer.writerow(
            [x["item"].relative_to(args.rootdir), x["size"], x.get("hash-md5")]
        )
