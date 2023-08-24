from argparse import ArgumentParser
from datalad_tabby.io.xlsx import xlsx2tabby
from pathlib import Path


def get_prefix_sheet(fpath):
    if "_" in fpath.stem:
        prefix, sheet = fpath.stem.split("_", maxsplit=1)
    else:
        prefix = ""
        sheet = fpath.stem
    return prefix, sheet


def get_dirpath_equivalent(fpath):
    """Translate prefixed path into file-in-directory"""
    prefix, sheet = get_prefix_sheet(fpath)
    if prefix == "":
        return fpath
    return fpath.parent / prefix / (sheet + fpath.suffix)


def affix_convention(fpath):
    """Add convention to file path, based on our knowledge"""
    conventions = {
        "dataset": "@tby-crc1451v0",
        "funding": "@tby-crc1451v0",
        "publications": "@tby-crc1451v0",
        "data-controller": "@tby-crc1451v0",
        "used-for": "@tby-crc1451v0",
        "authors": "@tby-crc1451v0",
        "files": "@tby-ds1",
    }

    _, sheet = get_prefix_sheet(fpath)
    convention = conventions.get(sheet, "")
    return fpath.parent / f"{fpath.stem}{convention}{fpath.suffix}"


parser = ArgumentParser()
parser.add_argument("xslx_file", type=Path, help="source xlsx file")
parser.add_argument("dest_dir", type=Path, help="directory to deposit tabby files")
args = parser.parse_args()

res = xlsx2tabby(
    src=args.xslx_file,
    dest=args.dest_dir,
)

for fpath in res:
    newpath = affix_convention(get_dirpath_equivalent(fpath))
    print(newpath)
    if newpath != fpath:
        if not newpath.parent.is_dir():
            newpath.parent.mkdir()
        fpath.rename(newpath)
