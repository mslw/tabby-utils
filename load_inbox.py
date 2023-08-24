from argparse import ArgumentParser
from datalad_tabby.io.xlsx import xlsx2tabby
from pathlib import Path
from shutil import copy


def get_prefix_sheet(fpath):
    prefix, _, sheet = fpath.stem.rpartition("_")
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
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument("-x", "--xlsx_file", type=Path, help="source xlsx file")
grp.add_argument("-t", "--tsv_files", type=Path, nargs="*", help="source tsv files")
parser.add_argument("-d", "--dest_dir", type=Path, required=True, help="directory to deposit tabby files")
args = parser.parse_args()

if args.xlsx_file is not None:
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


if args.tsv_files is not None:
    for fpath in args.tsv_files:
        # hypothetical dirpath with convention
        newpath_here = affix_convention(get_dirpath_equivalent(fpath))
        # what we actually want
        newpath_there = args.dest_dir / newpath_here.parent.name / newpath_here.name
        print(newpath_there)
        if not newpath_there.parent.is_dir():
            newpath_there.parent.mkdir()
        copy(fpath, newpath_there)

