#! /usr/bin/env python3

import re
import os
import argparse
import logging
import hashlib
from itertools import repeat
from datetime import datetime as dt
from pathlib import Path
from multiprocessing import Pool, Manager

try:
    from exif import Image
except ImportError:
    raise ImportError("You need to install 'exif' (pip install exif)")

logger = logging.getLogger("sort-images")

# should be 3 or lower, as some cameras add 4 digit serial numbers to images
# like IMG_3892 and may some exist which use - instead _
max_conflict_digits = 3


parser = argparse.ArgumentParser()

parser.add_argument(
    "-n",
    "--dry-run",
    action="store_true",
    help="Show only actions without performing any changes"
)

parser.add_argument(
    "--prune",
    action="store_true",
    help="Remove non-images"
)

parser.add_argument(
    "--conflict",
    choices=['counter', 'hash', 'ignore'],
    default="count", help="How to solve conflicts?"
)

parser.add_argument(
    "--replace",
    action="store_true",
    help="Replace existing images even they are no duplicates"
)

parser.add_argument(
    "--pattern",
    help="Pattern for images",
    default=(
        "{creation:%Y}/{creation:%m}/{creation:%Y-%m-%d_%H:%M:%S}{suffix}"
    )
)


parser.add_argument("src", nargs="*")
parser.add_argument("dest")

extract_conflict_pattern = re.compile(
    "^(?P<unrelated>.*?)"
    "(?:-(?P<conflict_hash>[a-f0-9]{16}))?"
    "(?:-(?P<conflict_counter>[0-9]{1,%s}))?$" % max_conflict_digits
)

extraction_pattern = re.compile(
    "^(?P<prefix>.*?)"
    "(?P<year>[1-3][0-9]{3})-?(?P<month>[0-9]{2})-?(?P<day>[0-9]{2})"
    "(?:[-_ ]?(?P<hour>[0-9]{2})[:.]?(?P<minute>[0-9]{2})"
    "[:.]?(?P<second>[0-9]{2})?"
    ")?(?P<suffix>.*?)$"
)

img_suffixes = {
    ".png", ".tiff", ".jpg", ".jpeg"
}
mov_suffixes = {
    ".mp4", ".webm", ".ogg", ".ogv", ".mov"
}

media_suffixes = img_suffixes | mov_suffixes


def generate_hash(path):
    file_hash = hashlib.blake2b(digest_size=8)
    with path.open(mode='rb') as f:
        buffer = f.read(512000)
        while buffer != b"":
            file_hash.update(buffer)
            buffer = f.read(512000)
    return file_hash.hexdigest()


def generate_new_name(
    argob, path, file_info, conflict=0
):
    replacements = {
        "creation": file_info["creation"],
        "prefix": file_info["file_prefix"],
        "suffix": file_info["file_suffix"],
        "type": file_info["file_content_type"],
        "hash": file_info["file_hash"]
    }
    conflictstr = ""
    if conflict > 0 and argob.conflict == "counter":
        conflictstr = "-%s" % conflict
    elif conflict > 1 and argob.conflict == "hash":
        conflictstr = "-%s-%s" % (file_info["file_hash"], conflict-1)
    elif conflict > 0 and argob.conflict == "hash":
        conflictstr = "-%s" % file_info["file_hash"]

    replacestr = argob.pattern.format(**replacements)
    replace_base, replace_name = replacestr.rsplit("/", 1)
    if file_info["dtpattern_in_path"]:
        new_name = extraction_pattern.sub(replace_name, path.stem, count=1)
        new_name = "{}{}{}".format(new_name, conflictstr, path.suffix.lower())
        newpath = Path(argob.dest, replace_base, new_name)
    else:
        newpath = Path(
            argob.dest, replace_base,
            "{}_{}{}{}".format(
                replace_name,
                file_info["cleaned_file_name"],
                conflictstr,
                path.suffix.lower()
            )
        )
    return newpath


def rename_file(argob, path, file_info):
    conflict_count = file_info["conflict_count"]
    canreplace = False
    duplicate = False
    max_conflicts = 10 * max_conflict_digits
    while True:
        if conflict_count >= max_conflicts:
            logger.critical(
                "Conflict counter >= %s", max_conflicts
            )
            return False
        newpath = generate_new_name(
            argob, path, file_info, conflict=conflict_count
        )
        if newpath == path:
            return False
        else:
            _strnewpath = str(newpath)
            conflict = False
            # collides with a not processed file
            # we need no lock here, files are static
            if _strnewpath in argob.sharedns.maybe_unprocessed_files:
                conflict = True
            else:
                with argob.sharedns_lock:
                    if _strnewpath in argob.sharedns.collisions:
                        conflict = True
                    elif newpath.exists():
                        conflict = True
                        # found old file, not in set
                        if argob.replace:
                            canreplace = True
                            argob.sharedns.collisions[_strnewpath] = []
                    else:
                        argob.sharedns.collisions[_strnewpath] = []
            if not conflict or canreplace:
                break
            # conflict with previously existing file
            oldhash = generate_hash(newpath)
            if oldhash in {
                file_info["file_hash"], file_info["old_file_hash"]
            }:
                duplicate = True
                break
            elif argob.conflict == "ignore":
                with argob.sharedns_lock:
                    argob.sharedns.existing[_strnewpath].append(str(path))

    if argob.dry_run:
        if canreplace:
            logger.info("Would replace: %s with %s", newpath, path)
        else:
            logger.info("Would rename: %s to %s", path, newpath)
    else:
        if duplicate:
            path.unlink()
        else:
            newpath.parent.mkdir(mode=0o770, parents=True, exist_ok=True)
            path.rename(newpath)
    return duplicate


def processFile(args):
    argob, path = args
    path = Path(path)
    file_info = {
        "creation": None,
        "file_prefix": "",
        "file_suffix": "",
        "file_content_type": "",
        "old_conflict": "",
        "dtpattern_in_path": False,
        "conflict_count": 0,
        "cleaned_file_name": ""
    }
    creation = None
    image_exif = None
    image_exif_date_error = False
    lower_suffix = path.suffix.lower()
    if lower_suffix in img_suffixes:
        file_info["file_content_type"] = "IMG"
        try:
            with path.open(mode='rb') as f:
                image_exif = Image(f)
        except Exception as exc:
            logger.debug("Exception while reading exif: %s" % exc)
        if not image_exif or not image_exif.has_exif:
            logger.debug("image has no exif data/is not compatible: %s", path)
            image_exif = None
        elif hasattr(image_exif, "datetime"):
            try:
                creation = \
                    dt.strptime(image_exif.datetime, "%Y:%m:%d %H:%M:%S")
            except ValueError:
                logger.warning("Invalid format: %s", image_exif.datetime)
                image_exif_date_error = True
        elif hasattr(image_exif, "datetime_original"):
            try:
                creation = \
                    dt.strptime(
                        image_exif.datetime_original, "%Y:%m:%d %H:%M:%S"
                    )
            except ValueError:
                logger.warning(
                    "Invalid format: %s",
                    image_exif.datetime_original
                )
                image_exif_date_error = True
    elif lower_suffix in mov_suffixes:
        file_info["file_content_type"] = "MOV"
    else:
        if not argob.prune:
            logger.info("unrecognized file: %s", path)
        elif argob.dry_run:
            logger.info("Would remove: %s (unrecognized)", path)
        else:
            path.unlink()
        return 1
    if argob.conflict == "ignore":
        file_info["cleaned_file_name"] = path.stem
    else:
        # find conflict counter and hash
        conflictmatch = extract_conflict_pattern.match(path.stem).groupdict()
        file_info["cleaned_file_name"] = conflictmatch["unrelated"]
        if conflictmatch["conflict_hash"]:
            if argob.conflict == "hash":
                # if hash is found, one conflict is indicated
                file_info["conflict_count"] = 1
            else:
                file_info["cleaned_file_name"] = "%s-%s" % (
                    file_info["cleaned_file_name"],
                    conflictmatch["conflict_hash"]
                )
        # if hash and counter: conflict_count = counter + 1
        # if counter: conflict_count  = counter
        # if hash: conflict_count  = 1
        file_info["conflict_count"] += \
            int(conflictmatch["conflict_counter"] or 0)
    # check and potential extract datetime info from filename
    dtnamematch = extraction_pattern.match(file_info["cleaned_file_name"])
    if dtnamematch:
        dtnamematchg = dtnamematch.groupdict()
        file_info["file_prefix"] = dtnamematchg["prefix"] or ""
        file_info["file_suffix"] = dtnamematchg["suffix"] or ""

        file_info["dtpattern_in_path"] = True

        if not creation:
            logger.debug("extract time from path: %s", path)
            creation = dt(
                year=int(dtnamematchg["year"]),
                month=int(dtnamematchg["month"]),
                day=int(dtnamematchg["day"]),
                hour=int(dtnamematchg["hour"] or 0),
                minute=int(dtnamematchg["minute"] or 0),
                second=int(dtnamematchg["second"] or 0)
            )
    # still no creation time
    if not creation:
        logger.debug("extract time from st_ctime: %s", path)
        creation = dt.fromtimestamp(path.stat().st_ctime)
    file_info["creation"] = creation

    if image_exif_date_error:
        file_info["old_file_hash"] = generate_hash(path)
        if argob.dry_run:
            logger.info(
                "Would fix: %s to %s, of %s",
                image_exif.datetime,
                creation.strftime("%Y:%m:%d %H:%M:%S"),
                path
            )
        else:
            image_exif.datetime = creation.strftime("%Y:%m:%d %H:%M:%S")
            image_exif.datetime_original = image_exif.datetime
            with path.open(mode='wb') as f:
                f.write(image_exif.get_file())

    file_info["file_hash"] = generate_hash(path)
    if rename_file(argob, path, file_info):
        return 1
    return 0


def sortFiles(argob):
    argob.dest = Path(argob.dest)
    if not argob.src:
        argob.src = [Path(argob.dest)]
    else:
        argob.src = [
            Path(src) for src in argob.src
        ]

    old_pruned_files = 0

    # find all files in dest and cleanup if pruning
    if argob.prune:
        for file in argob.dest.rglob("*"):
            # prune also symlinks to files
            if not file.is_file():
                continue
            if file.suffix.lower() not in media_suffixes:
                old_pruned_files += 1
                if argob.dry_run:
                    logger.info("Would remove: %s (unrecognized)", file)
                else:
                    file.unlink()
    src_dest_overlap = False
    files = set()
    # find relevant files, elsewise it is too risky when dest and src overlap
    for src in argob.src:
        if (
            argob.dest == src
            or argob.dest in src.parents
            or src in argob.dest.parents
        ):
            src_dest_overlap = True
        # only list non hidden files in src
        for file in src.rglob("[!.]*"):
            # only move real files, ignore symlinks
            if not file.is_file() or file.is_symlink():
                continue
            files.add(str(file))
    with Manager() as manager:
        argob.sharedns_lock = manager.Lock()
        argob.sharedns = manager.Namespace()
        argob.sharedns.collisions = {}
        if src_dest_overlap:
            argob.sharedns.maybe_unprocessed_files = files
        else:
            argob.sharedns.maybe_unprocessed_files = set()

        with Pool() as pool:
            pruned_and_deduplicated_files = sum(pool.imap_unordered(
                processFile,
                zip(repeat(argob), files),
                chunksize=8
            ))
            pool.close()
            pool.join()
        logger.info(
            "Processed unique images and videos: %s",
            len(files) - pruned_and_deduplicated_files
        )
        pruned_files = old_pruned_files + pruned_and_deduplicated_files
        if argob.prune:
            if argob.dry_run:
                logger.info("Would prune files: %i", pruned_files)
            else:
                logger.info("Pruned files: %i", pruned_files)
        unsolved_conflicts = dict(filter(
            lambda x: len(x[1]) > 0, argob.sharedns.collisions
        ))
        if unsolved_conflicts:
            logger.info("files with conflicts:")
            for newname, oldnames in unsolved_conflicts.items():
                logger.info("%s -> %s", oldnames, newname)


def main(argv=None):
    logging.basicConfig(
        level=(
            logging.DEBUG
            if os.environ.get("DEBUG") == "true"
            else logging.INFO
        )
    )
    sortFiles(parser.parse_args(argv))


if __name__ == "__main__":
    main()
