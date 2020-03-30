#! /usr/bin/env python3

import re
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
    ".mp4", ".webm", ".ogg", ".ogv"
}

media_suffixes = img_suffixes | mov_suffixes


def processFile(args):
    argob, path = args
    creation = None
    file_content_type = ""
    image_exif = None
    image_exif_error = False
    lower_suffix = path.suffix.lower()
    if lower_suffix in img_suffixes:
        file_content_type = "IMG"
        try:
            with path.open(mode='rb') as f:
                image_exif = Image(f)
        except Exception as exc:
            logger.error(exc)
        if not image_exif:
            if not argob.prune:
                logger.info("non-image with image ending: %s", path)
            elif argob.dry_run:
                logger.info("Would remove: %s", path)
            else:
                path.unlink()
            return 1
        if not image_exif.has_exif:
            logger.debug("image has no exif data: %s", path)
        elif hasattr(image_exif, "datetime"):
            try:
                creation = \
                    dt.strptime(image_exif.datetime, "%Y:%m:%d %H:%M:%S")
            except ValueError:
                logging.warning("Invalid format: %s", image_exif.datetime)
                image_exif_error = True
        elif hasattr(image_exif, "datetime_original"):
            try:
                creation = \
                    dt.strptime(
                        image_exif.datetime_original, "%Y:%m:%d %H:%M:%S"
                    )
            except ValueError:
                logging.warning("Invalid format: %s",
                                image_exif.datetime_original)
                image_exif_error = True
    elif lower_suffix in mov_suffixes:
        file_content_type = "MOV"
    else:
        if not argob.prune:
            logger.info("unrecognized file: %s", path)
        elif argob.dry_run:
            logger.info("Would remove: %s (unrecognized)", path)
        else:
            path.unlink()
        return 1
    # check and potential extract from filename
    dtnamematch = extraction_pattern.match(path.stem)
    file_prefix = ""
    file_suffix = ""
    if dtnamematch:
        dtnamematchg = dtnamematch.groupdict()
        file_prefix = dtnamematchg["prefix"] or ""
        file_suffix = dtnamematchg["suffix"] or ""

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

    if image_exif_error:
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

    file_hash = hashlib.blake2b(digest_size=8)
    with path.open(mode='rb') as f:
        buffer = f.read(512000)
        while buffer != b"":
            file_hash.update(buffer)
            buffer = f.read(512000)
    file_hash = file_hash.hexdigest()

    replacestr = argob.pattern.format(
        creation=creation,
        prefix=file_prefix,
        suffix=file_suffix,
        type=file_content_type,
        hash=file_hash
    )
    replace_base, replace_name = replacestr.rsplit("/", 1)
    if dtnamematch:
        new_name = extraction_pattern.sub(replace_name, path.stem, count=1)
        new_name = "{}{}".format(new_name, path.suffix)
        newpath = Path(argob.dest, replace_base, new_name)
    else:
        newpath = Path(
            argob.dest, replace_base, "{}_{}".format(replace_name, path.stem)
        )
    if newpath == path:
        pass
    else:
        _strnewpath = str(newpath)
        conflict = False
        duplicate = False
        oldhash = None
        with argob.sharedns_lock:
            if _strnewpath in argob.sharedns.existing:
                argob.sharedns.existing[_strnewpath].append(_strnewpath)
                conflict = True
                oldhash = argob.sharedns.hashes.get(_strnewpath)
                if argob.replace:
                    argob.sharedns.hashes[_strnewpath] = file_hash
            else:
                argob.sharedns.existing[_strnewpath] = []
                argob.sharedns.hashes[_strnewpath] = file_hash
        if conflict:
            # conflict with previously existing file
            if not oldhash:
                oldhash = hashlib.blake2b(digest_size=8)
                with path.open(mode='rb') as f:
                    buffer = f.read(512000)
                    while buffer != b"":
                        oldhash.update(buffer)
                        buffer = f.read(512000)
                oldhash = oldhash.hexdigest()
            if oldhash == file_hash:
                conflict = False
                duplicate = True

        if argob.dry_run:
            if conflict and argob.replace:
                logger.info("Would replace: %s with %s", newpath, path)
            else:
                logger.info("Would rename: %s to %s", path, newpath)
        elif not conflict or argob.replace:
            if duplicate:
                path.unlink()
            else:
                newpath.parent.mkdir(mode=0o770, parents=True, exist_ok=True)
                path.rename(newpath)
        if duplicate:
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
    # first have all files, elsewise it is too risky
    files = []
    for src in argob.src:
        # only list non hidden files
        for file in src.rglob("[!.]*"):
            if not file.is_file() or file.is_symlink():
                continue
            files.append(file)
    with Manager() as manager:
        argob.sharedns_lock = manager.Lock()
        argob.sharedns = manager.Namespace()
        argob.sharedns.existing = {}
        argob.sharedns.hashes = {}
        for file in argob.dest.rglob("*"):
            if not file.is_file() or file.is_symlink():
                continue
            if file.suffix.lower() not in media_suffixes:
                if not argob.prune:
                    logger.info("unrecognized file: %s", file)
                elif argob.dry_run:
                    logger.info("Would remove: %s (unrecognized)", file)
                else:
                    file.unlink()
                continue
            argob.sharedns.existing[str(file)] = []
        with Pool() as pool:
            pruned_files = sum(pool.imap_unordered(
                processFile,
                zip(repeat(argob), files),
                chunksize=8
            ))
            pool.close()
            pool.join()
        logger.info(
            "Processed images and videos: %s", len(files) - pruned_files
        )
        if argob.prune:
            if argob.dry_run:
                logger.info("Would prune files: %i", pruned_files)
            else:
                logger.info("Pruned files: %i", pruned_files)
        conflicts = dict(filter(
            lambda x: len(x[1]) > 0, argob.sharedns.existing
        ))
        if conflicts:
            logger.info("files with conflicts:")
            for newname, oldnames in conflicts.items():
                logger.info("%s -> %s", oldnames, newname)


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    sortFiles(parser.parse_args(argv))


if __name__ == "__main__":
    main()
