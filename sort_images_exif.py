#! /usr/bin/env python3

import re
import argparse
import logging
from itertools import repeat
from datetime import datetime as dt
from pathlib import Path
from multiprocessing import Pool

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
    "--pattern",
    help="Pattern for images",
    default=(
        "{creation:%Y}/{creation:%m}/{creation:%Y-%m-%d_%H:%M:%S}{file_suffix}"
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


def processFile(args):
    argob, path = args
    creation = None
    file_content = ""
    image_exif = None
    image_exif_error = False
    if path.suffix.lower() in {
        ".png", ".tiff", ".jpg", ".jpeg"
    }:
        file_content = "IMG"
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
    elif path.suffix.lower() in {
        ".mp4", ".webm", ".ogg"
    }:
        file_content = "MOV"
    else:
        if not argob.prune:
            logger.info("unrecognized file: %s", path)
        elif argob.dry_run:
            logger.info("Would remove: %s (unrecognized)", path)
        else:
            path.unlink()
        return 1
    # check  and potential extract from filename
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

    if image_exif_error and not argob.dry_run:
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

    replacestr = argob.pattern.format(
        creation=creation,
        file_prefix=file_prefix,
        file_suffix=file_suffix,
        file_content=file_content
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
    elif argob.dry_run:
        logger.info("Would rename: %s to %s", path, newpath)
    else:
        newpath.parent.mkdir(mode=0o770, parents=True, exist_ok=True)
        path.rename(newpath)
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
        for file in src.rglob("*"):
            if not file.is_file() or file.is_symlink():
                continue
            files.append(file)
    with Pool() as pool:
        removed = sum(pool.imap_unordered(
            processFile,
            zip(repeat(argob), files),
            chunksize=8
        ))
        pool.close()
        pool.join()
    logger.info("Processed images and videos: %s", len(files) - removed)
    if argob.prune:
        logger.info("Pruned files: %s", removed)


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    sortFiles(parser.parse_args(argv))


if __name__ == "__main__":
    main()
