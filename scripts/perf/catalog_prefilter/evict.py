"""Advise Linux to drop only the benchmark leaf files while the Lucene reader is closed."""

import os
import sys
from pathlib import Path

for file in Path(sys.argv[1]).iterdir():
    if file.is_file() and file.name != "write.lock":
        with file.open("rb") as src:
            os.posix_fadvise(src.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
