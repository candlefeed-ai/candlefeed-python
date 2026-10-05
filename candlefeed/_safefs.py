"""Write files under a root without ever following a symlink below it.

Directories are opened one component at a time with O_NOFOLLOW and every later operation is relative to
that directory's descriptor, so swapping a component for a symlink mid-download can't redirect a write.
Temporary files are created with O_CREAT | O_EXCL | O_NOFOLLOW. The root itself is the caller's choice
and may be a symlink (macOS /tmp is one); nothing below it may be.

This needs directory-relative system calls (dir_fd) and O_NOFOLLOW, which CPython provides on Linux and
macOS. Where they're missing (Windows) SafeDir refuses to work rather than fall back to path checks,
because a path check can't stop a folder being swapped for a symlink between the check and the write.
"""
from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import BinaryIO, Optional, Sequence

_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_BINARY = getattr(os, "O_BINARY", 0)
DIR_FD = bool(_NOFOLLOW) and {os.open, os.mkdir, os.unlink, os.rename, os.stat} <= os.supports_dir_fd


class UnsafePath(OSError):
    """A path component below the root is a symlink or not a plain directory/file."""


class UnsupportedPlatform(OSError):
    """This Python lacks the directory-relative, no-follow operations safe writes depend on."""


class SafeDir:
    def __init__(self, root: Path, parts: Sequence[str]) -> None:
        for p in parts:
            if p in ("", ".", "..") or "/" in p or "\\" in p or "\0" in p:
                raise UnsafePath(f"bad path component {p!r}")
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root.joinpath(*parts)
        self._fd: Optional[int] = None
        if not DIR_FD:
            raise UnsupportedPlatform(
                "Safe L2 downloads need directory-relative file operations (dir_fd and O_NOFOLLOW), which this "
                "Python doesn't provide. They're available in CPython on Linux and macOS.")
        fd = os.open(root, os.O_RDONLY | _DIRECTORY)
        try:
            for p in parts:
                try:
                    os.mkdir(p, 0o755, dir_fd=fd)
                except FileExistsError:
                    pass
                try:
                    nfd = os.open(p, os.O_RDONLY | _DIRECTORY | _NOFOLLOW, dir_fd=fd)
                except OSError:
                    raise UnsafePath(f"{p!r} under {root} is a symlink or not a directory") from None
                os.close(fd)
                fd = nfd
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "SafeDir":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _check_name(self, name: str) -> None:
        if name in ("", ".", "..") or "/" in name or "\\" in name or "\0" in name:
            raise UnsafePath(f"bad file name {name!r}")

    def regular_size(self, name: str) -> Optional[int]:
        """Size of a plain file called name, or None if it's absent or anything else (a symlink included)."""
        self._check_name(name)
        try:
            st = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        return st.st_size if stat.S_ISREG(st.st_mode) else None

    def sha256(self, name: str) -> Optional[str]:
        self._check_name(name)
        try:
            fd = os.open(name, os.O_RDONLY | _NOFOLLOW | _BINARY, dir_fd=self._fd)
        except OSError:
            return None
        with os.fdopen(fd, "rb") as fh:
            if not stat.S_ISREG(os.fstat(fh.fileno()).st_mode):
                return None
            h = hashlib.sha256()
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    def unlink(self, name: str) -> None:
        self._check_name(name)
        try:
            os.unlink(name, dir_fd=self._fd)
        except FileNotFoundError:
            pass

    def create_exclusive(self, name: str) -> BinaryIO:
        """A new file opened for writing. Fails rather than open anything that already exists, a symlink included."""
        self._check_name(name)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW | _BINARY
        fd = os.open(name, flags, 0o644, dir_fd=self._fd)
        return os.fdopen(fd, "wb")

    def replace(self, src: str, dst: str) -> None:
        """Rename within this directory. A symlink at dst is replaced as a name, never written through."""
        self._check_name(src)
        self._check_name(dst)
        os.rename(src, dst, src_dir_fd=self._fd, dst_dir_fd=self._fd)   # POSIX rename replaces atomically
