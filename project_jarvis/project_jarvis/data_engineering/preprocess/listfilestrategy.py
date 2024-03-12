import base64
import hashlib
import os
import re
import tempfile
from abc import ABC
from glob import glob
from typing import IO, AsyncGenerator, Dict, List, Optional, Union


class File:
    """
    Represents a file stored either locally or in a data lake storage account
    This file might contain access control information about which users or groups can access it
    """

    def __init__(self, content: IO, acls: Optional[dict[str, list]] = None):
        self.content = content
        self.acls = acls or {}

    def filename(self):
        return os.path.basename(self.content.name)

    def file_extension(self):
        return os.path.splitext(self.content.name)[1]

    def filename_to_id(self):
        filename_ascii = re.sub("[^0-9a-zA-Z_-]", "_", self.filename())
        filename_hash = base64.b16encode(self.filename().encode("utf-8")).decode("ascii")
        return f"file-{filename_ascii}-{filename_hash}"

    def close(self):
        if self.content:
            self.content.close()


class ListFileStrategy(ABC):
    """
    Abstract strategy for listing files that are located somewhere. For example, on a local computer or remotely in a storage account
    """

    async def list(self) -> AsyncGenerator[File, None]:
        if False:  # pragma: no cover - this is necessary for mypy to type check
            yield

    async def list_paths(self) -> AsyncGenerator[str, None]:
        if False:  # pragma: no cover - this is necessary for mypy to type check
            yield


class LocalListFileStrategy(ListFileStrategy):
    """
    Concrete strategy for listing files that are located in a local filesystem
    """

    def __init__(self, path_pattern: str, verbose: bool = False):
        self.path_pattern = path_pattern
        self.verbose = verbose

    async def list_paths(self) -> AsyncGenerator[str, None]:
        async for p in self._list_paths(self.path_pattern):
            yield p

    async def _list_paths(self, path_pattern: str) -> AsyncGenerator[str, None]:
        for path in glob(path_pattern):
            print(path)
            if os.path.isdir(path):
                async for p in self._list_paths(f"{path}/*"):
                    yield p
            else:
                # Only list files, not directories
                yield path

    async def list(self) -> AsyncGenerator[File, None]:
        async for path in self.list_paths():
            if not self.check_md5(path):
                yield File(content=open(path, mode="rb"))

    def check_md5(self, path: str) -> bool:
        # if filename ends in .md5 skip
        if path.endswith(".md5"):
            return True

        # if there is a file called .md5 in this directory, see if its updated
        stored_hash = None
        with open(path, "rb") as file:
            existing_hash = hashlib.md5(file.read()).hexdigest()
        hash_path = f"{path}.md5"
        if os.path.exists(hash_path):
            with open(hash_path, encoding="utf-8") as md5_f:
                stored_hash = md5_f.read()

        if stored_hash and stored_hash.strip() == existing_hash.strip():
            if self.verbose:
                print(f"Skipping {path}, no changes detected.")
            return True

        # Write the hash
        with open(hash_path, "w", encoding="utf-8") as md5_f:
            md5_f.write(existing_hash)

        return False

