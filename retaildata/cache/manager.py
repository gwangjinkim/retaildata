import shutil
from pathlib import Path
from typing import List, Dict, Optional
import json
from retaildata.config import settings
from retaildata.datasets.registry import Registry

class CacheManager:
    def __init__(self):
        self.data_dir = settings.final_data_dir

    def _get_path(self, dataset_id: str, subdir: str) -> Path:
        return self.data_dir / subdir / dataset_id

    def is_downloaded(self, dataset_id: str) -> bool:
        """Check if a dataset is downloaded (metadata exists)."""
        meta_path = self._get_path(dataset_id, "meta") / "metadata.json"
        return meta_path.exists()

    def get_size(self, dataset_id: str) -> int:
        """Calculate total size of a dataset in bytes."""
        total_size = 0
        for subdir in ["raw", "prepared", "meta"]:
            path = self._get_path(dataset_id, subdir)
            if path.exists():
                for p in path.rglob("*"):
                    if p.is_file():
                        total_size += p.stat().st_size
        return total_size

    def list_downloaded(self) -> Dict[str, Dict[str, any]]:
        """List all downloaded datasets with details."""
        downloaded = {}
        meta_dir = self.data_dir / "meta"
        if not meta_dir.exists():
            return {}

        for ds_dir in meta_dir.iterdir():
            if ds_dir.is_dir():
                dataset_id = ds_dir.name
                if Registry.get(dataset_id): # Only track known datasets
                    size = self.get_size(dataset_id)
                    downloaded[dataset_id] = {
                        "size": size,
                        "path": str(self._get_path(dataset_id, "raw"))
                    }
        return downloaded

    def delete_dataset(self, dataset_id: str) -> bool:
        """Delete a dataset's files."""
        # Check if it was downloaded first? Or just force delete.
        # We delete from raw, prepared, and meta.
        deleted = False
        for subdir in ["raw", "prepared", "meta"]:
            path = self._get_path(dataset_id, subdir)
            if path.exists():
                shutil.rmtree(path)
                deleted = True
        return deleted

    def purge_all(self):
        """Delete all data in the data directory."""
        if self.data_dir.exists():
            # We want to keep the root data dir but empty it, or at least the subdirs we manage.
            # Safety: only delete known subdirs
            for subdir in ["raw", "prepared", "meta"]:
                path = self.data_dir / subdir
                if path.exists():
                    shutil.rmtree(path)

manager = CacheManager()
