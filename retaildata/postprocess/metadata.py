import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

class MetadataManager:
    @staticmethod
    def save_metadata(path: Path, dataset_id: str, provider: str, source_url: Optional[str] = None, **kwargs):
        """Saves metadata about the dataset download."""
        path.parent.mkdir(parents=True, exist_ok=True)
        
        metadata = {
            "id": dataset_id,
            "provider": provider,
            "downloaded_at": datetime.now().isoformat(),
            "source_url": source_url,
            **kwargs
        }
        
        with open(path, "w") as f:
            json.dump(metadata, f, indent=2)

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """Calculates SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    @staticmethod
    def save_checksums(base_dir: Path, checksums_path: Path):
        """Generates and saves checksums for all files in base_dir."""
        checksums = {}
        processed_files = []
        
        # Traverse all files in data directory
        for file_path in base_dir.rglob("*"):
            if file_path.is_file() and file_path != checksums_path:
                rel_path = str(file_path.relative_to(base_dir))
                checksums[rel_path] = MetadataManager.calculate_checksum(file_path)
        
        checksums_path.parent.mkdir(parents=True, exist_ok=True)
        with open(checksums_path, "w") as f:
            json.dump(checksums, f, indent=2)
