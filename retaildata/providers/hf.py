from pathlib import Path
from typing import Optional
from huggingface_hub import snapshot_download
from retaildata.datasets.registry import Dataset
from retaildata.providers.base import BaseProvider
from retaildata.postprocess.metadata import MetadataManager

class HFProvider(BaseProvider):
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Downloads a dataset from Hugging Face Hub.
        """
        if not dataset.hf_repo_id:
            raise ValueError(f"Dataset {dataset.id} does not have a hf_repo_id defined.")

        print(f"Downloading {dataset.id} from Hugging Face Hub ({dataset.hf_repo_id})...")
        
        try:
            # Download to a temporary location first or directly? 
            # snapshot_download usually caches. We want to copy to our destination.
            # actually snapshot_download can download to local_dir.
            
            # If dataset has specific files, we could use allow_patterns
            allow_patterns = kwargs.get("allow_patterns")
            ignore_patterns = kwargs.get("ignore_patterns")
            
            # Retrieve token from CredentialManager
            from retaildata.credentials.manager import manager as cred_manager
            token = cred_manager.get_credential("hf", "token")

            snapshot_download(
                repo_id=dataset.hf_repo_id,
                repo_type="dataset",
                local_dir=destination,
                local_dir_use_symlinks=False, # We want actual files
                allow_patterns=allow_patterns,
                ignore_patterns=ignore_patterns,
                token=token,
                tqdm_class=None # validation? None allows default tqdm
            )
            
            print(f"Download complete: {destination}")
            
            # Save metadata
            MetadataManager.save_metadata(
                path=meta_dir / dataset.id / "metadata.json",
                dataset_id=dataset.id,
                provider="hf",
                source_repo=dataset.hf_repo_id
            )
            
            # Save checksums
            checksums_path = meta_dir / dataset.id / "checksums.json"
            MetadataManager.save_checksums(destination, checksums_path)

        except Exception as e:
            print(f"Error downloading {dataset.id} from HF: {e}")
            raise
