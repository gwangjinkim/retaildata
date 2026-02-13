import os
import shutil
from pathlib import Path
from retaildata.datasets.registry import Dataset
from retaildata.providers.base import BaseProvider
from retaildata.credentials.manager import manager
from retaildata.postprocess.metadata import MetadataManager

class KaggleProvider(BaseProvider):
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Downloads a dataset from Kaggle.
        """
        if not dataset.kaggle_id:
            raise ValueError(f"Dataset {dataset.id} does not have a kaggle_id defined.")

        # Retrieve credentials
        username = manager.get_credential("kaggle", "username")
        key = manager.get_credential("kaggle", "key")

        if not username or not key:
            raise RuntimeError(
                f"Kaggle credentials not found. Please run 'retaildata auth set kaggle' first."
            )

        # Inject credentials into env vars for the kaggle api
        os.environ["KAGGLE_USERNAME"] = username
        os.environ["KAGGLE_KEY"] = key

        # Import kaggle here to ensure it picks up the env vars
        # Or verify if we need to reload it. usually env vars are checked at instantiation of KaggelApi
        from kaggle.api.kaggle_api_extended import KaggleApi
        
        api = KaggleApi()
        api.authenticate()

        print(f"Downloading {dataset.kaggle_id} from Kaggle...")
        destination.mkdir(parents=True, exist_ok=True)

        # Download files
        # Check if it's a competition or a dataset
        try:
             if dataset.kaggle_id.startswith("c/"):
                 competition_id = dataset.kaggle_id[2:]
                 print(f"Downloading competition {competition_id} from Kaggle...")
                 api.competition_download_files(competition_id, path=destination, quiet=False)
                 
                 # Competitions download a single zip file usually, need to unzip it
                 zip_path = destination / f"{competition_id}.zip"
                 if zip_path.exists():
                     shutil.unpack_archive(zip_path, destination)
                     zip_path.unlink()
             else:
                 api.dataset_download_files(dataset.kaggle_id, path=destination, unzip=True, quiet=False)
             
             print(f"Download complete: {destination}")

             # Clean up zip file if strictly zip was downloaded (Kaggle API behavior varies)
             # But unzip=True usually extracts it.
             # Sometimes it leaves the zip?
             for item in destination.glob("*.zip"):
                 # Optional: remove the zip to save space? 
                 # For now let's keep it or follow standard. 
                 # Actually, let's remove it to be clean.
                 try:
                     item.unlink()
                 except:
                     pass

             # Save metadata
             MetadataManager.save_metadata(
                path=meta_dir / dataset.id / "metadata.json",
                dataset_id=dataset.id,
                provider="kaggle",
                source_url=f"https://www.kaggle.com/datasets/{dataset.kaggle_id}",
                kaggle_id=dataset.kaggle_id
             )
             
             checksums_path = meta_dir / dataset.id / "checksums.json"
             MetadataManager.save_checksums(destination, checksums_path)

        except Exception as e:
            print(f"Error downloading from Kaggle: {e}")
            raise
