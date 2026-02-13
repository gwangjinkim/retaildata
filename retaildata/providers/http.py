from pathlib import Path
import httpx
from tqdm import tqdm
from retaildata.datasets.registry import Dataset
from retaildata.providers.base import BaseProvider
from retaildata.postprocess.metadata import MetadataManager

class HTTPProvider(BaseProvider):
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Downloads one or more files from URLs using httpx.
        """
        if not dataset.url and not dataset.urls:
            raise ValueError(f"Dataset {dataset.id} does not have any URLs defined for HTTP provider.")

        destination.mkdir(parents=True, exist_ok=True)
        
        source_url = ""
        if dataset.urls:
            from retaildata.utils.parallel import parallel_downloader
            print(f"Downloading multiple files for {dataset.id} in parallel...")
            parallel_downloader.download_many(dataset.urls, destination)
            source_url = ", ".join(dataset.urls)
        else:
            url = dataset.url
            filename = url.split("/")[-1]
            file_path = destination / filename
            source_url = url
            
            print(f"Downloading {dataset.id} from {url}...")
            
            try:
                with httpx.stream("GET", url, follow_redirects=True) as response:
                    response.raise_for_status()
                    total = int(response.headers.get("content-length", 0))

                    with open(file_path, "wb") as f, tqdm(
                        desc=filename,
                        total=total,
                        unit="iB",
                        unit_scale=True,
                        unit_divisor=1024,
                    ) as progress_bar:
                        for chunk in response.iter_bytes():
                            size = f.write(chunk)
                            progress_bar.update(size)
                
                print(f"Download complete: {file_path}")
            except Exception as e:
                print(f"Error downloading {dataset.id}: {e}")
                raise

        try:
            # Save metadata
            MetadataManager.save_metadata(
                path=meta_dir / dataset.id / "metadata.json",
                dataset_id=dataset.id,
                provider="http",
                source_url=source_url
            )
            
            # Save checksums
            checksums_path = meta_dir / dataset.id / "checksums.json"
            MetadataManager.save_checksums(destination, checksums_path)
            
        except Exception as e:
            print(f"An error occurred while saving metadata: {e}")
            raise
