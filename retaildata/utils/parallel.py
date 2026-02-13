import httpx
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
from tqdm import tqdm

class ParallelDownloader:
    """
    Utility for downloading multiple files in parallel.
    """
    
    @staticmethod
    def download_file(url: str, dest_dir: Path, client: httpx.Client) -> Tuple[str, bool]:
        """Downloads a single file."""
        filename = url.split("/")[-1]
        file_path = dest_dir / filename
        try:
            with client.stream("GET", url, follow_redirects=True) as response:
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
            return filename, True
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return filename, False

    def download_many(self, urls: List[str], dest_dir: Path, max_workers: int = 4):
        """Downloads multiple URLs in parallel."""
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        results = []
        with httpx.Client(timeout=60.0) as client:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.download_file, url, dest_dir, client): url for url in urls}
                
                with tqdm(total=len(urls), desc="Downloading files", unit="file") as progress:
                    for future in as_completed(futures):
                        filename, success = future.result()
                        results.append((filename, success))
                        progress.update(1)
        
        return results

parallel_downloader = ParallelDownloader()
