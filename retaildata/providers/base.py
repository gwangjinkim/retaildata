from abc import ABC, abstractmethod
from pathlib import Path
from retaildata.datasets.registry import Dataset

class BaseProvider(ABC):
    @abstractmethod
    def download(self, dataset: Dataset, destination: Path, meta_dir: Path, **kwargs):
        """
        Downloads the dataset files to the specified destination.
        
        Args:
            dataset: The Dataset object containing metadata.
            destination: The directory where files should be downloaded.
            meta_dir: The directory where metadata should be stored.
            **kwargs: Additional provider-specific arguments.
        """
        pass

    def prepare(self, dataset: Dataset, source_dir: Path, target_dir: Path, **kwargs):
        """
        Optional step to prepare/normalize the dataset structure.
        """
        pass
