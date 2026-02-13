from abc import ABC, abstractmethod
from typing import Optional

class CredentialStore(ABC):
    @abstractmethod
    def get(self, service_name: str, username: str) -> Optional[str]:
        """Retrieve a password from the store."""
        pass

    @abstractmethod
    def set(self, service_name: str, username: str, password: str):
        """Save a password to the store."""
        pass

    @abstractmethod
    def delete(self, service_name: str, username: str):
        """Delete a password from the store."""
        pass
