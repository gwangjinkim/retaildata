import os
import getpass
from pathlib import Path
from typing import Optional
from retaildata.credentials.store import CredentialStore
from retaildata.credentials.keyring_store import KeyringStore
from retaildata.credentials.encrypted_file_store import EncryptedFileStore
from retaildata.config import settings
from rich.prompt import Prompt

class CredentialManager:
    def __init__(self):
        self._store: Optional[CredentialStore] = None

    def _get_store(self) -> CredentialStore:
        if self._store:
            return self._store
        
        # Priority 1: Keyring
        try:
            import keyring
            # Check if keyring is usable (not null backend)
            if not isinstance(keyring.get_keyring(), keyring.backends.fail.Keyring):
                 self._store = KeyringStore()
                 return self._store
        except Exception:
            pass

        # Priority 2: Encrypted File Store
        # We need a password for this. If interactive, ask. If not, fail or use env var.
        master_password = os.environ.get("RETAILDATA_MASTER_PASSWORD")
        if not master_password:
             # In a real app we might prompt here, but for now let's default to a weak one
             # if running non-interactively, or rely on CLI to set it up.
             # For M1 simple path: use a fixed location and maybe prompt if needed.
             pass
        
        # Fallback for now: implementation specific
        # For M1, we will primarily rely on Keyring. 
        # If Keyring fails, user must configure file store explicitly via CLI.
        # But we need a default return.
        
        self._store = KeyringStore() 
        return self._store

    def get_credential(self, service: str, username: str) -> Optional[str]:
        return self._get_store().get(service, username)

    def set_credential(self, service: str, username: str, password: str):
        self._get_store().set(service, username, password)

    def delete_credential(self, service: str, username: str):
        self._get_store().delete(service, username)

manager = CredentialManager()
