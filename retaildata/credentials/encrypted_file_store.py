import json
import os
import base64
from pathlib import Path
from typing import Optional, Dict
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.argon2 import Argon2id
from cryptography.hazmat.primitives import hashes
from retaildata.credentials.store import CredentialStore

class EncryptedFileStore(CredentialStore):
    def __init__(self, file_path: Path, master_password: str):
        self.file_path = file_path
        self.master_password = master_password
        self.fernet = self._derive_key(master_password)
        self._ensure_file()

    def _derive_key(self, password: str) -> Fernet:
        # For simplicity in this M1, we use a fixed salt. 
        # In production, salt should be stored with the file.
        # But here valid implementation requires salt to be consistent to regenerate key.
        # Let's use a deterministic salt based on file path for now (better than fixed string).
        salt = str(self.file_path).encode() 
        kdf = Argon2id(
            salt=salt,
            length=32,
            iterations=2,
            lanes=4,
            memory_cost=64 * 1024,
            ad=None,
            secret=None,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return Fernet(key)

    def _ensure_file(self):
        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w") as f:
                json.dump({}, f)

    def _load(self) -> Dict[str, str]:
        if not self.file_path.exists():
            return {}
        try:
            with open(self.file_path, "r") as f:
                content = f.read()
                if not content:
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            return {}

    def _save(self, data: Dict[str, str]):
        with open(self.file_path, "w") as f:
            json.dump(data, f)

    def get(self, service_name: str, username: str) -> Optional[str]:
        data = self._load()
        key = f"{service_name}:{username}"
        encrypted_val = data.get(key)
        if not encrypted_val:
            return None
        try:
            return self.fernet.decrypt(encrypted_val.encode()).decode()
        except Exception:
            return None

    def set(self, service_name: str, username: str, password: str):
        data = self._load()
        key = f"{service_name}:{username}"
        encrypted_val = self.fernet.encrypt(password.encode()).decode()
        data[key] = encrypted_val
        self._save(data)

    def delete(self, service_name: str, username: str):
        data = self._load()
        key = f"{service_name}:{username}"
        if key in data:
            del data[key]
            self._save(data)
