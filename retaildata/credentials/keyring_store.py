import keyring
from typing import Optional
from retaildata.credentials.store import CredentialStore

class KeyringStore(CredentialStore):
    def get(self, service_name: str, username: str) -> Optional[str]:
        return keyring.get_password(service_name, username)

    def set(self, service_name: str, username: str, password: str):
        keyring.set_password(service_name, username, password)

    def delete(self, service_name: str, username: str):
        try:
            keyring.delete_password(service_name, username)
        except keyring.errors.PasswordDeleteError:
            pass  # Already deleted or not found
