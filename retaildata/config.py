from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from platformdirs import user_data_dir, user_cache_dir

class Settings(BaseSettings):
    app_name: str = "retaildata"
    
    # Default paths (platform specific)
    default_data_dir: Path = Path(user_data_dir("retaildata"))
    default_cache_dir: Path = Path(user_cache_dir("retaildata"))
    
    # User overrides (can be set via env RETAILDATA_DATA_DIR etc.)
    data_dir: Optional[Path] = None
    cache_dir: Optional[Path] = None
    
    # Cache settings
    cache_enabled: bool = True
    
    model_config = SettingsConfigDict(env_prefix="RETAILDATA_")

    @property
    def final_data_dir(self) -> Path:
        return self.data_dir or self.default_data_dir

    @property
    def final_cache_dir(self) -> Path:
        return self.cache_dir or self.default_cache_dir

settings = Settings()
