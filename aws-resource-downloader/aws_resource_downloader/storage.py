"""
Storage engine for raw resource metadata.
"""
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List
import orjson
from loguru import logger

class StorageManager:
    """
    Handles saving raw API responses to disk.
    Structure: data/resource_metadata/<service>/<resource>/<region>/<timestamp>_<page>.json
    """

    def __init__(self, base_dir: Path = Path("data/resource_metadata")):
        self.base_dir = Path(base_dir).resolve()
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create base_dir {self.base_dir}: {e}")
            raise

    def save_page(
        self,
        service: str,
        resource: str,
        region: str,
        data: Dict[str, Any] | List[Any],
        page_num: int,
        timestamp: datetime,
    ) -> Path:
        """
        Save a single page of data to disk.
        """
        # Construct path: base/service/resource/region/
        output_dir = self.base_dir / service / resource / region
        output_dir.mkdir(parents=True, exist_ok=True)

        # Format filename: YYYY-MM-DD_HH-MM-SS_pageN.json
        ts_str = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{ts_str}_page{page_num}.json"
        file_path = output_dir / filename
        
        # Prepare content with metadata
        content = {
            "_meta": {
                "service": service,
                "resource": resource,
                "region": region,
                "page": page_num,
                "timestamp": timestamp.isoformat()
            },
            "payload": data
        }

        # Atomic write (write to .tmp then rename)
        temp_path = file_path.with_suffix(".tmp")
        
        try:
            with open(temp_path, "wb") as f:
                f.write(orjson.dumps(content, option=orjson.OPT_INDENT_2))
            
            # Atomic rename
            temp_path.rename(file_path)
            
            logger.debug(f"Saved {file_path}")
            return file_path
            
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Failed to save {file_path}: {e}")
            raise
