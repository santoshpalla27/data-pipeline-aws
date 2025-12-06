"""
Storage engine for raw resource metadata (V2).
Supports GZIP compression and Run-ID based organization.
"""
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
import gzip
import orjson
from loguru import logger

class StorageManager:
    """
    Handles saving raw API responses to disk with compression.
    Structure: data/resource_metadata/run_<timestamp>/<service>/<resource>/<region>/page_<N>.json.gz
    """

    def __init__(self, base_dir: Path, run_id: str, compress: bool = True):
        self.base_dir = Path(base_dir).resolve()
        self.run_id = run_id
        self.compress = compress
        
        # Create run directory
        self.run_dir = self.base_dir / self.run_id
        try:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage Initialized at {self.run_dir}")
        except Exception as e:
            logger.error(f"Failed to create run directory {self.run_dir}: {e}")
            raise

    def save_page(
        self,
        service: str,
        resource: str,
        region: str,
        data: Dict[str, Any] | List[Any],
        page_num: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Path:
        """
        Save a single page of data to disk.
        """
        # Construct path: run_id/service/resource/region/
        output_dir = self.run_dir / service / resource / region
        output_dir.mkdir(parents=True, exist_ok=True)

        # Format filename: page_N.json(.gz)
        filename = f"page_{page_num}.json"
        if self.compress:
            filename += ".gz"
            
        file_path = output_dir / filename
        
        # Prepare content with metadata
        content = {
            "_meta": {
                "run_id": self.run_id,
                "service": service,
                "resource": resource,
                "region": region,
                "page": page_num,
                "timestamp": datetime.utcnow().isoformat(),
                **(metadata or {})
            },
            "payload": data
        }

        # Atomic write (write to .tmp then rename)
        temp_path = file_path.with_suffix(".tmp")
        
        try:
            # Serialize JSON
            json_bytes = orjson.dumps(content, option=orjson.OPT_INDENT_2)
            
            if self.compress:
                with gzip.open(temp_path, "wb") as f:
                    f.write(json_bytes)
            else:
                with open(temp_path, "wb") as f:
                    f.write(json_bytes)
            
            # Atomic rename
            temp_path.rename(file_path)
            
            logger.debug(f"Saved {file_path}")
            return file_path
            
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Failed to save {file_path}: {e}")
            raise
