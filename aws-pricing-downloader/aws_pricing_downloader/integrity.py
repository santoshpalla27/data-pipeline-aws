"""
SHA256 integrity verification for downloaded files.
"""

import hashlib
from pathlib import Path
from typing import Optional
import orjson

from aws_pricing_downloader.logger import get_logger
from aws_pricing_downloader.exceptions import IntegrityError, StorageError


logger = get_logger(__name__)


class IntegrityVerifier:
    """Verify file integrity using SHA256 hashes."""
    
    def __init__(self, output_dir: Path):
        """
        Initialize integrity verifier.
        
        Args:
            output_dir: Directory containing files to verify
        """
        self.output_dir = output_dir
    
    def _get_hash_path(self, service_code: str) -> Path:
        """Get path to hash file."""
        return self.output_dir / f"{service_code}.sha256"
    
    def _get_data_path(self, service_code: str) -> Path:
        """Get path to data file."""
        return self.output_dir / f"{service_code}.json"
    
    def compute_hash(self, file_path: Path) -> str:
        """
        Compute SHA256 hash of a file.
        
        Args:
            file_path: Path to file
            
        Returns:
            Hex-encoded SHA256 hash
        """
        sha256 = hashlib.sha256()
        
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    sha256.update(chunk)
            
            hash_value = sha256.hexdigest()
            
            logger.debug(
                "Computed file hash",
                extra={
                    "file": str(file_path),
                    "sha256": hash_value,
                }
            )
            
            return hash_value
            
        except Exception as e:
            raise StorageError(
                f"Failed to compute hash for {file_path}: {str(e)}",
                path=str(file_path)
            ) from e
    
    def save_hash(self, service_code: str, hash_value: str, etag: str | None = None):
        """
        Save hash metadata to file.
        
        Args:
            service_code: Service code
            hash_value: SHA256 hash
            etag: Optional ETag from response
        """
        hash_path = self._get_hash_path(service_code)
        
        metadata = {
            "sha256": hash_value,
            "etag": etag,
        }
        
        try:
            with open(hash_path, "wb") as f:
                f.write(orjson.dumps(metadata, option=orjson.OPT_INDENT_2))
            
            logger.debug(
                "Saved hash metadata",
                extra={
                    "service_code": service_code,
                    "sha256": hash_value,
                    "etag": etag,
                }
            )
            
        except Exception as e:
            raise StorageError(
                f"Failed to save hash for {service_code}: {str(e)}",
                path=str(hash_path)
            ) from e
    
    def load_hash(self, service_code: str) -> Optional[dict]:
        """
        Load hash metadata from file.
        
        Args:
            service_code: Service code
            
        Returns:
            Dictionary with sha256 and etag, or None if not found
        """
        hash_path = self._get_hash_path(service_code)
        
        if not hash_path.exists():
            return None
        
        try:
            with open(hash_path, "rb") as f:
                metadata = orjson.loads(f.read())
            
            logger.debug(
                "Loaded hash metadata",
                extra={
                    "service_code": service_code,
                    "sha256": metadata.get("sha256"),
                }
            )
            
            return metadata
            
        except Exception as e:
            logger.warning(
                "Failed to load hash metadata",
                extra={"service_code": service_code},
                exc_info=True
            )
            return None
    
    def verify_file(self, service_code: str) -> bool:
        """
        Verify file integrity against stored hash.
        
        Args:
            service_code: Service code
            
        Returns:
            True if file is valid, False if verification fails
            
        Raises:
            IntegrityError: On hash mismatch
        """
        data_path = self._get_data_path(service_code)
        
        if not data_path.exists():
            return False
        
        # Load stored hash
        metadata = self.load_hash(service_code)
        if not metadata:
            logger.debug(
                "No stored hash found, skipping verification",
                extra={"service_code": service_code}
            )
            return True
        
        stored_hash = metadata.get("sha256")
        if not stored_hash:
            return True
        
        # Compute current hash
        current_hash = self.compute_hash(data_path)
        
        if current_hash != stored_hash:
            logger.error(
                "Hash mismatch detected",
                extra={
                    "service_code": service_code,
                    "expected": stored_hash,
                    "actual": current_hash,
                }
            )
            raise IntegrityError(
                f"Hash mismatch for {service_code}",
                expected=stored_hash,
                actual=current_hash,
            )
        
        logger.debug(
            "File integrity verified",
            extra={"service_code": service_code, "sha256": current_hash}
        )
        
        return True
    
    def should_download(self, service_code: str, etag: str | None) -> bool:
        """
        Determine if file should be downloaded based on hash and ETag.
        
        Args:
            service_code: Service code
            etag: ETag from response
            
        Returns:
            True if download is needed
        """
        data_path = self._get_data_path(service_code)
        
        # File doesn't exist
        if not data_path.exists():
            return True
        
        # No stored metadata
        metadata = self.load_hash(service_code)
        if not metadata:
            return True
        
        # ETag changed
        stored_etag = metadata.get("etag")
        if stored_etag and etag and stored_etag != etag:
            logger.info(
                "ETag changed, download required",
                extra={
                    "service_code": service_code,
                    "old_etag": stored_etag,
                    "new_etag": etag,
                }
            )
            return True
        
        # Verify integrity
        try:
            if not self.verify_file(service_code):
                return True
        except IntegrityError:
            logger.warning(
                "Integrity check failed, re-download required",
                extra={"service_code": service_code}
            )
            return True
        
        return False
