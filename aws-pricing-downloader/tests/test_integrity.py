"""
Tests for integrity module.
"""

import pytest
from pathlib import Path

from aws_pricing_downloader.integrity import IntegrityVerifier
from aws_pricing_downloader.exceptions import IntegrityError


def test_compute_hash(test_config, temp_dir):
    """Test hash computation."""
    verifier = IntegrityVerifier(temp_dir)
    
    # Create test file
    test_file = temp_dir / "test.json"
    test_data = b'{"test": "data"}'
    test_file.write_bytes(test_data)
    
    hash_value = verifier.compute_hash(test_file)
    
    assert len(hash_value) == 64  # SHA256 is 64 hex chars
    assert isinstance(hash_value, str)


def test_save_and_load_hash(test_config, temp_dir):
    """Test saving and loading hash metadata."""
    verifier = IntegrityVerifier(temp_dir)
    
    service_code = "AmazonEC2"
    hash_value = "abc123"
    etag = '"xyz789"'
    
    verifier.save_hash(service_code, hash_value, etag)
    
    metadata = verifier.load_hash(service_code)
    assert metadata is not None
    assert metadata["sha256"] == hash_value
    assert metadata["etag"] == etag


def test_verify_file_success(test_config, temp_dir):
    """Test successful file verification."""
    verifier = IntegrityVerifier(temp_dir)
    
    service_code = "AmazonEC2"
    
    # Create and hash file
    test_file = temp_dir / f"{service_code}.json"
    test_data = b'{"test": "data"}'
    test_file.write_bytes(test_data)
    
    hash_value = verifier.compute_hash(test_file)
    verifier.save_hash(service_code, hash_value)
    
    # Should verify successfully
    assert verifier.verify_file(service_code)


def test_verify_file_mismatch(test_config, temp_dir):
    """Test file verification with hash mismatch."""
    verifier = IntegrityVerifier(temp_dir)
    
    service_code = "AmazonEC2"
    
    # Create file
    test_file = temp_dir / f"{service_code}.json"
    test_data = b'{"test": "data"}'
    test_file.write_bytes(test_data)
    
    # Save wrong hash
    verifier.save_hash(service_code, "wrong_hash")
    
    # Should raise IntegrityError
    with pytest.raises(IntegrityError):
        verifier.verify_file(service_code)


def test_should_download_new_file(test_config, temp_dir):
    """Test download decision for new file."""
    verifier = IntegrityVerifier(temp_dir)
    
    # New file should be downloaded
    assert verifier.should_download("NewService", '"etag123"')


def test_should_download_etag_changed(test_config, temp_dir):
    """Test download decision when ETag changes."""
    verifier = IntegrityVerifier(temp_dir)
    
    service_code = "AmazonEC2"
    
    # Create file with old ETag
    test_file = temp_dir / f"{service_code}.json"
    test_file.write_bytes(b'test')
    
    old_etag = '"old_etag"'
    hash_value = verifier.compute_hash(test_file)
    verifier.save_hash(service_code, hash_value, old_etag)
    
    # New ETag should trigger download
    new_etag = '"new_etag"'
    assert verifier.should_download(service_code, new_etag)
