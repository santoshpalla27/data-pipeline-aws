"""
Custom exceptions for AWS Pricing Downloader.
"""


class DownloadError(Exception):
    """Base exception for download-related errors."""
    
    def __init__(self, message: str, service_code: str | None = None, url: str | None = None):
        self.service_code = service_code
        self.url = url
        super().__init__(message)


class StorageError(Exception):
    """Exception for storage-related errors."""
    
    def __init__(self, message: str, path: str | None = None):
        self.path = path
        super().__init__(message)


class IntegrityError(Exception):
    """Exception for integrity verification failures."""
    
    def __init__(self, message: str, expected: str | None = None, actual: str | None = None):
        self.expected = expected
        self.actual = actual
        super().__init__(message)


class HttpError(Exception):
    """Exception for HTTP-related errors."""
    
    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        url: str | None = None,
        response_body: str | None = None
    ):
        self.status_code = status_code
        self.url = url
        self.response_body = response_body
        super().__init__(message)