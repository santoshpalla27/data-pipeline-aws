"""
AWS Session and Region Management.
"""
from typing import List
import boto3
from loguru import logger

class AwsSessionManager:
    """
    Manages AWS sessions and region discovery.
    """
    def __init__(self, profile: str | None = None, region: str | None = None):
        self.profile = profile
        self.region = region
        self.session = boto3.Session(profile_name=profile, region_name=region)
        self.identity = self.get_caller_identity()

    def get_caller_identity(self) -> dict:
        """Get current STS identity."""
        try:
            return self.session.client("sts").get_caller_identity()
        except Exception as e:
            logger.warning(f"Failed to get caller identity: {e}")
            return {}

    def get_client(self, service: str, region: str | None = None):
        """Create a boto3 client."""
        return self.session.client(service, region_name=region)

    def get_available_regions(self, service: str) -> List[str]:
        """
        Get all enabled regions for a service.
        """
        try:
            return self.session.get_available_regions(service)
        except Exception as e:
            logger.error(f"Failed to list regions for {service}: {e}")
            return []
