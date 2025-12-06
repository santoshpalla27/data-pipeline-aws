"""
AWS Session and Region Management (V2).
Includes account-level region discovery.
"""
from typing import List, Set
import boto3
from loguru import logger

class AwsSessionManager:
    """
    Manages AWS sessions and robust region discovery.
    """
    def __init__(self, profile: str | None = None, region: str | None = None):
        self.profile = profile
        self.region = region
        self.session = boto3.Session(profile_name=profile, region_name=region)
        self.identity = self.get_caller_identity()
        
        # Cache for enabled regions
        self._enabled_regions: Set[str] = set()
        self._discover_enabled_regions()

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

    def _discover_enabled_regions(self):
        """
        Fetch list of all enabled regions for this account using EC2 API.
        This prevents attempting calls to opted-out regions.
        """
        try:
            ec2 = self.session.client("ec2", region_name="us-east-1")
            response = ec2.describe_regions(AllRegions=False)
            self._enabled_regions = {r["RegionName"] for r in response["Regions"]}
            logger.info(f"Discovered {len(self._enabled_regions)} enabled regions for account.")
        except Exception as e:
            logger.warning(f"Failed to discover enabled regions via EC2: {e}. Falling back to 'us-east-1'.")
            self._enabled_regions = {"us-east-1"}

    def get_available_regions(self, service: str) -> List[str]:
        """
        Get all enabled regions for a service.
        Intersects the service's supported regions (from boto3) with the account's enabled regions.
        """
        try:
            # All regions supported by the SDK for this service
            service_regions = set(self.session.get_available_regions(service))
            
            if not service_regions:
                # Some global services return empty list; default to us-east-1 or enabled regions
                return list(self._enabled_regions)

            # return intersection
            available = list(service_regions.intersection(self._enabled_regions))
            available.sort()
            return available
            
        except Exception as e:
            logger.error(f"Failed to list regions for {service}: {e}")
            return []
