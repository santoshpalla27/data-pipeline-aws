"""
Generic AWS Resource Collector.
"""
from typing import Iterator, Any, Dict, List
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger
from pydantic import BaseModel

class ResourceConfig(BaseModel):
    """Configuration for a single resource type."""
    name: str
    api_method: str
    response_key: str | None = None
    pagination_config: Dict[str, Any] = {}
    regional: bool = True

class BaseCollector:
    """
    Generic collector engine.
    """
    def __init__(self, session_manager, service_name: str, resource_config: ResourceConfig):
        self.session_manager = session_manager
        self.service = service_name
        self.config = resource_config

    def collect(self, region: str) -> Iterator[Any]:
        """
        Yields pages of data for the resource in the given region.
        """
        client = self.session_manager.get_client(self.service, region)
        method_name = self.config.api_method
        
        logger.info(f"Collecting {self.service}.{self.config.name} in {region}...")

        # 1. Use Paginator if available
        if client.can_paginate(method_name):
            paginator = client.get_paginator(method_name)
            try:
                # We wrap the iterator to handle retries on individual pages if possible,
                # but standard boto3 paginators retry internally for throttling.
                # We add an outer retry just in case.
                for page in self._get_paginator_page(paginator, **self.config.pagination_config):
                    yield self._extract_data(page)
            except Exception as e:
                logger.error(f"Pagination failed for {self.service}.{self.config.name} in {region}: {e}")
                raise

        # 2. Raw Call (One-shot)
        else:
            try:
                method = getattr(client, method_name)
                response = self._call_safely(method)
                yield self._extract_data(response)
            except Exception as e:
                logger.error(f"Call failed for {self.service}.{self.config.name} in {region}: {e}")
                raise

    @retry(
        retry=retry_if_exception_type((ClientError, BotoCoreError)),
        stop=stop_after_attempt(6),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True
    )
    def _call_safely(self, method, **kwargs):
        """Execute a boto3 method with retries and improved logging."""
        try:
            return method(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            req_id = e.response.get("ResponseMetadata", {}).get("RequestId")
            logger.warning(f"ClientError {code} req_id={req_id} for {self.service}.{self.config.name}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type((ClientError, BotoCoreError)),
        stop=stop_after_attempt(6),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        reraise=True
    )
    def _get_paginator_page(self, paginator, **kwargs):
        """
        Yield pages from paginator with retry on iterator creation/access.
        """
        return paginator.paginate(**kwargs)

    def _extract_data(self, page: Dict[str, Any]) -> Any:
        """Extract relevant key or return full page."""
        # Strip ResponseMetadata to reduce noise
        if "ResponseMetadata" in page:
            del page["ResponseMetadata"]
            
        if self.config.response_key:
            return page.get(self.config.response_key, [])
        return page
