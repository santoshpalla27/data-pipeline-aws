"""
Generic AWS Resource Collector (V2).
Implements safe, manual pagination with per-page retries.
"""
from typing import Iterator, Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
from loguru import logger
from aws_resource_downloader.registry import ResourceConfig

class BaseCollector:
    """
    Generic collector engine with robust manual pagination.
    """
    def __init__(self, session_manager, service_name: str, resource_config: ResourceConfig):
        self.session_manager = session_manager
        self.service = service_name
        self.config = resource_config

    def collect(self, region: str) -> Iterator[Any]:
        """
        Yields pages of data for the resource in the given region.
        """
        # Create client
        try:
             client = self.session_manager.get_client(self.service, region)
        except Exception as e:
            logger.error(f"Failed to create client for {self.service} in {region}: {e}")
            return

        method_name = self.config.api_method
        logger.info(f"Collecting {self.service}.{self.config.name} in {region}...")

        # Determine Pagination Strategy
        if client.can_paginate(method_name):
            try:
                yield from self._collect_paginated(client, method_name, region)
            except Exception as e:
                logger.error(f"Collector failed for {self.service}.{self.config.name} in {region}: {e}")
                # We do not raise here to allow other resources/regions to continue
        else:
             # One-shot call
            try:
                yield from self._collect_oneshot(client, method_name, region)
            except Exception as e:
                logger.error(f"Collector failed (oneshot) for {self.service}.{self.config.name} in {region}: {e}")

    def _collect_paginated(self, client, method_name: str, region: str) -> Iterator[Any]:
        """
        Manually iterates pages using the Paginator model to ensure per-page retries.
        """
        # Introspect Paginator Model to find Token Keys
        paginator = client.get_paginator(method_name)
        
        # paginator._model.input_token and output_token can be strings or lists
        input_token_keys = paginator._model.input_token
        output_token_keys = paginator._model.output_token
        
        # Normalize to lists
        if isinstance(input_token_keys, str): input_token_keys = [input_token_keys]
        if isinstance(output_token_keys, str): output_token_keys = [output_token_keys]
        
        if not input_token_keys or not output_token_keys:
            # Fallback for weird models: usage standard iterator (unsafe per-page, but functional)
            logger.warning(f"Could not determine token keys for {method_name}, falling back to standard iterator.")
            yield from self._standard_paginate(paginator)
            return

        # Prepare initial params
        params = self.config.pagination_config.copy()
        
        next_token = None
        page_num = 0
        
        while True:
            page_num += 1
            
            # Inject Token
            if next_token:
                # Some APIs use multiple tokens, complex to map generic. 
                # We assume 1:1 mapping for 99% of APIs: input_token[0] = next_token
                params[input_token_keys[0]] = next_token

            # Call API with Retry
            try:
                method = getattr(client, method_name)
                response = self._call_with_retry(method, **params)
            except RetryError as re:
                logger.error(f"Exhausted retries for {self.service}.{self.config.name} in {region} page {page_num}: {re}")
                break
            except Exception as e:
                logger.error(f"Fatal error for {self.service}.{self.config.name} in {region} page {page_num}: {e}")
                break

            # Yield Page (after stripping metadata if desired, but we keep full storage meta logic elsewhere)
            # We strip ResponseMetadata here to save space
            if "ResponseMetadata" in response:
                del response["ResponseMetadata"]
            
            yield response
            
            # Extract Next Token
            # output_token_keys[0] usually contains the next token or generic path
            # Simple extraction:
            next_token = self._extract_token(response, output_token_keys[0])
            
            if not next_token:
                break

    def _extract_token(self, response: dict, key_path: str) -> Any:
        """Extract token (simple key lookup)."""
        # Boto3 models can have 'Res/Key' syntax but usually just 'NextToken'
        return response.get(key_path)

    def _standard_paginate(self, paginator) -> Iterator[Any]:
        """Fallback to standard boto3 pagination."""
        for page in paginator.paginate(**self.config.pagination_config):
             if "ResponseMetadata" in page: del page["ResponseMetadata"]
             yield page

    def _collect_oneshot(self, client, method_name: str, region: str) -> Iterator[Any]:
        """One-shot API call with retry."""
        method = getattr(client, method_name)
        response = self._call_with_retry(method, **self.config.pagination_config)
        if "ResponseMetadata" in response: del response["ResponseMetadata"]
        yield response

    @retry(
        retry=retry_if_exception_type((ClientError, BotoCoreError)),
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=1, max=30),
        reraise=True
    )
    def _call_with_retry(self, method, **kwargs):
        """Execute single API call with retry."""
        try:
            return method(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            # Filter non-retryable errors if needed (e.g. AccessDenied)
            if code in ["AccessDenied", "AuthFailure", "UnrecognizedClientException", "InvalidClientTokenId"]:
                # Do not retry, raise immediately to break retry loop (will be caught by outer loop)
                 raise
            
            # Log throttling
            if code in ["Throttling", "ThrottlingException", "RequestLimitExceeded", "ProvisionedThroughputExceededException"]:
                 logger.warning(f"Throttling detected ({code}). Backing off...")
            
            raise e
