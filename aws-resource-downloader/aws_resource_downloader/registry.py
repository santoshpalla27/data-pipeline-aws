"""
Service Registry Definition.
"""
from typing import List, Dict
from pydantic import BaseModel
from aws_resource_downloader.collector import ResourceConfig

class ServiceConfig(BaseModel):
    service_name: str
    resources: List[ResourceConfig]

class ServiceRegistry:
    """
    Loads and manages service definitions.
    """
    def __init__(self):
        self._services: Dict[str, ServiceConfig] = {}
        self._load_defaults()

    def get_service(self, name: str) -> ServiceConfig | None:
        return self._services.get(name)

    def list_services(self) -> List[str]:
        return list(self._services.keys())

    def _load_defaults(self):
        """Load built-in service definitions."""
        DEFAULT_REGISTRY = {
            "ec2": {
                "service_name": "ec2",
                "resources": [
                    {
                        "name": "instances",
                        "api_method": "describe_instances",
                        "response_key": "Reservations",
                        "pagination_config": {}
                    },
                    {
                        "name": "volumes",
                        "api_method": "describe_volumes",
                        "response_key": "Volumes",
                        "pagination_config": {}
                    },
                    {
                        "name": "instance_types",
                        "api_method": "describe_instance_types",
                        "response_key": "InstanceTypes",
                        "pagination_config": {}
                    }
                ]
            },
            "rds": {
                "service_name": "rds",
                "resources": [
                    {
                        "name": "db_instances",
                        "api_method": "describe_db_instances",
                        "response_key": "DBInstances",
                        "pagination_config": {}
                    },
                    {
                        "name": "db_clusters",
                        "api_method": "describe_db_clusters",
                        "response_key": "DBClusters",
                        "pagination_config": {}
                    }
                ]
            },
            "s3": {
                "service_name": "s3",
                "resources": [
                    {
                        "name": "buckets",
                        "api_method": "list_buckets",
                        "response_key": "Buckets",
                        "regional": False # Global
                    }
                ]
            },
            "eks": {
                "service_name": "eks",
                "resources": [
                    {
                        "name": "clusters",
                        "api_method": "list_clusters",
                        "response_key": "clusters",
                        "pagination_config": {}
                    }
                ]
            }
        }

        for name, cfg in DEFAULT_REGISTRY.items():
            self._services[name] = ServiceConfig(**cfg)

# Singleton instance
registry = ServiceRegistry()
