"""
Service Registry Definition (V2).
"""
from typing import List, Dict, Optional, Any
from pydantic import BaseModel

class ResourceConfig(BaseModel):
    name: str # e.g. "instances"
    api_method: str # e.g. "describe_instances"
    response_key: str | None = None # e.g. "Reservations"
    pagination_config: Dict[str, Any] = {} # e.g. {"Scope": "REGIONAL"}
    regional: bool = True # If False, only One region (Global) is queried.
    forced_region: Optional[str] = None # e.g. "us-east-1" for CloudFront/WAF Global

class ServiceConfig(BaseModel):
    service_name: str # Boto3 client name, e.g. "ec2"
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
        """Load service definitions mapping Pricing Codes to Boto3 APIs."""
        DEFAULT_REGISTRY = {
            # Compute
            "AmazonEC2": {
                "service_name": "ec2",
                "resources": [
                    {"name": "instances", "api_method": "describe_instances", "response_key": "Reservations"},
                    {"name": "volumes", "api_method": "describe_volumes", "response_key": "Volumes"},
                    {"name": "instance_types", "api_method": "describe_instance_types", "response_key": "InstanceTypes"}
                ]
            },
            "AmazonECS": {
                "service_name": "ecs",
                "resources": [
                    {"name": "clusters", "api_method": "list_clusters", "response_key": "clusterArns"}
                ]
            },
            "AmazonEKS": {
                "service_name": "eks",
                "resources": [
                    {"name": "clusters", "api_method": "list_clusters", "response_key": "clusters"}
                ]
            },
            "AWSLambda": {
                "service_name": "lambda",
                "resources": [
                    {"name": "functions", "api_method": "list_functions", "response_key": "Functions"}
                ]
            },
            
            # Storage
            "AmazonS3": {
                "service_name": "s3",
                "resources": [
                    {"name": "buckets", "api_method": "list_buckets", "response_key": "Buckets", "regional": False}
                ]
            },
            "AmazonEFS": {
                "service_name": "efs",
                "resources": [
                    {"name": "file_systems", "api_method": "describe_file_systems", "response_key": "FileSystems"}
                ]
            },
            "AmazonFSx": {
                "service_name": "fsx",
                "resources": [
                    {"name": "file_systems", "api_method": "describe_file_systems", "response_key": "FileSystems"}
                ]
            },
            "AWSBackup": {
                "service_name": "backup",
                "resources": [
                    {"name": "backup_plans", "api_method": "list_backup_plans", "response_key": "BackupPlansList"},
                    {"name": "backup_vaults", "api_method": "list_backup_vaults", "response_key": "BackupVaultList"}
                ]
            },
            "AmazonS3GlacierDeepArchive": {
                "service_name": "glacier",
                "resources": [
                    {"name": "vaults", "api_method": "list_vaults", "response_key": "VaultList"}
                ]
            },

            # Database
            "AmazonRDS": {
                "service_name": "rds",
                "resources": [
                    {"name": "db_instances", "api_method": "describe_db_instances", "response_key": "DBInstances"},
                    {"name": "db_clusters", "api_method": "describe_db_clusters", "response_key": "DBClusters"}
                ]
            },
            "AmazonDynamoDB": {
                "service_name": "dynamodb",
                "resources": [
                    {"name": "tables", "api_method": "list_tables", "response_key": "TableNames"}
                ]
            },
            "AmazonElastiCache": {
                "service_name": "elasticache",
                "resources": [
                    {"name": "cache_clusters", "api_method": "describe_cache_clusters", "response_key": "CacheClusters"},
                    {"name": "replication_groups", "api_method": "describe_replication_groups", "response_key": "ReplicationGroups"}
                ]
            },
            "AmazonRedshift": {
                "service_name": "redshift",
                "resources": [
                    {"name": "clusters", "api_method": "describe_clusters", "response_key": "Clusters"}
                ]
            },

            # Networking
            "AmazonVPC": {
                "service_name": "ec2",
                "resources": [
                    {"name": "vpcs", "api_method": "describe_vpcs", "response_key": "Vpcs"},
                    {"name": "subnets", "api_method": "describe_subnets", "response_key": "Subnets"},
                    {"name": "security_groups", "api_method": "describe_security_groups", "response_key": "SecurityGroups"},
                    {"name": "nat_gateways", "api_method": "describe_nat_gateways", "response_key": "NatGateways"}
                ]
            },
            "AWSELB": { # ELBv2 (ALB/NLB)
                "service_name": "elbv2",
                "resources": [
                    {"name": "load_balancers", "api_method": "describe_load_balancers", "response_key": "LoadBalancers"}
                ]
            },
            "AmazonRoute53": {
                "service_name": "route53",
                "resources": [
                    {"name": "hosted_zones", "api_method": "list_hosted_zones", "response_key": "HostedZones", "regional": False, "forced_region": "us-east-1"}
                ]
            },
            "AmazonCloudFront": {
                "service_name": "cloudfront",
                "resources": [
                    {"name": "distributions", "api_method": "list_distributions", "response_key": "DistributionList", "regional": False, "forced_region": "us-east-1"}
                ]
            },
            "AmazonApiGateway": {
                "service_name": "apigateway",
                "resources": [
                    {"name": "rest_apis", "api_method": "get_rest_apis", "response_key": "items"}
                ]
            },
            "AWSGlobalAccelerator": {
                "service_name": "globalaccelerator",
                "resources": [
                    {"name": "accelerators", "api_method": "list_accelerators", "response_key": "Accelerators", "regional": False, "forced_region": "us-west-2"} 
                    # Note: GlobalAccelerator API endpoint is usually us-west-2 based despite being global
                ]
            },

            # Messaging & Integration
            "AWSQueueService": {
                "service_name": "sqs",
                "resources": [
                    {"name": "queues", "api_method": "list_queues", "response_key": "QueueUrls"}
                ]
            },
            "AmazonSNS": {
                "service_name": "sns",
                "resources": [
                    {"name": "topics", "api_method": "list_topics", "response_key": "Topics"}
                ]
            },
            "AmazonMQ": {
                "service_name": "mq",
                "resources": [
                    {"name": "brokers", "api_method": "list_brokers", "response_key": "BrokerSummaries"}
                ]
            },
            "AmazonMSK": {
                "service_name": "kafka",
                "resources": [
                    {"name": "clusters", "api_method": "list_clusters", "response_key": "ClusterInfoList"}
                ]
            },
            "AWSEvents": { # Fixed capitalization
                "service_name": "events", # EventBridge
                "resources": [
                    {"name": "event_buses", "api_method": "list_event_buses", "response_key": "EventBuses"},
                    {"name": "rules", "api_method": "list_rules", "response_key": "Rules"}
                ]
            },

            # Management & Gov
            "AmazonCloudWatch": {
                "service_name": "cloudwatch",
                "resources": [
                    {"name": "alarms", "api_method": "describe_alarms", "response_key": "MetricAlarms"}
                ]
            },
            "AWSCloudTrail": {
                "service_name": "cloudtrail",
                "resources": [
                    {"name": "trails", "api_method": "list_trails", "response_key": "Trails"}
                ]
            },
            "AWSConfig": {
                "service_name": "config",
                "resources": [
                    {"name": "configuration_recorders", "api_method": "describe_configuration_recorders", "response_key": "ConfigurationRecorders"}
                ]
            },
            "AWSSystemsManager": {
                "service_name": "ssm",
                "resources": [
                    {"name": "documents", "api_method": "list_documents", "response_key": "DocumentIdentifiers"}
                ]
            },
            "AWSServiceCatalog": {
                "service_name": "servicecatalog",
                "resources": [
                    {"name": "portfolios", "api_method": "list_portfolios", "response_key": "PortfolioDetails"}
                ]
            },

            # Security
            "awskms": {
                "service_name": "kms",
                "resources": [
                    {"name": "keys", "api_method": "list_keys", "response_key": "Keys"}
                ]
            },
            "AWSSecretsManager": {
                "service_name": "secretsmanager",
                "resources": [
                    {"name": "secrets", "api_method": "list_secrets", "response_key": "SecretList"}
                ]
            },
            "awswaf": {
                "service_name": "wafv2",
                "resources": [
                    # Defaulting to REGIONAL to avoid 'Scope' error. 
                    # If users want CLOUDFRONT scope, they might need a separate service entry or flag.
                    {"name": "web_acls", "api_method": "list_web_acls", "response_key": "WebACLs", "pagination_config": {"Scope": "REGIONAL"}}
                ]
            },
            "AWSShield": {
                "service_name": "shield",
                "resources": [
                    {"name": "protections", "api_method": "list_protections", "response_key": "Protections", "regional": False, "forced_region": "us-east-1"}
                ]
            },
            "AWSFMS": {
                "service_name": "fms",
                "resources": [
                    {"name": "policies", "api_method": "list_policies", "response_key": "PolicyList", "regional": False, "forced_region": "us-east-1"}
                ]
            },

            # Developer Tools
            "AWSCodeCommit": {
                "service_name": "codecommit",
                "resources": [
                    {"name": "repositories", "api_method": "list_repositories", "response_key": "repositories"}
                ]
            },
            "CodeBuild": { # Alias for CodeBuild
                "service_name": "codebuild",
                "resources": [
                    {"name": "projects", "api_method": "list_projects", "response_key": "projects"}
                ]
            },
            "AWSCodeDeploy": {
                "service_name": "codedeploy",
                "resources": [
                    {"name": "applications", "api_method": "list_applications", "response_key": "applications"}
                ]
            },
            "AWSCodePipeline": {
                "service_name": "codepipeline",
                "resources": [
                    {"name": "pipelines", "api_method": "list_pipelines", "response_key": "pipelines"}
                ]
            },
            "AWSCodeArtifact": {
                "service_name": "codeartifact",
                "resources": [
                    {"name": "domains", "api_method": "list_domains", "response_key": "domains"}
                ]
            },
            "AmazonECR": {
                "service_name": "ecr",
                "resources": [
                    {"name": "repositories", "api_method": "describe_repositories", "response_key": "repositories"}
                ]
            },
            "AmazonECRPublic": { # Maps to ecr-public
                "service_name": "ecr-public",
                "resources": [
                    {"name": "repositories", "api_method": "describe_repositories", "response_key": "repositories", "regional": False, "forced_region": "us-east-1"}
                ]
            },
            "AWSXRay": {
                "service_name": "xray",
                "resources": [
                    {"name": "groups", "api_method": "get_groups", "response_key": "Groups"}
                ]
            },
            
            # Others
            "AmazonKinesis": {
                "service_name": "kinesis",
                "resources": [
                    {"name": "streams", "api_method": "list_streams", "response_key": "StreamNames"}
                ]
            },
            "AmazonStates": {
                "service_name": "stepfunctions",
                "resources": [
                    {"name": "state_machines", "api_method": "list_state_machines", "response_key": "stateMachines"}
                ]
            },
            "AmazonES": { # legacy ES / OpenSearch
                "service_name": "opensearch",
                "resources": [
                     {"name": "domains", "api_method": "list_domain_names", "response_key": "DomainNames"}
                ]
            }
        }

        # Handle Case Insensitivity /Aliases via copy if needed, but for now rely on exact keys from Pricing API
        for name, cfg in DEFAULT_REGISTRY.items():
            self._services[name] = ServiceConfig(**cfg)

# Singleton instance
registry = ServiceRegistry()
