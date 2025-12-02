import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env
env_path = Path(__file__).parent / '.env'
print(f"Loading .env from {env_path}")
load_dotenv(env_path)

try:
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    print("Credentials are VALID.")
    print(f"Account: {identity['Account']}")
    print(f"ARN: {identity['Arn']}")
except Exception as e:
    print("Credentials are INVALID or missing.")
    print(f"Error: {e}")
