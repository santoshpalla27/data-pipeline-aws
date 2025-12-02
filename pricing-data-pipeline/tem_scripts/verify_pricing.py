import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

print("Checking Pricing Service access...")
try:
    pricing = boto3.client('pricing', region_name='us-east-1')
    response = pricing.describe_services(ServiceCode='AmazonEC2')
    print("Pricing Service is ACCESSIBLE.")
    print(f"Service: {response['Services'][0]['ServiceCode']}")
except Exception as e:
    print("Pricing Service is NOT accessible.")
    print(f"Error: {e}")
