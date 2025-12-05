"""
Setup script for AWS Pricing Downloader.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Core dependencies (excluding dev dependencies)
install_requires = [
    "aiohttp>=3.9.0,<4.0.0",
    "aiofiles>=23.2.0,<24.0.0",
    "tenacity>=8.2.0,<9.0.0",
    "pydantic>=2.5.0,<3.0.0",
    "orjson>=3.9.0,<4.0.0",
]

# Development dependencies
dev_requires = [
    "pytest>=7.4.0,<8.0.0",
    "pytest-asyncio>=0.21.0,<0.22.0",
    "pytest-mock>=3.12.0,<4.0.0",
    "pytest-cov>=4.1.0,<5.0.0",
    "black>=23.0.0",
    "flake8>=6.0.0",
    "mypy>=1.0.0",
    "isort>=5.12.0",
]

setup(
    name="aws-pricing-downloader",
    version="2.0.0",
    author="Senior Python Architect",
    author_email="architect@example.com",
    description="Enterprise-grade AWS pricing data downloader",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/aws-pricing-downloader",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=install_requires,
    extras_require={
        "dev": dev_requires,
    },
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "aws-price=aws_pricing_downloader.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)