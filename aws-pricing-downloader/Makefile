.PHONY: help install install-dev test test-cov lint format clean build docker-build docker-run run-from-file

# Variables
PYTHON := python3
PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest
DOCKER_IMAGE := aws-pricing-downloader
DOCKER_TAG := 2.0.0

help:
	@echo "AWS Pricing Downloader v2.0 - Makefile Commands"
	@echo ""
	@echo "Development:"
	@echo "  install        Install package and dependencies"
	@echo "  install-dev    Install package with dev dependencies"
	@echo "  test           Run tests"
	@echo "  test-cov       Run tests with coverage report"
	@echo "  lint           Run linters"
	@echo "  format         Format code"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Build & Run:"
	@echo "  build          Build package"
	@echo "  docker-build   Build Docker image"
	@echo "  docker-run     Run Docker container"
	@echo "  run            Run downloader locally (all services)"
	@echo "  run-services   Run downloader for specific services"
	@echo "  run-from-file  Run downloader from services.txt"
	@echo ""

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"
	$(PIP) install black flake8 mypy isort aiofiles

test:
	$(PYTEST) tests/ -v

test-cov:
	$(PYTEST) tests/ --cov=aws_pricing_downloader --cov-report=html --cov-report=term

lint:
	flake8 aws_pricing_downloader/ tests/ --max-line-length=100
	mypy aws_pricing_downloader/ --ignore-missing-imports

format:
	black aws_pricing_downloader/ tests/ --line-length=100
	isort aws_pricing_downloader/ tests/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf data/
	rm -rf metrics/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

build: clean
	$(PYTHON) -m build


docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):latest .


docker-run:
	docker run --rm \
		-v $(PWD)/data:/data/aws_pricing \
		-v $(PWD)/metrics:/metrics \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

run:
	aws-price download

run-services:
	aws-price download --services AmazonEC2 AmazonS3 AWSLambda

run-from-file:
	@if [ -f services.txt ]; then \
		aws-price download --services $$(cat services.txt); \
	else \
		echo "Error: services.txt not found"; \
		exit 1; \
	fi

run-debug:
	aws-price download --log-level DEBUG --log-file logs/download.log
