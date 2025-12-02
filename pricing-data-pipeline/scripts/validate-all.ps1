$ErrorActionPreference = "Stop"

Write-Host "=========================================="
Write-Host "AWS Pricing Data - Complete Validation"
Write-Host "=========================================="
Write-Host ""

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

# Step 1: Run data validation
Write-Host "Step 1/2: Validating data integrity..."
python "$ScriptDir/4-validate.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "❌ Data validation failed. Fix errors before proceeding."
    exit 1
}

Write-Host ""
Write-Host "Step 2/2: Testing application queries..."
python "$ScriptDir/5-test-queries.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "⚠️  Query tests had failures. Review results."
    exit 1
}

Write-Host ""
Write-Host "=========================================="
Write-Host "✓ All validations passed!"
Write-Host "Data is ready for production use"
Write-Host "=========================================="
Write-Host ""
Write-Host "Reports available in: output/validation/"