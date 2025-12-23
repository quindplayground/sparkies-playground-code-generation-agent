#!/usr/bin/env python3
"""Check if all imports work correctly."""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

errors = []

# Test imports
try:
    from template_project.flows.portafolios_dynamodb.extract import extract
    print("✓ extract import OK")
except Exception as e:
    errors.append(f"extract import failed: {e}")
    print(f"✗ extract import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.transform import transform
    print("✓ transform import OK")
except Exception as e:
    errors.append(f"transform import failed: {e}")
    print(f"✗ transform import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.load import load
    print("✓ load import OK")
except Exception as e:
    errors.append(f"load import failed: {e}")
    print(f"✗ load import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.job import portafolios_dynamodb_job
    print("✓ job import OK")
except Exception as e:
    errors.append(f"job import failed: {e}")
    print(f"✗ job import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.steps.step_100_filter_active_records import step_100_filter_active_records
    print("✓ step_100 import OK")
except Exception as e:
    errors.append(f"step_100 import failed: {e}")
    print(f"✗ step_100 import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.steps.step_200_group_and_aggregate import step_200_group_and_aggregate
    print("✓ step_200 import OK")
except Exception as e:
    errors.append(f"step_200 import failed: {e}")
    print(f"✗ step_200 import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.steps.step_300_exclude_columns import step_300_exclude_columns
    print("✓ step_300 import OK")
except Exception as e:
    errors.append(f"step_300 import failed: {e}")
    print(f"✗ step_300 import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.steps.step_400_build_keys import step_400_build_keys
    print("✓ step_400 import OK")
except Exception as e:
    errors.append(f"step_400 import failed: {e}")
    print(f"✗ step_400 import failed: {e}")

try:
    from template_project.flows.portafolios_dynamodb.steps.step_500_finalize import step_500_finalize
    print("✓ step_500 import OK")
except Exception as e:
    errors.append(f"step_500 import failed: {e}")
    print(f"✗ step_500 import failed: {e}")

if errors:
    print(f"\n{len(errors)} import error(s) found")
    sys.exit(1)
else:
    print("\nAll imports OK!")
    sys.exit(0)
