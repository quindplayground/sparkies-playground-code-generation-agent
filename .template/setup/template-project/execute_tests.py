#!/usr/bin/env python3
"""Script to execute tests and capture output."""
import sys
import subprocess
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

result = subprocess.run(
    [
        sys.executable,
        "-m",
        "pytest",
        "template_project_tests/flows/portafolios_dynamodb/",
        "-v",
        "--tb=short",
    ],
    capture_output=True,
    text=True,
)

print(result.stdout)
if result.stderr:
    print("STDERR:", file=sys.stderr)
    print(result.stderr, file=sys.stderr)

sys.exit(result.returncode)
