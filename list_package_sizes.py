import os
import subprocess
from pathlib import Path

# Adjust the path to your environment's site-packages directory
site_packages_path = Path('venv/lib64/python3.10/site-packages')

# Ensure the script uses the correct Python interpreter from the virtual environment
pip_path = 'venv/bin/pip'

# Get a list of installed packages using pip
installed_packages = subprocess.run([pip_path, 'list'], capture_output=True, text=True)

# Process the output from 'pip list' to calculate sizes
for line in installed_packages.stdout.split('\n')[2:-1]:  # Skip the header lines and last empty line
    package_name, version = line.split()[:2]
    # Some packages may install into directories that don't match the package name exactly
    # This attempts to find the directory by package name, adjusting for common patterns
    possible_dirs = [d for d in site_packages_path.iterdir() if d.is_dir() and package_name in d.name]
    total_size = sum(sum(f.stat().st_size for f in pd.glob('**/*') if f.is_file()) for pd in possible_dirs)
    print(f"{package_name}: {total_size / (1024 * 1024):.2f} MB")
