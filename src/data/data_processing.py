import subprocess
import sys
import os

def run_script(script_path):
    print(f"Running: {script_path}")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

if __name__ == "__main__":
    # Always run from src/data so db and script are in the same folder
    os.chdir(os.path.dirname(__file__))
    # Get project root (three levels up from this file)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    # Find the data folder in the tree from root
    data_folder = os.path.join(project_root, 'data')
    print(f"Project root: {project_root}")
    print(f"Data folder: {data_folder}")
    # 1. Build master crosswalk
    run_script('combine_zip_county_cbsa.py')
    # 2. Build provider master (if needed)
    run_script('provider_hhcahps_master.py')
    # 3. Enrich provider with crosswalk
    run_script('enrich_provider_with_zip_county_cbsa.py')
    print("\nAll data processing steps completed.")
