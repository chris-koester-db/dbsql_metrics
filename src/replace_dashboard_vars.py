import json
import argparse
import subprocess
import os

# Add parser for command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--target", help="DAB target name (dev, prod, etc.)", required=True)
parser.add_argument("--profile", help="CLI profile to use", required=True)

args = parser.parse_args()

def get_dab_vars(target:str, profile:str):
    # Get parent directory, which is where we need to run the Databricks CLI
    current_dir = os.getcwd()
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    
    command = ["databricks", "bundle", "validate", "-o", "json", "-t", target, "-p", profile]
    
    # Run the shell command and capture the output
    result = subprocess.run(command, cwd=parent_dir, capture_output=True, text=True, check=True)
    
    # Remove any whitespace
    json_output = result.stdout.strip()
    
    # Deserialize the JSON string to a Python object
    data = json.loads(json_output)

    return data['variables']

# Get catalog and schema from the appropriate target
dab_vars = get_dab_vars(args.target, args.profile)
catalog = dab_vars['catalog']['value']
schema = dab_vars['schema']['value']

print(f"Using target environment: {args.target}")
print(f"Using catalog: {catalog}")
print(f"Using schema: {schema}")

# Read the JSON file
json_file_path = "dbsql_metrics.lvdash.json"

with open(json_file_path, 'r') as f:
    dashboard_data = json.load(f)

# Update catalog and schema values in the parameters
for dataset in dashboard_data.get('datasets', []):
    for param in dataset.get('parameters', []):
        if param.get('keyword') == 'catalog':
            param['defaultSelection']['values']['values'][0]['value'] = catalog
            print(f"Updated catalog parameter to: {catalog}")
        elif param.get('keyword') == 'schema':
            param['defaultSelection']['values']['values'][0]['value'] = schema
            print(f"Updated schema parameter to: {schema}")

# Write the updated JSON back to the file
with open(json_file_path, 'w') as f:
    json.dump(dashboard_data, f, indent=2)

print(f"Successfully updated {json_file_path} with catalog '{catalog}' and schema '{schema}'")
