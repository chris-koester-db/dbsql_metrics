import json
import yaml
import os

# Read the databricks.yml file to get catalog and schema
databricks_yml_path = "../databricks.yml"

with open(databricks_yml_path, 'r') as f:
    databricks_config = yaml.safe_load(f)

# Determine the target environment (default to 'dev' if not specified)
# You can modify this logic based on how you determine the environment
target_env = "dev"  # Change this to "prod" for production environment

# Get catalog and schema from the appropriate target
target_config = databricks_config['targets'][target_env]
catalog = target_config['variables']['catalog']
schema = target_config['variables']['schema']

print(f"Using target environment: {target_env}")
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
