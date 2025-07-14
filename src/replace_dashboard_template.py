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

# Read the template file
template_path = "dbsql_metrics_template.lvdash.json"
output_path = "dbsql_metrics.lvdash.json"

with open(template_path, 'r') as f:
    template_content = f.read()

# Replace placeholders with actual values
updated_content = template_content.replace("CATALOG_NAME", catalog)
updated_content = updated_content.replace("SCHEMA_NAME", schema)

# Write the updated content to the output file
with open(output_path, 'w') as f:
    f.write(updated_content)

print(f"Successfully created {output_path} with catalog '{catalog}' and schema '{schema}'")
