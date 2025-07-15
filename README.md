# dbsql_metrics

The 'dbsql_metrics' project demonstrates how to use [**Databricks Asset Bundles**](https://docs.databricks.com/aws/en/dev-tools/bundles/) with [**Unity Catalog Metric Views**](https://docs.databricks.com/aws/en/metric-views/) to create a end-to-end analytics solution on Databricks. This project showcases best practices for organizing, deploying, and managing Databricks SQL resources using the modern Asset Bundle framework for data warehouse workload.


## Prerequisites

### 1. Install the Databricks CLI
Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/install.html

### 2. Authenticate to your Databricks workspace
```bash
$ databricks configure
```

### 3. Set up Python Virtual Environment
Create and activate a Python virtual environment to manage dependencies:

```bash
# Create virtual environment
$ python -m venv venv

# Activate virtual environment
# On macOS/Linux:
$ source venv/bin/activate
# On Windows:
$ venv\Scripts\activate

# Install required Python packages
$ pip install PyYAML
```

### 4. Configure databricks.yml Variables
Update the variables in `databricks.yml` to match your environment. The dev target defaults to catalog `users` and a schema based on on the developer's name.

- **catalog**: The catalog name where your tables will be created
- **schema**: The schema name within the catalog
- **warehouse_id**: ID of your SQL warehouse for production deployment. For development, the bundle will lookup the ID based on the specified name (Eg, Shared Serverless).
- **workspace.host**: Your Databricks workspace URL

Example configuration for dev target:
```yaml
targets:
  prod:
    mode: development
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com
    variables:
      warehouse_id: your_warehouse_id
      catalog: your_catalog
      schema: your_schema
```

### 5. Update Dashboard Configuration
Before deploying the bundle, run the Python script to update the dashboard query parameters for the target environment. The target name is provided as an argument (dev, prod, etc.):

```bash
$ cd src
$ python replace_dashboard_vars.py dev
```

This script:
- Resolves DAB variables for the specified target
- Updates the existing dashboard file (`dbsql_metrics.lvdash.json`)
- Replaces the catalog and schema parameter values
- Preserves all other dashboard configuration settings

## Deployment

### Deploy to Development Environment
```bash
$ databricks bundle deploy --target dev
```
(Note that "dev" is the default target, so the `--target` parameter is optional here.)

This deploys everything that's defined for this project, including:
- A job called `[dev yourname] dbsql_metrics_job`
- SQL metric views and dashboards
- All associated resources

You can find the deployed job by opening your workspace and clicking on **Workflows**.

### Deploy to Production Environment
```bash
$ databricks bundle deploy --target prod
```

### Run a Job
```bash
$ databricks bundle run
```

## Development Tools

For enhanced development experience, consider installing:
- Databricks extension for Visual Studio Code: https://docs.databricks.com/dev-tools/vscode-ext.html

## Documentation

For comprehensive documentation on:
- **Databricks Asset Bundles**: https://docs.databricks.com/dev-tools/bundles/index.html
- **CI/CD configuration**: https://docs.databricks.com/dev-tools/bundles/index.html

## Project Structure

- `src/`: Source files including notebooks and dashboard templates
- `resources/`: Bundle resource definitions (jobs, dashboards)
- `databricks.yml`: Main bundle configuration file
- `src/replace_dashboard_vars.py`: Script to update dashboard configuration with catalog and schema values
