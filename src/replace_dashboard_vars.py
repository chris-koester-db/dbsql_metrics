"""
Before deploying the bundle, in the base folder, run the Python script to update the dashboard query parameters for the target environment. The target name and CLI profile are required arguments:

Usage:
    # From base directory:
    $ python src/replace_dashboard_vars.py --target dev --profile DEFAULT
    $ python src/replace_dashboard_vars.py --target prod --profile PROD
    
    
Note: Use relative paths (e.g., 'resources/synced_delta_tables.yml') not absolute paths (e.g., '/resources/synced_delta_tables.yml')
"""

import os
import sys
import json
import argparse
import subprocess

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", help="DAB target name (dev, prod, etc.)", required=True)
    parser.add_argument("--profile", help="CLI profile to use", required=True)
    return parser.parse_args()

# Parse arguments only when script is run directly
if __name__ == "__main__":
    args = parse_arguments()
else:
    # For testing, create a default args object
    class Args:
        def __init__(self):
            self.target = "dev"
            self.profile = "DEFAULT"
    args = Args()

def get_dab_vars(target: str, profile: str):
    """Get Databricks bundle variables with comprehensive error handling."""
    try:
        # Get the project root directory where databricks.yml is located
        current_dir = os.getcwd()
        if current_dir.endswith('/src'):
            # If we're in the src directory, go up one level to the project root
            project_root = os.path.abspath(os.path.join(current_dir, os.pardir))
        else:
            # If we're already in the project root, use current directory
            project_root = current_dir
        
        print(f"Project root directory: {project_root}")
        print(f"Current directory: {current_dir}")
        
        # Check if databricks.yml exists in project root
        databricks_yml_path = os.path.join(project_root, "databricks.yml")
        if not os.path.exists(databricks_yml_path):
            print(f"❌ Error: databricks.yml not found in project root: {databricks_yml_path}")
            print("💡 Solution: Make sure you're running this script from a Databricks bundle project directory")
            sys.exit(1)
        
        # Check if dashboard file exists (required for bundle validation)
        dashboard_file_path = os.path.join(project_root, "src", "dbsql_metrics.lvdash.json")
        if not os.path.exists(dashboard_file_path):
            print(f"❌ Error: Dashboard file not found: {dashboard_file_path}")
            print("💡 Solution: Make sure the dashboard file exists in the src/ directory")
            print("   The bundle validation requires this file to be present")
            sys.exit(1)
        
        command = ["databricks", "bundle", "validate", "-o", "json", "-t", target, "-p", profile]
        print(f"Running command: {' '.join(command)}")
        
        # Run the shell command and capture the output
        result = subprocess.run(command, cwd=project_root, capture_output=True, text=True, check=True)
        
        # Remove any whitespace
        json_output = result.stdout.strip()
        
        # Deserialize the JSON string to a Python object
        data = json.loads(json_output)

        if 'variables' not in data:
            print("❌ Error: No 'variables' section found in bundle validation output")
            print("💡 Solution: Check your databricks.yml configuration")
            sys.exit(1)

        return data['variables']
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Databricks CLI command failed with return code {e.returncode}")
        
        # Show only the key error message, not the full JSON output
        if e.stderr and e.stderr.strip():
            # Extract just the error message, not the full JSON
            error_lines = e.stderr.strip().split('\n')
            for line in error_lines:
                if line.strip().startswith('Error:'):
                    print(f"   {line.strip()}")
                    break
        
        # Parse common error patterns and provide specific guidance
        stderr_lower = e.stderr.lower() if e.stderr else ""
        
        if "no profile configured" in stderr_lower or "profile not found" in stderr_lower:
            print(f"\n💡 Solution: Profile '{profile}' not found in your Databricks configuration")
            print("   Run 'databricks auth profiles' to see available profiles")
            print("   Or run 'databricks configure' to set up a new profile")
        elif "cannot resolve bundle auth configuration" in stderr_lower:
            print(f"\n💡 Solution: Authentication issue with profile '{profile}'")
            print("   Run 'databricks auth login' to authenticate")
            print("   Or check your ~/.databrickscfg file for correct profile configuration")
        elif "target" in stderr_lower and ("not found" in stderr_lower or "no such target" in stderr_lower):
            print(f"\n💡 Solution: Target '{target}' not found in your databricks.yml")
            print("   Check your databricks.yml file for available targets")
            # Try to extract available targets from the stderr
            if "Available targets:" in e.stderr:
                available_targets = e.stderr.split("Available targets:")[-1].strip()
                print(f"   Available targets: {available_targets}")
        elif "workspace" in stderr_lower and "not found" in stderr_lower:
            print(f"\n💡 Solution: Workspace configuration issue")
            print("   Check your databricks.yml workspace configuration")
        elif "failed to read serialized dashboard" in stderr_lower or "no such file or directory" in stderr_lower:
            print(f"\n💡 Solution: Dashboard file missing or inaccessible")
            print("   Make sure src/dbsql_metrics.lvdash.json exists and is readable")
            print("   Check file permissions and path")
        
        sys.exit(1)
        
    except FileNotFoundError:
        print("❌ Error: 'databricks' command not found")
        print("💡 Solution: Install Databricks CLI and ensure it's in your PATH")
        print("   Install with: pip install databricks-cli")
        print("   Or visit: https://docs.databricks.com/dev-tools/cli/install.html")
        sys.exit(1)
        
    except json.JSONDecodeError as e:
        print(f"❌ Error: Failed to parse JSON output from Databricks CLI")
        print(f"   JSON Error: {e}")
        print("💡 Solution: Check if the Databricks CLI is working correctly")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("💡 Solution: Check your environment and try again")
        sys.exit(1)

def main():
    """Main function with comprehensive error handling."""
    try:
        # Get catalog and schema from the appropriate target
        print("🔍 Retrieving Databricks bundle variables...")
        dab_vars = get_dab_vars(args.target, args.profile)
        
        # Validate that required variables exist
        if 'catalog' not in dab_vars:
            print("❌ Error: 'catalog' variable not found in bundle configuration")
            print("💡 Solution: Check your databricks.yml variables section")
            sys.exit(1)
            
        if 'schema' not in dab_vars:
            print("❌ Error: 'schema' variable not found in bundle configuration")
            print("💡 Solution: Check your databricks.yml variables section")
            sys.exit(1)
        
        # Extract values safely
        catalog = dab_vars['catalog'].get('value')
        schema = dab_vars['schema'].get('value')
        
        if not catalog:
            print("❌ Error: 'catalog' variable has no value")
            print("💡 Solution: Check your databricks.yml catalog variable configuration")
            sys.exit(1)
            
        if not schema:
            print("❌ Error: 'schema' variable has no value")
            print("💡 Solution: Check your databricks.yml schema variable configuration")
            sys.exit(1)

        print(f"✅ Using target environment: {args.target}")
        print(f"✅ Using catalog: {catalog}")
        print(f"✅ Using schema: {schema}")

        # Read the JSON file
        json_file_path = "src/dbsql_metrics.lvdash.json"
        
        if not os.path.exists(json_file_path):
            print(f"❌ Error: Dashboard file not found: {json_file_path}")
            print("💡 Solution: Make sure the dashboard file exists in the src/ directory")
            sys.exit(1)

        print(f"📖 Reading dashboard file: {json_file_path}")
        with open(json_file_path, 'r') as f:
            dashboard_data = json.load(f)

        # Validate dashboard structure
        if 'datasets' not in dashboard_data:
            print("❌ Error: No 'datasets' section found in dashboard file")
            print("💡 Solution: Check if the dashboard file is properly formatted")
            sys.exit(1)

        # Update catalog and schema values in the parameters
        print("🔄 Updating dashboard parameters...")
        catalog_updated = False
        schema_updated = False
        
        for dataset in dashboard_data.get('datasets', []):
            for param in dataset.get('parameters', []):
                if param.get('keyword') == 'catalog':
                    # Safely navigate the nested structure
                    try:
                        param['defaultSelection']['values']['values'][0]['value'] = catalog
                        print(f"✅ Updated catalog parameter to: {catalog}")
                        catalog_updated = True
                    except (KeyError, IndexError, TypeError) as e:
                        print(f"⚠️  Warning: Could not update catalog parameter - unexpected structure: {e}")
                        
                elif param.get('keyword') == 'schema':
                    # Safely navigate the nested structure
                    try:
                        param['defaultSelection']['values']['values'][0]['value'] = schema
                        print(f"✅ Updated schema parameter to: {schema}")
                        schema_updated = True
                    except (KeyError, IndexError, TypeError) as e:
                        print(f"⚠️  Warning: Could not update schema parameter - unexpected structure: {e}")

        if not catalog_updated:
            print("⚠️  Warning: No catalog parameter found to update")
        if not schema_updated:
            print("⚠️  Warning: No schema parameter found to update")

        # Write the updated JSON back to the file
        print(f"💾 Writing updated dashboard file: {json_file_path}")
        with open(json_file_path, 'w') as f:
            json.dump(dashboard_data, f, indent=2)

        print(f"🎉 Successfully updated {json_file_path} with catalog '{catalog}' and schema '{schema}'")
        
    except json.JSONDecodeError as e:
        print(f"❌ Error: Failed to parse dashboard JSON file")
        print(f"   JSON Error: {e}")
        print("💡 Solution: Check if the dashboard file is valid JSON")
        sys.exit(1)
        
    except PermissionError as e:
        print(f"❌ Error: Permission denied when writing to file")
        print(f"   Error: {e}")
        print("💡 Solution: Check file permissions or run with appropriate privileges")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("💡 Solution: Check your environment and try again")
        sys.exit(1)

if __name__ == "__main__":
    main()
