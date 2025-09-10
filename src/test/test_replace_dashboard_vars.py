#!/usr/bin/env python3
"""
Unit tests for replace_dashboard_vars.py script.

This test suite covers all error scenarios and success cases for the dashboard variable replacement script.
"""

import unittest
import os
import json
import tempfile
import shutil
import subprocess
from unittest.mock import patch, MagicMock, mock_open
import sys

# Add the src directory to the path so we can import the module
# This allows the test to be run from the project root directory
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)  # Go up one level to src/
sys.path.insert(0, src_dir)

from replace_dashboard_vars import get_dab_vars, main


class TestReplaceDashboardVars(unittest.TestCase):
    """Test cases for the replace_dashboard_vars script."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        # Change to temp directory for isolated testing
        os.chdir(self.temp_dir)
        
        # Create a mock databricks.yml file
        self.databricks_yml_content = """
bundle:
  name: test_bundle
targets:
  dev:
    variables:
      catalog:
        default: "test_catalog"
      schema:
        default: "test_schema"
"""
        with open("databricks.yml", "w") as f:
            f.write(self.databricks_yml_content)
        
        # Create a mock dashboard file
        self.dashboard_content = {
            "datasets": [
                {
                    "parameters": [
                        {
                            "keyword": "catalog",
                            "defaultSelection": {
                                "values": {
                                    "values": [{"value": "old_catalog"}]
                                }
                            }
                        },
                        {
                            "keyword": "schema",
                            "defaultSelection": {
                                "values": {
                                    "values": [{"value": "old_schema"}]
                                }
                            }
                        }
                    ]
                }
            ]
        }
        
        # Create src directory and dashboard file
        os.makedirs("src", exist_ok=True)
        with open("src/dbsql_metrics.lvdash.json", "w") as f:
            json.dump(self.dashboard_content, f, indent=2)
    
    def tearDown(self):
        """Clean up after each test method."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir)
    
    @patch('subprocess.run')
    def test_get_dab_vars_success(self, mock_run):
        """Test successful retrieval of Databricks bundle variables."""
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        result = get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(result["catalog"]["value"], "test_catalog")
        self.assertEqual(result["schema"]["value"], "test_schema")
        mock_run.assert_called_once()
    
    @patch('subprocess.run')
    def test_get_dab_vars_missing_databricks_yml(self, mock_run):
        """Test error when databricks.yml is missing."""
        # Remove databricks.yml
        os.remove("databricks.yml")
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
        mock_run.assert_not_called()
    
    @patch('subprocess.run')
    def test_get_dab_vars_missing_dashboard_file(self, mock_run):
        """Test error when dashboard file is missing."""
        # Remove dashboard file
        os.remove("src/dbsql_metrics.lvdash.json")
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
        mock_run.assert_not_called()
    
    @patch('subprocess.run')
    def test_get_dab_vars_profile_not_found(self, mock_run):
        """Test error when profile is not found."""
        # Mock subprocess call with profile error
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "{}"
        mock_result.stderr = "Error: no profile configured"
        mock_run.side_effect = subprocess.CalledProcessError(1, "databricks", mock_result.stdout, mock_result.stderr)
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "INVALID_PROFILE")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_get_dab_vars_invalid_target(self, mock_run):
        """Test error when target is invalid."""
        # Mock subprocess call with target error
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "{}"
        mock_result.stderr = "Error: invalid_target: no such target. Available targets: dev, prod"
        mock_run.side_effect = subprocess.CalledProcessError(1, "databricks", mock_result.stdout, mock_result.stderr)
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("invalid_target", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_get_dab_vars_auth_configuration_error(self, mock_run):
        """Test error when authentication configuration is invalid."""
        # Mock subprocess call with auth error
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "{}"
        mock_result.stderr = "Error: cannot resolve bundle auth configuration"
        mock_run.side_effect = subprocess.CalledProcessError(1, "databricks", mock_result.stdout, mock_result.stderr)
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "INVALID_PROFILE")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_get_dab_vars_dashboard_file_error(self, mock_run):
        """Test error when dashboard file cannot be read."""
        # Mock subprocess call with dashboard file error
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "{}"
        mock_result.stderr = "Error: failed to read serialized dashboard from file_path"
        mock_run.side_effect = subprocess.CalledProcessError(1, "databricks", mock_result.stdout, mock_result.stderr)
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_get_dab_vars_no_variables_section(self, mock_run):
        """Test error when variables section is missing from output."""
        # Mock subprocess call with missing variables
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"bundle": {"name": "test"}})
        mock_run.return_value = mock_result
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_get_dab_vars_json_decode_error(self, mock_run):
        """Test error when JSON output is invalid."""
        # Mock subprocess call with invalid JSON
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "invalid json"
        mock_run.return_value = mock_result
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_get_dab_vars_command_not_found(self, mock_run):
        """Test error when databricks command is not found."""
        # Mock FileNotFoundError for databricks command
        mock_run.side_effect = FileNotFoundError("databricks command not found")
        
        with self.assertRaises(SystemExit) as cm:
            get_dab_vars("dev", "DEFAULT")
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_success(self, mock_run):
        """Test successful main execution."""
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        # Mock command line arguments
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            # Test that main runs without errors
            try:
                main()
                # If we get here, the test passed
                self.assertTrue(True)
            except Exception as e:
                self.fail(f"main() raised an exception: {e}")
    
    @patch('subprocess.run')
    def test_main_missing_catalog_variable(self, mock_run):
        """Test error when catalog variable is missing."""
        # Mock subprocess call with missing catalog
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_missing_schema_variable(self, mock_run):
        """Test error when schema variable is missing."""
        # Mock subprocess call with missing schema
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_empty_catalog_value(self, mock_run):
        """Test error when catalog variable has no value."""
        # Mock subprocess call with empty catalog value
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"default": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_empty_schema_value(self, mock_run):
        """Test error when schema variable has no value."""
        # Mock subprocess call with empty schema value
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"default": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_dashboard_file_not_found(self, mock_run):
        """Test error when dashboard file is not found."""
        # Remove dashboard file
        os.remove("src/dbsql_metrics.lvdash.json")
        
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_invalid_dashboard_json(self, mock_run):
        """Test error when dashboard file contains invalid JSON."""
        # Write invalid JSON to dashboard file
        with open("src/dbsql_metrics.lvdash.json", "w") as f:
            f.write("invalid json content")
        
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_dashboard_missing_datasets(self, mock_run):
        """Test error when dashboard file has no datasets section."""
        # Create dashboard file without datasets
        invalid_dashboard = {"name": "test_dashboard"}
        with open("src/dbsql_metrics.lvdash.json", "w") as f:
            json.dump(invalid_dashboard, f)
        
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            with self.assertRaises(SystemExit) as cm:
                main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_permission_error(self, mock_run):
        """Test error when there's a permission error writing the file."""
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        # Mock permission error when writing
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
                with self.assertRaises(SystemExit) as cm:
                    main()
        
        self.assertEqual(cm.exception.code, 1)
    
    @patch('subprocess.run')
    def test_main_unexpected_error(self, mock_run):
        """Test handling of unexpected errors."""
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "test_catalog"},
                "schema": {"value": "test_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        # Mock unexpected error
        with patch('json.load', side_effect=Exception("Unexpected error")):
            with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
                with self.assertRaises(SystemExit) as cm:
                    main()
        
        self.assertEqual(cm.exception.code, 1)


class TestIntegration(unittest.TestCase):
    """Integration tests that test the full workflow."""
    
    def setUp(self):
        """Set up test fixtures for integration tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        # Change to temp directory for isolated testing
        os.chdir(self.temp_dir)
        
        # Create a complete test environment
        self.setup_test_environment()
    
    def tearDown(self):
        """Clean up after integration tests."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir)
    
    def setup_test_environment(self):
        """Set up a complete test environment with all required files."""
        # Create databricks.yml
        databricks_yml = """
bundle:
  name: test_bundle
targets:
  dev:
    variables:
      catalog:
        default: "test_catalog"
      schema:
        default: "test_schema"
"""
        with open("databricks.yml", "w") as f:
            f.write(databricks_yml)
        
        # Create dashboard file
        os.makedirs("src", exist_ok=True)
        dashboard_content = {
            "datasets": [
                {
                    "parameters": [
                        {
                            "keyword": "catalog",
                            "defaultSelection": {
                                "values": {
                                    "values": [{"value": "old_catalog"}]
                                }
                            }
                        },
                        {
                            "keyword": "schema",
                            "defaultSelection": {
                                "values": {
                                    "values": [{"value": "old_schema"}]
                                }
                            }
                        }
                    ]
                }
            ]
        }
        with open("src/dbsql_metrics.lvdash.json", "w") as f:
            json.dump(dashboard_content, f, indent=2)
    
    @patch('subprocess.run')
    def test_full_workflow_success(self, mock_run):
        """Test the complete successful workflow."""
        # Mock successful subprocess call
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "variables": {
                "catalog": {"value": "new_catalog"},
                "schema": {"value": "new_schema"}
            }
        })
        mock_run.return_value = mock_result
        
        # Run the main function
        with patch('sys.argv', ['replace_dashboard_vars.py', '--target', 'dev', '--profile', 'DEFAULT']):
            # Test that main runs without errors
            try:
                main()
                # If we get here, the test passed
                self.assertTrue(True)
            except Exception as e:
                self.fail(f"main() raised an exception: {e}")
        
        # Verify the dashboard file was updated
        with open("src/dbsql_metrics.lvdash.json", "r") as f:
            updated_dashboard = json.load(f)
        
        catalog_value = updated_dashboard["datasets"][0]["parameters"][0]["defaultSelection"]["values"]["values"][0]["value"]
        schema_value = updated_dashboard["datasets"][0]["parameters"][1]["defaultSelection"]["values"]["values"][0]["value"]
        
        self.assertEqual(catalog_value, "new_catalog")
        self.assertEqual(schema_value, "new_schema")


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add all test cases
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestReplaceDashboardVars))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestIntegration))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
