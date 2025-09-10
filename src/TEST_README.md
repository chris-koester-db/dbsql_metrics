# Unit Tests for replace_dashboard_vars.py

This directory contains comprehensive unit tests for the `replace_dashboard_vars.py` script.

## Test Coverage

The test suite covers all major error scenarios and success cases:

### Error Scenarios Tested:
- ❌ Missing `databricks.yml` file
- ❌ Missing dashboard file (`src/dbsql_metrics.lvdash.json`)
- ❌ Invalid Databricks CLI profile
- ❌ Invalid target environment
- ❌ Authentication configuration errors
- ❌ Dashboard file read errors
- ❌ Missing variables in bundle output
- ❌ Invalid JSON in dashboard file
- ❌ Missing datasets section in dashboard
- ❌ Permission errors when writing files
- ❌ Databricks CLI command not found
- ❌ Missing catalog/schema variables
- ❌ Empty catalog/schema values

### Success Scenarios Tested:
- ✅ Successful variable retrieval and dashboard update
- ✅ Complete workflow integration test

## Running the Tests

### Using the test runner (recommended)
```bash
# From the project root directory
python run_tests.py
```

### Alternative: Running directly
```bash
# From the project root directory
python src/test_replace_dashboard_vars.py
```

### Using unittest module
```bash
# From the project root directory
python -m unittest src.test_replace_dashboard_vars -v
```

## Test Structure

- **TestReplaceDashboardVars**: Unit tests for individual functions
- **TestIntegration**: Integration tests for the complete workflow

## Mocking

The tests use extensive mocking to:
- Mock subprocess calls to Databricks CLI
- Mock file system operations
- Mock command line arguments
- Create isolated test environments

## Test Data

Each test creates its own temporary directory with:
- Mock `databricks.yml` file
- Mock dashboard JSON file
- Proper directory structure

This ensures tests don't interfere with each other or the actual project files.

## Expected Output

When all tests pass, you should see:
```
✅ All tests passed!
```

When tests fail, you'll see detailed error information and a summary of which tests failed.
