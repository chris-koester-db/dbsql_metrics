#!/usr/bin/env python3
"""
Test runner for replace_dashboard_vars.py unit tests.

This script runs the comprehensive unit tests for the dashboard variable replacement script.
"""

import sys
import os
import subprocess

def main():
    """Run the unit tests for replace_dashboard_vars.py."""
    print("🧪 Running unit tests for replace_dashboard_vars.py")
    print("=" * 60)
    
    # Get the project root directory (two levels up from src/test/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    os.chdir(project_root)
    
    # Path to the test file in src/test directory
    test_file = os.path.join(project_root, 'src', 'test', 'test_replace_dashboard_vars.py')
    
    # Run the tests from the project root
    try:
        result = subprocess.run([sys.executable, test_file], 
                              capture_output=False, text=True)
        
        if result.returncode == 0:
            print("\n✅ All tests passed!")
        else:
            print(f"\n❌ Tests failed with return code: {result.returncode}")
            
        return result.returncode
        
    except Exception as e:
        print(f"\n❌ Error running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
