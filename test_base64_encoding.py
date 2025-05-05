#!/usr/bin/env python
"""
Test script to verify base64 encoding/decoding works correctly for rsync commands
with special characters in paths.

This script:
1. Creates a complex rsync command with spaces and special characters
2. Encodes it using base64
3. Writes it to a temporary file
4. Generates a shell script to decode and print the command
5. Executes the shell script and verifies the output matches the original

Run this script on Eddie to test the implementation.
"""

import os
import base64
import subprocess
import tempfile
import shutil

def test_base64_encoding():
    """Test base64 encoding/decoding of complex rsync commands"""
    
    # Create a temporary directory for test files
    temp_dir = tempfile.mkdtemp()
    try:
        # Create a complex rsync command with spaces and special characters
        test_command = 'rsync -s --perms --chmod=a+rwx --files-from="file with spaces.txt" ' \
                       '"/path/with spaces & special chars!" ' \
                       '"/destination/with spaces/and/special\'characters"'
        
        print(f"Original command:\n{test_command}\n")
        
        # Base64 encode the command
        encoded_command = base64.b64encode(test_command.encode()).decode()
        print(f"Base64 encoded command:\n{encoded_command}\n")
        
        # Write encoded command to a temporary file
        cmd_file = os.path.join(temp_dir, "test_command.txt")
        with open(cmd_file, "w") as f:
            f.write(encoded_command)
        
        # Create a shell script to decode and echo the command
        shell_script = os.path.join(temp_dir, "test_decode.sh")
        with open(shell_script, "w") as f:
            f.write("""#!/bin/bash
ENCODED_SEED=$(cat "{cmd_file}")
SEED=$(echo "$ENCODED_SEED" | base64 --decode)
echo "Decoded command:"
echo "$SEED"
""".format(cmd_file=cmd_file))
        
        # Make the shell script executable
        os.chmod(shell_script, 0o755)
        
        # Execute the shell script
        print("Executing test decode script...")
        result = subprocess.run([shell_script], capture_output=True, text=True)
        
        # Print and check the result
        print(result.stdout)
        
        if test_command in result.stdout:
            print("SUCCESS: Original command was correctly recovered after base64 encoding/decoding")
            return True
        else:
            print("FAILURE: Command was not correctly recovered")
            print(f"Expected: {test_command}")
            print(f"Got: {result.stdout}")
            return False
            
    finally:
        # Clean up
        shutil.rmtree(temp_dir)

def test_complex_characters():
    """Test encoding/decoding with a wider range of special characters"""
    
    special_chars = [
        "Spaces in path",
        "Path with & ampersand",
        "Path with ' single quotes",
        "Path with \" double quotes",
        "Path with | pipes",
        "Path with > redirect",
        "Path with ; semicolon",
        "Path with $ dollar sign",
        "Path with ` backtick",
        "Path with () parentheses",
        "Path with [] brackets",
        "Path with {} braces",
        "Path with tab\tcharacter",
        "Path with newline\ncharacter",
        "Path with #*~!@%^*()_+-={}[]|\\:;\"'<>,.?/ all special chars"
    ]
    
    print("\nTesting complex character handling:")
    
    success = True
    for path in special_chars:
        # Create a command with the special path
        original = f'rsync -avz "/source/{path}" "/dest/{path}"'
        
        # Encode and decode
        encoded = base64.b64encode(original.encode()).decode()
        decoded = base64.b64decode(encoded).decode()
        
        # Check if they match
        if original == decoded:
            print(f"✓ Success: {path}")
        else:
            print(f"✗ Failed: {path}")
            print(f"  Original: {original}")
            print(f"  Decoded:  {decoded}")
            success = False
    
    return success

if __name__ == "__main__":
    print("=== Testing Base64 Encoding for rsync Commands ===\n")
    
    # Test basic functionality
    basic_success = test_base64_encoding()
    
    # Test complex character handling
    complex_success = test_complex_characters()
    
    # Final result
    if basic_success and complex_success:
        print("\nALL TESTS PASSED! The base64 encoding solution works correctly.")
    else:
        print("\nSome tests FAILED. Please review the output above.")
