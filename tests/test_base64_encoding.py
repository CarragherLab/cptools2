"""
Tests for base64 encoding/decoding of rsync commands with special characters.
"""

import base64
import os

import pytest


def test_base64_encoding(tmp_path):
    """Test base64 encoding/decoding of complex rsync commands."""
    test_command = (
        'rsync -s --perms --chmod=a+rwx --files-from="file with spaces.txt" '
        '"/path/with spaces & special chars!" '
        '"/destination/with spaces/and/special\'characters"'
    )

    # Base64 encode the command
    encoded_command = base64.b64encode(test_command.encode()).decode()

    # Write encoded command to a temporary file
    cmd_file = os.path.join(str(tmp_path), "test_command.txt")
    with open(cmd_file, "w") as f:
        f.write(encoded_command)

    # Decode in Python (cross-platform)
    decoded = base64.b64decode(encoded_command.encode()).decode()
    assert decoded == test_command


SPECIAL_CHAR_PATHS = [
    "Spaces in path",
    "Path with & ampersand",
    "Path with ' single quotes",
    'Path with " double quotes',
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
    "Path with #*~!@%^*()_+-={}[]|\\:;\"'<>,.?/ all special chars",
]


@pytest.mark.parametrize("path", SPECIAL_CHAR_PATHS)
def test_complex_characters(path):
    """Test encoding/decoding with a wide range of special characters."""
    original = f'rsync -avz "/source/{path}" "/dest/{path}"'
    encoded = base64.b64encode(original.encode()).decode()
    decoded = base64.b64decode(encoded).decode()
    assert original == decoded
