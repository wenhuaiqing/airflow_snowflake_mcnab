"""
Utility functions for file operations and checksums
"""

import hashlib
import os
from typing import List, Dict, Tuple


def get_all_files(directory: str) -> List[str]:
    """
    Get all files in a directory recursively
    
    Args:
        directory: Path to directory
        
    Returns:
        List of file paths
    """
    files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files


def get_all_checksums(files: List[str]) -> Dict[str, str]:
    """
    Calculate MD5 checksums for all files
    
    Args:
        files: List of file paths
        
    Returns:
        Dictionary mapping file paths to checksums
    """
    checksums = {}
    for file_path in files:
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                content = f.read()
                checksums[file_path] = hashlib.md5(content).hexdigest()
    return checksums


def compare_checksums(checksums1: Dict[str, str], checksums2: Dict[str, str]) -> Tuple[bool, List[str]]:
    """
    Compare two sets of checksums
    
    Args:
        checksums1: First set of checksums
        checksums2: Second set of checksums
        
    Returns:
        Tuple of (are_equal, list_of_differences)
    """
    differences = []
    
    # Check for files in checksums1 but not in checksums2
    for file_path, checksum in checksums1.items():
        if file_path not in checksums2:
            differences.append(f"File {file_path} missing in second set")
        elif checksums2[file_path] != checksum:
            differences.append(f"Checksum mismatch for {file_path}")
    
    # Check for files in checksums2 but not in checksums1
    for file_path in checksums2:
        if file_path not in checksums1:
            differences.append(f"File {file_path} missing in first set")
    
    return len(differences) == 0, differences
