"""
Data normalization utilities for hostname matching and data cleaning.
"""

import re
from typing import Any, Dict, List, Optional


def normalize_hostname(hostname: Optional[str], uppercase: bool = True) -> str:
    """
    Normalize a hostname for consistent matching.
    
    Args:
        hostname: The hostname to normalize
        uppercase: Whether to convert to uppercase
        
    Returns:
        Normalized hostname string
    """
    if not hostname:
        return ''
    
    normalized = str(hostname).strip()
    
    normalized = normalized.split('.')[0]
    
    normalized = re.sub(r'[^\w\-]', '', normalized)
    
    if uppercase:
        normalized = normalized.upper()
    
    return normalized


def normalize_ip(ip: Optional[str]) -> str:
    """
    Normalize an IP address for matching.
    
    Args:
        ip: The IP address to normalize
        
    Returns:
        Normalized IP string
    """
    if not ip:
        return ''
    
    ip = str(ip).strip()
    
    parts = ip.split(',')
    if parts:
        ip = parts[0].strip()
    
    return ip


def clean_string(value: Any) -> str:
    """Clean and convert a value to string."""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'Yes' if value else 'No'
    if isinstance(value, (list, dict)):
        return str(value)
    return str(value).strip()


def normalize_workloads(
    workloads: List[Dict[str, Any]], 
    uppercase_hostname: bool = True
) -> List[Dict[str, Any]]:
    """
    Normalize all workloads for consistent data format.
    
    Args:
        workloads: List of workload dictionaries
        uppercase_hostname: Whether to uppercase hostnames
        
    Returns:
        List of normalized workload dictionaries
    """
    normalized = []
    
    for workload in workloads:
        hostname = workload.get('hostname', '')
        
        workload['hostname_normalized'] = normalize_hostname(hostname, uppercase_hostname)
        workload['primary_ip_normalized'] = normalize_ip(workload.get('primary_ip', ''))
        
        normalized.append(workload)
    
    return normalized


def normalize_servers(
    servers: List[Dict[str, Any]], 
    uppercase_hostname: bool = True
) -> List[Dict[str, Any]]:
    """
    Normalize all servers for consistent data format.
    
    Args:
        servers: List of server dictionaries
        uppercase_hostname: Whether to uppercase hostnames
        
    Returns:
        List of normalized server dictionaries
    """
    normalized = []
    
    for server in servers:
        hostname = server.get('name', '') or server.get('hostname', '')
        
        server['hostname_normalized'] = normalize_hostname(hostname, uppercase_hostname)
        server['ip_normalized'] = normalize_ip(server.get('ip_address', ''))
        
        normalized.append(server)
    
    return normalized


def extract_unique_labels(workloads: List[Dict[str, Any]]) -> Dict[str, set]:
    """
    Extract all unique label keys and values from workloads.
    
    Args:
        workloads: List of workload dictionaries
        
    Returns:
        Dictionary mapping label keys to sets of values
    """
    labels = {}
    
    for workload in workloads:
        for key, value in workload.items():
            if key.startswith('label_') and value:
                label_key = key[6:]
                if label_key not in labels:
                    labels[label_key] = set()
                labels[label_key].add(str(value))
    
    return labels


def extract_unique_values(
    data: List[Dict[str, Any]], 
    field: str
) -> List[str]:
    """
    Extract unique non-empty values for a field.
    
    Args:
        data: List of dictionaries
        field: Field name to extract
        
    Returns:
        Sorted list of unique values
    """
    values = set()
    
    for item in data:
        value = item.get(field, '')
        if value:
            values.add(str(value))
    
    return sorted(values)
