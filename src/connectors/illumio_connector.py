"""
Illumio PCE API connector for extracting workload data.
Supports async pagination for large-scale deployments (100k+ workloads).
"""

import asyncio
import base64
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base_connector import BaseAsyncConnector


class IllumioConnector(BaseAsyncConnector):
    """
    Async connector for Illumio PCE API.
    Extracts workloads, labels, and related metadata.
    """
    
    def __init__(
        self,
        pce_url: str,
        org_id: str,
        api_user: str,
        api_secret: str,
        port: int = 8443,
        verify_ssl: bool = True,
        page_size: int = 500,
        max_concurrent_requests: int = 15,
        timeout: int = 30,
        max_retries: int = 3,
        initial_delay: float = 1,
        backoff_multiplier: float = 2,
        max_delay: float = 60
    ):
        base_url = f"{pce_url}:{port}/api/v2/orgs/{org_id}"
        
        super().__init__(
            base_url=base_url,
            max_concurrent_requests=max_concurrent_requests,
            timeout=timeout,
            max_retries=max_retries,
            initial_delay=initial_delay,
            backoff_multiplier=backoff_multiplier,
            max_delay=max_delay,
            verify_ssl=verify_ssl
        )
        
        self.api_user = api_user
        self.api_secret = api_secret
        self.page_size = page_size
        self.org_id = org_id
        
        self._labels_cache: Dict[str, Dict[str, str]] = {}
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Generate Basic Auth headers for Illumio API."""
        credentials = f"{self.api_user}:{self.api_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        
        return {
            'Authorization': f'Basic {encoded}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    async def test_connection(self) -> bool:
        """Test connection to the PCE."""
        try:
            await self._create_session()
            response = await self._request_with_retry(
                'GET',
                f"{self.base_url}/workloads",
                params={'max_results': 1}
            )
            return response is not None
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    async def fetch_labels(self) -> Dict[str, Dict[str, str]]:
        """
        Fetch all labels from PCE and cache them for workload enrichment.
        
        Returns:
            Dictionary mapping label href to {key, value}
        """
        self.logger.info("Fetching labels from PCE...")
        
        labels = await self._paginated_fetch(
            endpoint="/labels",
            page_size=self.page_size,
            offset_param="offset",
            limit_param="max_results"
        )
        
        self._labels_cache = {
            label['href']: {
                'key': label.get('key', ''),
                'value': label.get('value', '')
            }
            for label in labels
        }
        
        self.logger.info(f"Fetched {len(self._labels_cache)} labels")
        return self._labels_cache
    
    async def fetch_all_data(self) -> List[Dict[str, Any]]:
        """
        Fetch all workloads from PCE with full details.
        
        Returns:
            List of workload dictionaries with enriched label data
        """
        await self.fetch_labels()
        
        self.logger.info("Fetching workloads from PCE...")
        
        workloads = await self._paginated_fetch(
            endpoint="/workloads",
            page_size=self.page_size,
            offset_param="offset",
            limit_param="max_results"
        )
        
        self.logger.info(f"Fetched {len(workloads)} workloads. Enriching data...")
        
        enriched_workloads = [
            self._enrich_workload(workload) 
            for workload in workloads
        ]
        
        return enriched_workloads
    
    def _enrich_workload(self, workload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich workload data with resolved labels and flattened structure.
        
        Args:
            workload: Raw workload data from API
            
        Returns:
            Enriched workload dictionary
        """
        labels_raw = workload.get('labels', [])
        resolved_labels = {}
        
        for label_ref in labels_raw:
            href = label_ref.get('href', '')
            if href in self._labels_cache:
                label_info = self._labels_cache[href]
                resolved_labels[label_info['key']] = label_info['value']
        
        interfaces = workload.get('interfaces', [])
        primary_ip = ''
        all_ips = []
        
        for iface in interfaces:
            ip = iface.get('address', '')
            if ip:
                all_ips.append(ip)
                if not primary_ip:
                    primary_ip = ip
        
        agent = workload.get('agent', {}) or {}
        agent_config = agent.get('config', {}) or {}
        agent_status = agent.get('status', {}) or {}
        
        enriched = {
            'href': workload.get('href', ''),
            'name': workload.get('name', ''),
            'hostname': workload.get('hostname', ''),
            'hostname_normalized': (workload.get('hostname', '') or '').upper(),
            'description': workload.get('description', ''),
            'distinguished_name': workload.get('distinguished_name', ''),
            
            'primary_ip': primary_ip,
            'all_ips': ', '.join(all_ips),
            'public_ip': workload.get('public_ip', ''),
            'interfaces_count': len(interfaces),
            
            'online': workload.get('online', False),
            'managed': workload.get('managed', False),
            'enforcement_mode': workload.get('enforcement_mode', ''),
            'visibility_level': workload.get('visibility_level', ''),
            
            'agent_href': agent.get('href', ''),
            'agent_status': agent_status.get('status', ''),
            'agent_version': agent_status.get('agent_version', ''),
            'agent_last_heartbeat': agent_status.get('last_heartbeat_on', ''),
            'agent_mode': agent_config.get('mode', ''),
            'agent_visibility_level': agent_config.get('visibility_level', ''),
            'agent_log_traffic': agent_config.get('log_traffic', False),
            
            'ven_version': agent_status.get('agent_version', ''),
            'ven_status': self._determine_ven_status(workload),
            
            'os_type': workload.get('os_type', ''),
            'os_id': workload.get('os_id', ''),
            'os_detail': workload.get('os_detail', ''),
            'service_principal_name': workload.get('service_principal_name', ''),
            
            'data_center': workload.get('data_center', ''),
            'data_center_zone': workload.get('data_center_zone', ''),
            
            'firewall_coexistence': workload.get('firewall_coexistence', {}).get('illumio_primary', None),
            'containers_inherit_host_policy': workload.get('containers_inherit_host_policy', None),
            'blocked_connection_action': workload.get('blocked_connection_action', ''),
            
            'vulnerability_exposure_score': workload.get('vulnerability_exposure_score', ''),
            'vulnerability_summary': str(workload.get('vulnerability_summary', {})),
            
            'created_at': workload.get('created_at', ''),
            'updated_at': workload.get('updated_at', ''),
            'created_by': workload.get('created_by', {}).get('href', ''),
            'deleted': workload.get('deleted', False),
            'delete_type': workload.get('delete_type', ''),
            
            'caps': ', '.join(workload.get('caps', [])),
            
            'labels_raw': str(labels_raw),
            'labels_resolved': str(resolved_labels),
            
            'label_role': resolved_labels.get('role', ''),
            'label_app': resolved_labels.get('app', ''),
            'label_env': resolved_labels.get('env', ''),
            'label_loc': resolved_labels.get('loc', ''),
        }
        
        for key, value in resolved_labels.items():
            if key not in ('role', 'app', 'env', 'loc'):
                enriched[f'label_{key}'] = value
        
        return enriched
    
    def _determine_ven_status(self, workload: Dict[str, Any]) -> str:
        """
        Determine the overall VEN status for a workload.
        
        Returns one of: active, offline, suspended, uninstalled, unmanaged
        """
        if not workload.get('managed', False):
            return 'unmanaged'
        
        agent = workload.get('agent', {}) or {}
        agent_status = agent.get('status', {}) or {}
        status = agent_status.get('status', '')
        
        if status:
            return status.lower()
        
        if workload.get('online', False):
            return 'active'
        else:
            return 'offline'
    
    async def fetch_workload_count(self) -> int:
        """Get total count of workloads without fetching all data."""
        response = await self._request_with_retry(
            'GET',
            f"{self.base_url}/workloads",
            params={'max_results': 1}
        )
        
        if response:
            return len(response)
        return 0
    
    async def get_pce_health(self) -> Dict[str, Any]:
        """Get PCE health status information."""
        response = await self._request_with_retry(
            'GET',
            f"{self.base_url}/health"
        )
        return response or {}


async def create_illumio_connector(config) -> IllumioConnector:
    """Factory function to create an Illumio connector from config."""
    return IllumioConnector(
        pce_url=config.illumio.pce_url,
        org_id=config.illumio.org_id,
        api_user=config.illumio.api_user,
        api_secret=config.illumio.api_secret,
        port=config.illumio.port,
        verify_ssl=config.illumio.verify_ssl,
        page_size=config.illumio.page_size,
        max_concurrent_requests=config.illumio.max_concurrent_requests,
        timeout=config.illumio.timeout,
        max_retries=config.retry.max_attempts,
        initial_delay=config.retry.initial_delay,
        backoff_multiplier=config.retry.backoff_multiplier,
        max_delay=config.retry.max_delay
    )
