"""
ServiceNow CMDB API connector for extracting server inventory.
Supports async pagination for large-scale environments (100k+ servers).
"""

import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from .base_connector import BaseAsyncConnector


class ServiceNowConnector(BaseAsyncConnector):
    """
    Async connector for ServiceNow Table API.
    Extracts server records from CMDB with configurable filtering.
    """
    
    def __init__(
        self,
        instance_url: str,
        api_user: str,
        api_key: str,
        table: str = "cmdb_ci_server",
        verify_ssl: bool = True,
        page_size: int = 10000,
        max_concurrent_requests: int = 10,
        timeout: int = 60,
        max_retries: int = 3,
        initial_delay: float = 1,
        backoff_multiplier: float = 2,
        max_delay: float = 60,
        operating_entity_filter: Optional[str] = None
    ):
        base_url = f"{instance_url}/api/now/table"
        
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
        
        self.instance_url = instance_url.rstrip('/')
        self.api_user = api_user
        self.api_key = api_key
        self.table = table
        self.page_size = page_size
        self.operating_entity_filter = operating_entity_filter
        
        self._discovered_fields: List[str] = []
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Generate headers for ServiceNow API authentication."""
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def _get_basic_auth_headers(self) -> Dict[str, str]:
        """Alternative: Basic Auth headers if using username/password."""
        import base64
        credentials = f"{self.api_user}:{self.api_key}"
        encoded = base64.b64encode(credentials.encode()).decode()
        
        return {
            'Authorization': f'Basic {encoded}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    async def _create_session(self):
        """Override to use Basic Auth if api_user looks like a username."""
        if self._session is None or self._session.closed:
            import aiohttp
            import ssl
            
            if '@' not in self.api_user and len(self.api_key) < 100:
                headers = self._get_basic_auth_headers()
            else:
                headers = self._get_auth_headers()
            
            # Create SSL context based on verify_ssl setting
            if self.verify_ssl:
                ssl_context = None  # Use default SSL verification
            else:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent_requests,
                limit_per_host=self.max_concurrent_requests,
                enable_cleanup_closed=True,
                ssl=ssl_context
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers=headers
            )
            self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            from datetime import datetime
            self.stats['start_time'] = datetime.now()
    
    async def test_connection(self) -> bool:
        """Test connection to ServiceNow instance."""
        try:
            await self._create_session()
            response = await self._request_with_retry(
                'GET',
                f"{self.base_url}/{self.table}",
                params={'sysparm_limit': 1}
            )
            return response is not None and 'result' in response
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def _build_query(self) -> str:
        """Build ServiceNow encoded query string."""
        conditions = []
        
        if self.operating_entity_filter:
            safe_filter = self.operating_entity_filter.replace("'", "\\'")
            conditions.append(f"u_operating_entityLIKE{safe_filter}")
            conditions.append(f"^ORoperating_entityLIKE{safe_filter}")
            conditions.append(f"^ORcompanyLIKE{safe_filter}")
        
        if conditions:
            return ''.join(conditions)
        return ''
    
    async def get_total_count(self) -> int:
        """Get total count of records matching the filter."""
        query = self._build_query()
        params = {
            'sysparm_limit': 1,
            'sysparm_fields': 'sys_id'
        }
        
        if query:
            params['sysparm_query'] = query
        
        response = await self._request_with_retry(
            'GET',
            f"{self.base_url}/{self.table}",
            params={**params, 'sysparm_count': 'true'}
        )
        
        if response:
            headers = getattr(response, 'headers', {})
            count = headers.get('X-Total-Count', '0')
            try:
                return int(count)
            except (ValueError, TypeError):
                pass
        
        return 0
    
    async def fetch_all_data(self) -> List[Dict[str, Any]]:
        """
        Fetch all servers from CMDB with the configured filter.
        
        Returns:
            List of server dictionaries with normalized data
        """
        self.logger.info(f"Fetching servers from ServiceNow CMDB table: {self.table}")
        
        if self.operating_entity_filter:
            self.logger.info(f"Filter: Operating Entity contains '{self.operating_entity_filter}'")
        
        query = self._build_query()
        params = {}
        
        if query:
            params['sysparm_query'] = query
        
        servers = await self._fetch_with_servicenow_pagination(params)
        
        self.logger.info(f"Fetched {len(servers)} servers. Normalizing data...")
        
        if servers:
            self._discover_fields(servers[0])
        
        normalized_servers = [
            self._normalize_server(server) 
            for server in servers
        ]
        
        return normalized_servers
    
    async def _fetch_with_servicenow_pagination(
        self,
        params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fetch all records using ServiceNow pagination.
        ServiceNow uses sysparm_offset and sysparm_limit.
        """
        all_data = []
        offset = 0
        
        params['sysparm_limit'] = self.page_size
        
        first_response = await self._request_with_retry(
            'GET',
            f"{self.base_url}/{self.table}",
            params={**params, 'sysparm_offset': 0}
        )
        
        if not first_response:
            return []
        
        first_batch = first_response.get('result', [])
        all_data.extend(first_batch)
        
        self.logger.info(f"First batch: {len(first_batch)} records")
        
        if len(first_batch) < self.page_size:
            return all_data
        
        offset = self.page_size
        
        while True:
            tasks = []
            for i in range(self.max_concurrent_requests):
                current_offset = offset + (i * self.page_size)
                
                task = self._request_with_retry(
                    'GET',
                    f"{self.base_url}/{self.table}",
                    params={**params, 'sysparm_offset': current_offset}
                )
                tasks.append(task)
            
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            new_items = 0
            for response in responses:
                if isinstance(response, Exception):
                    self.logger.error(f"Batch request failed: {response}")
                    continue
                
                if response and 'result' in response:
                    batch = response['result']
                    all_data.extend(batch)
                    new_items += len(batch)
            
            if new_items == 0:
                break
            
            offset += len(tasks) * self.page_size
            self.logger.info(f"Progress: {len(all_data)} records fetched")
            
            if new_items < len(tasks) * self.page_size:
                break
        
        return all_data
    
    def _discover_fields(self, sample_record: Dict[str, Any]):
        """Discover all fields from a sample record for logging."""
        self._discovered_fields = list(sample_record.keys())
        
        custom_fields = [f for f in self._discovered_fields if f.startswith('u_')]
        if custom_fields:
            self.logger.info(f"Discovered {len(custom_fields)} custom fields (u_*): {custom_fields[:10]}...")
    
    def _normalize_server(self, server: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize server data and flatten nested structures.
        
        Args:
            server: Raw server record from ServiceNow
            
        Returns:
            Normalized server dictionary
        """
        def get_display_value(field_data):
            """Extract display value from ServiceNow field."""
            if isinstance(field_data, dict):
                return field_data.get('display_value', field_data.get('value', ''))
            return field_data or ''
        
        hostname = server.get('name', '') or server.get('host_name', '') or ''
        
        normalized = {
            'sys_id': server.get('sys_id', ''),
            'name': server.get('name', ''),
            'hostname': hostname,
            'hostname_normalized': hostname.upper(),
            'asset_tag': server.get('asset_tag', ''),
            'serial_number': server.get('serial_number', ''),
            'fqdn': server.get('fqdn', ''),
            'dns_domain': server.get('dns_domain', ''),
            
            'ip_address': server.get('ip_address', ''),
            'mac_address': server.get('mac_address', ''),
            
            'sys_class_name': server.get('sys_class_name', ''),
            'category': server.get('category', ''),
            'subcategory': server.get('subcategory', ''),
            'classification': server.get('classification', ''),
            
            'operating_entity': get_display_value(server.get('u_operating_entity', server.get('operating_entity', ''))),
            'company': get_display_value(server.get('company', '')),
            'department': get_display_value(server.get('department', '')),
            'location': get_display_value(server.get('location', '')),
            'cost_center': server.get('cost_center', ''),
            'business_unit': server.get('business_unit', ''),
            
            'os': server.get('os', ''),
            'os_version': server.get('os_version', ''),
            'os_domain': server.get('os_domain', ''),
            'cpu_count': server.get('cpu_count', ''),
            'cpu_type': server.get('cpu_type', ''),
            'cpu_speed': server.get('cpu_speed', ''),
            'ram': server.get('ram', ''),
            'disk_space': server.get('disk_space', ''),
            'virtual': server.get('virtual', ''),
            
            'operational_status': get_display_value(server.get('operational_status', '')),
            'install_status': get_display_value(server.get('install_status', '')),
            
            'assigned_to': get_display_value(server.get('assigned_to', '')),
            'managed_by': get_display_value(server.get('managed_by', '')),
            'owned_by': get_display_value(server.get('owned_by', '')),
            'supported_by': get_display_value(server.get('supported_by', '')),
            'support_group': get_display_value(server.get('support_group', '')),
            
            'environment': server.get('u_environment', server.get('environment', '')),
            'application': get_display_value(server.get('u_application', '')),
            'criticality': server.get('u_criticality', server.get('criticality', '')),
            
            'sys_created_on': server.get('sys_created_on', ''),
            'sys_updated_on': server.get('sys_updated_on', ''),
            'sys_created_by': server.get('sys_created_by', ''),
            'sys_updated_by': server.get('sys_updated_by', ''),
            'discovery_source': server.get('discovery_source', ''),
            'last_discovered': server.get('last_discovered', ''),
        }
        
        for key, value in server.items():
            if key.startswith('u_') and key not in normalized:
                normalized[key] = get_display_value(value)
        
        return normalized
    
    def get_discovered_fields(self) -> List[str]:
        """Return list of all discovered fields from the CMDB."""
        return self._discovered_fields.copy()


async def create_servicenow_connector(config) -> ServiceNowConnector:
    """Factory function to create a ServiceNow connector from config."""
    return ServiceNowConnector(
        instance_url=config.servicenow.instance_url,
        api_user=config.servicenow.api_user,
        api_key=config.servicenow.api_key,
        table=config.servicenow.table,
        verify_ssl=config.servicenow.verify_ssl,
        page_size=config.servicenow.page_size,
        max_concurrent_requests=config.servicenow.max_concurrent_requests,
        timeout=config.servicenow.timeout,
        max_retries=config.retry.max_attempts,
        initial_delay=config.retry.initial_delay,
        backoff_multiplier=config.retry.backoff_multiplier,
        max_delay=config.retry.max_delay,
        operating_entity_filter=config.filtering.operating_entity_contains
    )
