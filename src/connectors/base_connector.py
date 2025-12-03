"""
Base async connector class for API integrations.
Provides common functionality for retry logic, rate limiting, and async HTTP requests.
"""

import asyncio
import aiohttp
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
import logging


class BaseAsyncConnector(ABC):
    """
    Abstract base class for async API connectors.
    Implements retry logic, rate limiting, and connection pooling.
    """
    
    def __init__(
        self,
        base_url: str,
        max_concurrent_requests: int = 10,
        timeout: int = 30,
        max_retries: int = 3,
        initial_delay: float = 1,
        backoff_multiplier: float = 2,
        max_delay: float = 60
    ):
        self.base_url = base_url.rstrip('/')
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_multiplier = backoff_multiplier
        self.max_delay = max_delay
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'retries': 0,
            'start_time': None,
            'end_time': None
        }
    
    @abstractmethod
    def _get_auth_headers(self) -> Dict[str, str]:
        """Return authentication headers for API requests."""
        pass
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """Test the API connection. Returns True if successful."""
        pass
    
    @abstractmethod
    async def fetch_all_data(self) -> List[Dict[str, Any]]:
        """Fetch all data from the API with pagination."""
        pass
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()
        return False
    
    async def _create_session(self):
        """Create aiohttp session and semaphore."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent_requests,
                limit_per_host=self.max_concurrent_requests,
                enable_cleanup_closed=True
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers=self._get_auth_headers()
            )
            self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            self.stats['start_time'] = datetime.now()
    
    async def _close_session(self):
        """Close aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self.stats['end_time'] = datetime.now()
    
    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Make an HTTP request with retry logic and exponential backoff.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments for aiohttp request
            
        Returns:
            JSON response as dict, or None if all retries failed
        """
        delay = self.initial_delay
        
        for attempt in range(self.max_retries):
            try:
                async with self._semaphore:
                    self.stats['requests_made'] += 1
                    
                    async with self._session.request(method, url, **kwargs) as response:
                        if response.status == 200:
                            self.stats['requests_successful'] += 1
                            return await response.json()
                        
                        elif response.status == 429:
                            retry_after = int(response.headers.get('Retry-After', 60))
                            self.logger.warning(
                                f"Rate limited. Waiting {retry_after}s before retry."
                            )
                            await asyncio.sleep(retry_after)
                            self.stats['retries'] += 1
                            continue
                        
                        elif response.status in (401, 403):
                            self.logger.error(
                                f"Authentication failed: {response.status}"
                            )
                            self.stats['requests_failed'] += 1
                            return None
                        
                        elif response.status >= 500:
                            self.logger.warning(
                                f"Server error {response.status}. Attempt {attempt + 1}/{self.max_retries}"
                            )
                            self.stats['retries'] += 1
                        
                        else:
                            text = await response.text()
                            self.logger.error(
                                f"Request failed: {response.status} - {text[:200]}"
                            )
                            self.stats['requests_failed'] += 1
                            return None
            
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Request timeout. Attempt {attempt + 1}/{self.max_retries}"
                )
                self.stats['retries'] += 1
            
            except aiohttp.ClientError as e:
                self.logger.warning(
                    f"Client error: {e}. Attempt {attempt + 1}/{self.max_retries}"
                )
                self.stats['retries'] += 1
            
            if attempt < self.max_retries - 1:
                await asyncio.sleep(delay)
                delay = min(delay * self.backoff_multiplier, self.max_delay)
        
        self.stats['requests_failed'] += 1
        self.logger.error(f"All {self.max_retries} retry attempts failed for {url}")
        return None
    
    async def _paginated_fetch(
        self,
        endpoint: str,
        page_size: int,
        params: Optional[Dict[str, Any]] = None,
        data_key: Optional[str] = None,
        offset_param: str = "offset",
        limit_param: str = "limit",
        total_key: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of data from a paginated API endpoint.
        
        Args:
            endpoint: API endpoint (without base URL)
            page_size: Number of items per page
            params: Additional query parameters
            data_key: Key in response containing the data array (None if response is array)
            offset_param: Name of the offset parameter
            limit_param: Name of the limit parameter
            total_key: Key in response containing total count (for progress tracking)
            
        Returns:
            List of all fetched items
        """
        all_data = []
        offset = 0
        total_items = None
        
        params = params or {}
        params[limit_param] = page_size
        
        first_response = await self._request_with_retry(
            'GET',
            f"{self.base_url}{endpoint}",
            params={**params, offset_param: 0}
        )
        
        if not first_response:
            return []
        
        if data_key:
            first_batch = first_response.get(data_key, [])
            if total_key:
                total_items = first_response.get(total_key)
        else:
            first_batch = first_response if isinstance(first_response, list) else []
        
        all_data.extend(first_batch)
        
        if len(first_batch) < page_size:
            return all_data
        
        offset = page_size
        
        if total_items:
            remaining_pages = (total_items - page_size + page_size - 1) // page_size
            self.logger.info(f"Fetching {remaining_pages} additional pages...")
        
        while True:
            tasks = []
            for i in range(self.max_concurrent_requests):
                current_offset = offset + (i * page_size)
                if total_items and current_offset >= total_items:
                    break
                    
                task = self._request_with_retry(
                    'GET',
                    f"{self.base_url}{endpoint}",
                    params={**params, offset_param: current_offset}
                )
                tasks.append(task)
            
            if not tasks:
                break
            
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            new_items = 0
            for response in responses:
                if isinstance(response, Exception):
                    self.logger.error(f"Batch request failed: {response}")
                    continue
                    
                if response:
                    if data_key:
                        batch = response.get(data_key, [])
                    else:
                        batch = response if isinstance(response, list) else []
                    
                    all_data.extend(batch)
                    new_items += len(batch)
            
            if new_items == 0:
                break
            
            offset += len(tasks) * page_size
            
            if total_items:
                progress = min(100, (len(all_data) / total_items) * 100)
                self.logger.info(f"Progress: {len(all_data)}/{total_items} ({progress:.1f}%)")
        
        return all_data
    
    def get_stats(self) -> Dict[str, Any]:
        """Return statistics about API requests."""
        stats = self.stats.copy()
        if stats['start_time'] and stats['end_time']:
            stats['duration_seconds'] = (
                stats['end_time'] - stats['start_time']
            ).total_seconds()
        return stats
