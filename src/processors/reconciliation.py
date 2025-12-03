"""
Data reconciliation between Illumio workloads and ServiceNow CMDB servers.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum


class ReconciliationStatus(Enum):
    """Status categories for reconciliation results."""
    DEPLOYED_ACTIVE = "deployed_active"
    DEPLOYED_OFFLINE = "deployed_offline"
    DEPLOYED_SUSPENDED = "deployed_suspended"
    DEPLOYED_UNINSTALLED = "deployed_uninstalled"
    NOT_DEPLOYED = "not_deployed"
    NOT_IN_CMDB = "not_in_cmdb"
    UNKNOWN = "unknown"


@dataclass
class ReconciliationStats:
    """Statistics from reconciliation process."""
    total_cmdb_servers: int = 0
    total_illumio_workloads: int = 0
    
    deployed_active: int = 0
    deployed_offline: int = 0
    deployed_suspended: int = 0
    deployed_uninstalled: int = 0
    not_deployed: int = 0
    not_in_cmdb: int = 0
    
    matched_by_hostname: int = 0
    
    coverage_rate: float = 0.0
    active_rate: float = 0.0
    enforcement_rate: float = 0.0
    
    by_environment: Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_application: Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_operating_entity: Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_ven_status: Dict[str, int] = field(default_factory=dict)
    by_enforcement_mode: Dict[str, int] = field(default_factory=dict)
    by_ven_version: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            'total_cmdb_servers': self.total_cmdb_servers,
            'total_illumio_workloads': self.total_illumio_workloads,
            'deployed_active': self.deployed_active,
            'deployed_offline': self.deployed_offline,
            'deployed_suspended': self.deployed_suspended,
            'deployed_uninstalled': self.deployed_uninstalled,
            'not_deployed': self.not_deployed,
            'not_in_cmdb': self.not_in_cmdb,
            'matched_by_hostname': self.matched_by_hostname,
            'coverage_rate': self.coverage_rate,
            'active_rate': self.active_rate,
            'enforcement_rate': self.enforcement_rate,
            'by_environment': self.by_environment,
            'by_application': self.by_application,
            'by_operating_entity': self.by_operating_entity,
            'by_ven_status': self.by_ven_status,
            'by_enforcement_mode': self.by_enforcement_mode,
            'by_ven_version': self.by_ven_version
        }


class DataReconciliation:
    """
    Reconciles Illumio workloads with ServiceNow CMDB servers.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stats = ReconciliationStats()
    
    def reconcile(
        self,
        workloads: List[Dict[str, Any]],
        servers: Optional[List[Dict[str, Any]]] = None
    ) -> Tuple[List[Dict[str, Any]], ReconciliationStats]:
        """
        Reconcile workloads with CMDB servers.
        
        Args:
            workloads: List of Illumio workloads
            servers: List of CMDB servers (optional - if None, Illumio-only analysis)
            
        Returns:
            Tuple of (reconciled_data, stats)
        """
        self.stats = ReconciliationStats()
        self.stats.total_illumio_workloads = len(workloads)
        
        if servers is None:
            self.logger.info("No CMDB data - performing Illumio-only analysis")
            return self._illumio_only_analysis(workloads)
        
        self.stats.total_cmdb_servers = len(servers)
        self.logger.info(
            f"Reconciling {len(workloads)} workloads with {len(servers)} servers"
        )
        
        workload_map = {}
        for workload in workloads:
            hostname = workload.get('hostname_normalized', '')
            if hostname:
                workload_map[hostname] = workload
        
        reconciled = []
        matched_hostnames = set()
        
        for server in servers:
            hostname = server.get('hostname_normalized', '')
            
            record = self._create_base_record(server=server)
            
            if hostname and hostname in workload_map:
                workload = workload_map[hostname]
                matched_hostnames.add(hostname)
                self.stats.matched_by_hostname += 1
                
                record = self._merge_records(record, workload)
                record['reconciliation_status'] = self._determine_status(workload)
                record['match_type'] = 'hostname'
            else:
                record['reconciliation_status'] = ReconciliationStatus.NOT_DEPLOYED.value
                record['match_type'] = 'none'
            
            self._update_status_counts(record['reconciliation_status'])
            self._update_breakdown_stats(record)
            
            reconciled.append(record)
        
        for hostname, workload in workload_map.items():
            if hostname not in matched_hostnames:
                record = self._create_base_record(workload=workload)
                record['reconciliation_status'] = ReconciliationStatus.NOT_IN_CMDB.value
                record['match_type'] = 'none'
                
                self.stats.not_in_cmdb += 1
                self._update_breakdown_stats(record)
                
                reconciled.append(record)
        
        self._calculate_rates()
        
        self.logger.info(f"Reconciliation complete. Stats: {self.stats.to_dict()}")
        
        return reconciled, self.stats
    
    def _illumio_only_analysis(
        self, 
        workloads: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], ReconciliationStats]:
        """Analyze Illumio workloads without CMDB data."""
        reconciled = []
        
        for workload in workloads:
            record = self._create_base_record(workload=workload)
            record['reconciliation_status'] = self._determine_status(workload)
            record['match_type'] = 'illumio_only'
            
            self._update_status_counts(record['reconciliation_status'])
            self._update_breakdown_stats(record)
            
            reconciled.append(record)
        
        self._calculate_rates()
        
        return reconciled, self.stats
    
    def _create_base_record(
        self,
        server: Optional[Dict[str, Any]] = None,
        workload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a base reconciliation record."""
        record = {
            'cmdb_sys_id': '',
            'cmdb_name': '',
            'cmdb_hostname': '',
            'cmdb_ip_address': '',
            'cmdb_operating_entity': '',
            'cmdb_environment': '',
            'cmdb_application': '',
            'cmdb_os': '',
            'cmdb_operational_status': '',
            'cmdb_location': '',
            'cmdb_assigned_to': '',
            
            'illumio_href': '',
            'illumio_hostname': '',
            'illumio_name': '',
            'illumio_primary_ip': '',
            'illumio_online': '',
            'illumio_managed': '',
            'illumio_ven_status': '',
            'illumio_ven_version': '',
            'illumio_enforcement_mode': '',
            'illumio_visibility_level': '',
            'illumio_os_type': '',
            'illumio_label_app': '',
            'illumio_label_env': '',
            'illumio_label_role': '',
            'illumio_label_loc': '',
            'illumio_last_heartbeat': '',
            
            'hostname_normalized': '',
            'reconciliation_status': '',
            'match_type': ''
        }
        
        if server:
            record.update({
                'cmdb_sys_id': server.get('sys_id', ''),
                'cmdb_name': server.get('name', ''),
                'cmdb_hostname': server.get('hostname', ''),
                'cmdb_ip_address': server.get('ip_address', ''),
                'cmdb_operating_entity': server.get('operating_entity', ''),
                'cmdb_environment': server.get('environment', ''),
                'cmdb_application': server.get('application', ''),
                'cmdb_os': server.get('os', ''),
                'cmdb_operational_status': server.get('operational_status', ''),
                'cmdb_location': server.get('location', ''),
                'cmdb_assigned_to': server.get('assigned_to', ''),
                'hostname_normalized': server.get('hostname_normalized', '')
            })
        
        if workload:
            record.update({
                'illumio_href': workload.get('href', ''),
                'illumio_hostname': workload.get('hostname', ''),
                'illumio_name': workload.get('name', ''),
                'illumio_primary_ip': workload.get('primary_ip', ''),
                'illumio_online': 'Yes' if workload.get('online') else 'No',
                'illumio_managed': 'Yes' if workload.get('managed') else 'No',
                'illumio_ven_status': workload.get('ven_status', ''),
                'illumio_ven_version': workload.get('ven_version', ''),
                'illumio_enforcement_mode': workload.get('enforcement_mode', ''),
                'illumio_visibility_level': workload.get('visibility_level', ''),
                'illumio_os_type': workload.get('os_type', ''),
                'illumio_label_app': workload.get('label_app', ''),
                'illumio_label_env': workload.get('label_env', ''),
                'illumio_label_role': workload.get('label_role', ''),
                'illumio_label_loc': workload.get('label_loc', ''),
                'illumio_last_heartbeat': workload.get('agent_last_heartbeat', ''),
                'hostname_normalized': workload.get('hostname_normalized', '') or record.get('hostname_normalized', '')
            })
        
        return record
    
    def _merge_records(
        self, 
        record: Dict[str, Any], 
        workload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge workload data into record."""
        record.update({
            'illumio_href': workload.get('href', ''),
            'illumio_hostname': workload.get('hostname', ''),
            'illumio_name': workload.get('name', ''),
            'illumio_primary_ip': workload.get('primary_ip', ''),
            'illumio_online': 'Yes' if workload.get('online') else 'No',
            'illumio_managed': 'Yes' if workload.get('managed') else 'No',
            'illumio_ven_status': workload.get('ven_status', ''),
            'illumio_ven_version': workload.get('ven_version', ''),
            'illumio_enforcement_mode': workload.get('enforcement_mode', ''),
            'illumio_visibility_level': workload.get('visibility_level', ''),
            'illumio_os_type': workload.get('os_type', ''),
            'illumio_label_app': workload.get('label_app', ''),
            'illumio_label_env': workload.get('label_env', ''),
            'illumio_label_role': workload.get('label_role', ''),
            'illumio_label_loc': workload.get('label_loc', ''),
            'illumio_last_heartbeat': workload.get('agent_last_heartbeat', '')
        })
        return record
    
    def _determine_status(self, workload: Dict[str, Any]) -> str:
        """Determine the reconciliation status based on workload state."""
        ven_status = workload.get('ven_status', '').lower()
        online = workload.get('online', False)
        managed = workload.get('managed', False)
        
        if not managed:
            return ReconciliationStatus.DEPLOYED_UNINSTALLED.value
        
        if ven_status == 'suspended':
            return ReconciliationStatus.DEPLOYED_SUSPENDED.value
        
        if ven_status == 'uninstalled':
            return ReconciliationStatus.DEPLOYED_UNINSTALLED.value
        
        if online:
            return ReconciliationStatus.DEPLOYED_ACTIVE.value
        else:
            return ReconciliationStatus.DEPLOYED_OFFLINE.value
    
    def _update_status_counts(self, status: str):
        """Update status counts in stats."""
        if status == ReconciliationStatus.DEPLOYED_ACTIVE.value:
            self.stats.deployed_active += 1
        elif status == ReconciliationStatus.DEPLOYED_OFFLINE.value:
            self.stats.deployed_offline += 1
        elif status == ReconciliationStatus.DEPLOYED_SUSPENDED.value:
            self.stats.deployed_suspended += 1
        elif status == ReconciliationStatus.DEPLOYED_UNINSTALLED.value:
            self.stats.deployed_uninstalled += 1
        elif status == ReconciliationStatus.NOT_DEPLOYED.value:
            self.stats.not_deployed += 1
    
    def _update_breakdown_stats(self, record: Dict[str, Any]):
        """Update breakdown statistics."""
        status = record.get('reconciliation_status', '')
        
        env = record.get('cmdb_environment', '') or record.get('illumio_label_env', '') or 'Unknown'
        if env not in self.stats.by_environment:
            self.stats.by_environment[env] = {}
        self.stats.by_environment[env][status] = self.stats.by_environment[env].get(status, 0) + 1
        
        app = record.get('cmdb_application', '') or record.get('illumio_label_app', '') or 'Unknown'
        if app not in self.stats.by_application:
            self.stats.by_application[app] = {}
        self.stats.by_application[app][status] = self.stats.by_application[app].get(status, 0) + 1
        
        oe = record.get('cmdb_operating_entity', '') or 'Unknown'
        if oe not in self.stats.by_operating_entity:
            self.stats.by_operating_entity[oe] = {}
        self.stats.by_operating_entity[oe][status] = self.stats.by_operating_entity[oe].get(status, 0) + 1
        
        ven_status = record.get('illumio_ven_status', '') or 'N/A'
        self.stats.by_ven_status[ven_status] = self.stats.by_ven_status.get(ven_status, 0) + 1
        
        mode = record.get('illumio_enforcement_mode', '') or 'N/A'
        self.stats.by_enforcement_mode[mode] = self.stats.by_enforcement_mode.get(mode, 0) + 1
        
        version = record.get('illumio_ven_version', '') or 'N/A'
        self.stats.by_ven_version[version] = self.stats.by_ven_version.get(version, 0) + 1
    
    def _calculate_rates(self):
        """Calculate coverage and health rates."""
        total_cmdb = self.stats.total_cmdb_servers
        
        if total_cmdb > 0:
            deployed = (
                self.stats.deployed_active + 
                self.stats.deployed_offline + 
                self.stats.deployed_suspended + 
                self.stats.deployed_uninstalled
            )
            self.stats.coverage_rate = (deployed / total_cmdb) * 100
            self.stats.active_rate = (self.stats.deployed_active / total_cmdb) * 100
        
        total_deployed = (
            self.stats.deployed_active + 
            self.stats.deployed_offline + 
            self.stats.deployed_suspended
        )
        
        if total_deployed > 0:
            enforced = self.stats.by_enforcement_mode.get('full', 0) + \
                       self.stats.by_enforcement_mode.get('selective', 0)
            self.stats.enforcement_rate = (enforced / total_deployed) * 100
    
    def get_not_deployed(
        self, 
        reconciled: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter records that are not deployed."""
        return [
            r for r in reconciled 
            if r.get('reconciliation_status') == ReconciliationStatus.NOT_DEPLOYED.value
        ]
    
    def get_shadow_it(
        self, 
        reconciled: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter records that are in Illumio but not in CMDB."""
        return [
            r for r in reconciled 
            if r.get('reconciliation_status') == ReconciliationStatus.NOT_IN_CMDB.value
        ]
    
    def get_offline_agents(
        self, 
        reconciled: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter records with offline agents."""
        return [
            r for r in reconciled 
            if r.get('reconciliation_status') == ReconciliationStatus.DEPLOYED_OFFLINE.value
        ]
    
    def get_suspended_agents(
        self, 
        reconciled: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter records with suspended agents."""
        return [
            r for r in reconciled 
            if r.get('reconciliation_status') == ReconciliationStatus.DEPLOYED_SUSPENDED.value
        ]
