"""
Excel exporter for generating extraction files.
Uses xlsxwriter for efficient large file generation.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

import pandas as pd


class ExcelExporter:
    """
    Exports data to Excel files with formatting.
    Optimized for large datasets (100k+ rows).
    """
    
    def __init__(
        self, 
        output_path: Path,
        file_prefix: str = "illumio_monitoring",
        branding: Optional[Dict[str, Any]] = None
    ):
        self.output_path = output_path
        self.file_prefix = file_prefix
        self.branding = branding or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.primary_color = self.branding.get('primary_color', '#A100FF')
        self.header_bg_color = self.primary_color.replace('#', '')
    
    def _get_filename(self, name: str) -> Path:
        """Generate filename with date."""
        date_str = datetime.now().strftime("%d-%m-%Y")
        return self.output_path / f"{self.file_prefix}_{name}_{date_str}.xlsx"
    
    def export_workloads(self, workloads: List[Dict[str, Any]]) -> Path:
        """Export Illumio workloads to Excel."""
        filename = self._get_filename("illumio_workloads")
        self.logger.info(f"Exporting {len(workloads)} workloads to {filename}")
        
        df = pd.DataFrame(workloads)
        
        column_order = [
            'hostname', 'hostname_normalized', 'name', 'primary_ip', 'all_ips',
            'online', 'managed', 'ven_status', 'ven_version',
            'enforcement_mode', 'visibility_level',
            'label_app', 'label_env', 'label_role', 'label_loc',
            'os_type', 'os_detail',
            'agent_status', 'agent_last_heartbeat',
            'data_center', 'data_center_zone',
            'created_at', 'updated_at',
            'href'
        ]
        
        existing_cols = [c for c in column_order if c in df.columns]
        other_cols = [c for c in df.columns if c not in column_order]
        df = df[existing_cols + other_cols]
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='All_Workloads', index=False)
            
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': self.header_bg_color,
                'font_color': 'white',
                'border': 1
            })
            
            worksheet = writer.sheets['All_Workloads']
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                max_len = max(df[value].astype(str).map(len).max(), len(value)) + 2
                worksheet.set_column(col_num, col_num, min(max_len, 50))
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            worksheet.freeze_panes(1, 0)
            
            if 'ven_status' in df.columns:
                status_df = df.groupby('ven_status').size().reset_index(name='count')
                status_df.to_excel(writer, sheet_name='By_Status', index=False)
                
                ws = writer.sheets['By_Status']
                for col_num, value in enumerate(status_df.columns.values):
                    ws.write(0, col_num, value, header_format)
            
            label_cols = [c for c in df.columns if c.startswith('label_')]
            if label_cols:
                label_summary = []
                for col in label_cols:
                    label_name = col.replace('label_', '')
                    value_counts = df[col].value_counts().head(20)
                    for value, count in value_counts.items():
                        if value:
                            label_summary.append({
                                'Label': label_name,
                                'Value': value,
                                'Count': count
                            })
                
                if label_summary:
                    label_df = pd.DataFrame(label_summary)
                    label_df.to_excel(writer, sheet_name='By_Label', index=False)
                    
                    ws = writer.sheets['By_Label']
                    for col_num, value in enumerate(label_df.columns.values):
                        ws.write(0, col_num, value, header_format)
        
        self.logger.info(f"Workloads exported to {filename}")
        return filename
    
    def export_servers(self, servers: List[Dict[str, Any]]) -> Path:
        """Export ServiceNow servers to Excel."""
        filename = self._get_filename("servicenow_servers")
        self.logger.info(f"Exporting {len(servers)} servers to {filename}")
        
        df = pd.DataFrame(servers)
        
        column_order = [
            'hostname', 'hostname_normalized', 'name', 'ip_address',
            'operating_entity', 'environment', 'application',
            'os', 'os_version',
            'operational_status', 'install_status',
            'location', 'assigned_to', 'managed_by',
            'sys_id', 'sys_created_on', 'sys_updated_on'
        ]
        
        existing_cols = [c for c in column_order if c in df.columns]
        other_cols = [c for c in df.columns if c not in column_order]
        df = df[existing_cols + other_cols]
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='All_Servers', index=False)
            
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': self.header_bg_color,
                'font_color': 'white',
                'border': 1
            })
            
            worksheet = writer.sheets['All_Servers']
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                max_len = max(df[value].astype(str).map(len).max(), len(value)) + 2
                worksheet.set_column(col_num, col_num, min(max_len, 50))
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            worksheet.freeze_panes(1, 0)
            
            if 'operating_entity' in df.columns:
                oe_df = df.groupby('operating_entity').size().reset_index(name='count')
                oe_df = oe_df.sort_values('count', ascending=False)
                oe_df.to_excel(writer, sheet_name='By_OE', index=False)
                
                ws = writer.sheets['By_OE']
                for col_num, value in enumerate(oe_df.columns.values):
                    ws.write(0, col_num, value, header_format)
            
            if 'environment' in df.columns:
                env_df = df.groupby('environment').size().reset_index(name='count')
                env_df = env_df.sort_values('count', ascending=False)
                env_df.to_excel(writer, sheet_name='By_Environment', index=False)
                
                ws = writer.sheets['By_Environment']
                for col_num, value in enumerate(env_df.columns.values):
                    ws.write(0, col_num, value, header_format)
        
        self.logger.info(f"Servers exported to {filename}")
        return filename
    
    def export_reconciliation(
        self, 
        reconciled: List[Dict[str, Any]],
        stats: Dict[str, Any]
    ) -> Path:
        """Export full reconciliation data to Excel."""
        filename = self._get_filename("reconciliation_full")
        self.logger.info(f"Exporting {len(reconciled)} reconciled records to {filename}")
        
        df = pd.DataFrame(reconciled)
        
        column_order = [
            'hostname_normalized', 'reconciliation_status', 'match_type',
            'cmdb_name', 'cmdb_ip_address', 'cmdb_operating_entity',
            'cmdb_environment', 'cmdb_application', 'cmdb_os',
            'cmdb_operational_status',
            'illumio_hostname', 'illumio_primary_ip', 'illumio_ven_status',
            'illumio_ven_version', 'illumio_enforcement_mode',
            'illumio_online', 'illumio_managed',
            'illumio_label_app', 'illumio_label_env',
            'cmdb_sys_id', 'illumio_href'
        ]
        
        existing_cols = [c for c in column_order if c in df.columns]
        other_cols = [c for c in df.columns if c not in column_order]
        df = df[existing_cols + other_cols]
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Full_Data', index=False)
            
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': self.header_bg_color,
                'font_color': 'white',
                'border': 1
            })
            
            worksheet = writer.sheets['Full_Data']
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                max_len = min(
                    max(df[value].astype(str).map(len).max(), len(value)) + 2,
                    50
                )
                worksheet.set_column(col_num, col_num, max_len)
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            worksheet.freeze_panes(1, 0)
            
            stats_data = [
                ['Metric', 'Value'],
                ['Total CMDB Servers', stats.get('total_cmdb_servers', 0)],
                ['Total Illumio Workloads', stats.get('total_illumio_workloads', 0)],
                ['Deployed Active', stats.get('deployed_active', 0)],
                ['Deployed Offline', stats.get('deployed_offline', 0)],
                ['Deployed Suspended', stats.get('deployed_suspended', 0)],
                ['Not Deployed', stats.get('not_deployed', 0)],
                ['Not in CMDB (Shadow IT)', stats.get('not_in_cmdb', 0)],
                ['Coverage Rate (%)', f"{stats.get('coverage_rate', 0):.2f}"],
                ['Active Rate (%)', f"{stats.get('active_rate', 0):.2f}"],
                ['Enforcement Rate (%)', f"{stats.get('enforcement_rate', 0):.2f}"]
            ]
            
            stats_df = pd.DataFrame(stats_data[1:], columns=stats_data[0])
            stats_df.to_excel(writer, sheet_name='Summary_Stats', index=False)
            
            ws = writer.sheets['Summary_Stats']
            ws.write(0, 0, 'Metric', header_format)
            ws.write(0, 1, 'Value', header_format)
            ws.set_column(0, 0, 30)
            ws.set_column(1, 1, 20)
        
        self.logger.info(f"Reconciliation exported to {filename}")
        return filename
    
    def export_gap_analysis(self, not_deployed: List[Dict[str, Any]]) -> Path:
        """Export gap analysis (servers not deployed) to Excel."""
        filename = self._get_filename("gap_not_deployed")
        self.logger.info(f"Exporting {len(not_deployed)} not deployed servers to {filename}")
        
        df = pd.DataFrame(not_deployed)
        
        columns_to_keep = [
            'hostname_normalized', 'cmdb_name', 'cmdb_ip_address',
            'cmdb_operating_entity', 'cmdb_environment', 'cmdb_application',
            'cmdb_os', 'cmdb_operational_status', 'cmdb_location',
            'cmdb_assigned_to', 'cmdb_sys_id'
        ]
        
        df = df[[c for c in columns_to_keep if c in df.columns]]
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Gap_List', index=False)
            
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': self.header_bg_color,
                'font_color': 'white',
                'border': 1
            })
            
            worksheet = writer.sheets['Gap_List']
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            worksheet.freeze_panes(1, 0)
            
            if 'cmdb_application' in df.columns:
                app_df = df.groupby('cmdb_application').size().reset_index(name='count')
                app_df = app_df.sort_values('count', ascending=False)
                app_df.to_excel(writer, sheet_name='By_Application', index=False)
            
            if 'cmdb_environment' in df.columns:
                env_df = df.groupby('cmdb_environment').size().reset_index(name='count')
                env_df = env_df.sort_values('count', ascending=False)
                env_df.to_excel(writer, sheet_name='By_Environment', index=False)
        
        self.logger.info(f"Gap analysis exported to {filename}")
        return filename
    
    def export_shadow_it(self, shadow_it: List[Dict[str, Any]]) -> Path:
        """Export shadow IT (workloads not in CMDB) to Excel."""
        filename = self._get_filename("shadow_it")
        self.logger.info(f"Exporting {len(shadow_it)} shadow IT records to {filename}")
        
        df = pd.DataFrame(shadow_it)
        
        columns_to_keep = [
            'hostname_normalized', 'illumio_hostname', 'illumio_name',
            'illumio_primary_ip', 'illumio_ven_status', 'illumio_ven_version',
            'illumio_enforcement_mode', 'illumio_os_type',
            'illumio_label_app', 'illumio_label_env', 'illumio_label_role',
            'illumio_href'
        ]
        
        df = df[[c for c in columns_to_keep if c in df.columns]]
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Shadow_List', index=False)
            
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': self.header_bg_color,
                'font_color': 'white',
                'border': 1
            })
            
            worksheet = writer.sheets['Shadow_List']
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
            worksheet.freeze_panes(1, 0)
        
        self.logger.info(f"Shadow IT exported to {filename}")
        return filename
    
    def export_health_issues(
        self, 
        offline: List[Dict[str, Any]],
        suspended: List[Dict[str, Any]]
    ) -> Path:
        """Export agent health issues to Excel."""
        filename = self._get_filename("health_issues")
        self.logger.info(
            f"Exporting health issues: {len(offline)} offline, {len(suspended)} suspended"
        )
        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': self.header_bg_color,
                'font_color': 'white',
                'border': 1
            })
            
            columns_to_keep = [
                'hostname_normalized', 'illumio_hostname', 'illumio_primary_ip',
                'illumio_ven_status', 'illumio_ven_version', 'illumio_last_heartbeat',
                'cmdb_operating_entity', 'cmdb_environment', 'cmdb_application'
            ]
            
            if offline:
                offline_df = pd.DataFrame(offline)
                offline_df = offline_df[[c for c in columns_to_keep if c in offline_df.columns]]
                offline_df.to_excel(writer, sheet_name='Offline_Agents', index=False)
                
                ws = writer.sheets['Offline_Agents']
                for col_num, value in enumerate(offline_df.columns.values):
                    ws.write(0, col_num, value, header_format)
                ws.autofilter(0, 0, len(offline_df), len(offline_df.columns) - 1)
            
            if suspended:
                suspended_df = pd.DataFrame(suspended)
                suspended_df = suspended_df[[c for c in columns_to_keep if c in suspended_df.columns]]
                suspended_df.to_excel(writer, sheet_name='Suspended_Agents', index=False)
                
                ws = writer.sheets['Suspended_Agents']
                for col_num, value in enumerate(suspended_df.columns.values):
                    ws.write(0, col_num, value, header_format)
                ws.autofilter(0, 0, len(suspended_df), len(suspended_df.columns) - 1)
        
        self.logger.info(f"Health issues exported to {filename}")
        return filename
