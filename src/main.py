#!/usr/bin/env python3
"""
Illumio Monitoring Tool - Main Entry Point

This script orchestrates the extraction of data from Illumio PCE and ServiceNow CMDB,
reconciles the data, and generates Excel extractions and PDF reports.

Usage:
    python main.py [--config CONFIG_PATH]
    
Environment Variables:
    ILLUMIO_API_USER: Illumio API username
    ILLUMIO_API_SECRET: Illumio API secret
    SNOW_API_USER: ServiceNow API username  
    SNOW_API_KEY: ServiceNow API key
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from utils import load_config, create_output_directories, setup_logger, get_logger
from connectors import create_illumio_connector, create_servicenow_connector
from processors import DataReconciliation, normalize_workloads, normalize_servers
from exporters import ExcelExporter, PDFReportGenerator


class IllumioMonitoringTool:
    """
    Main orchestrator for the Illumio Monitoring Tool.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = None
        self.logger = None
        self.extracts_path = None
        self.reports_path = None
        
        self.workloads: List[Dict[str, Any]] = []
        self.servers: List[Dict[str, Any]] = []
        self.reconciled: List[Dict[str, Any]] = []
        self.stats: Dict[str, Any] = {}
        
        self.servicenow_available = True
        self.execution_stats = {
            'start_time': None,
            'end_time': None,
            'illumio_fetch_time': None,
            'servicenow_fetch_time': None,
            'reconciliation_time': None,
            'export_time': None,
            'errors': []
        }
    
    def initialize(self) -> bool:
        """Initialize configuration and logging."""
        try:
            self.config = load_config(self.config_path)
            
            self.logger = setup_logger(
                name="illumio_monitor",
                level=self.config.logging.level,
                log_file=self.config.logging.file,
                max_size_mb=self.config.logging.max_size_mb,
                backup_count=self.config.logging.backup_count,
                log_format=self.config.logging.format
            )
            
            self.extracts_path, self.reports_path = create_output_directories(self.config)
            
            self.logger.info("=" * 60)
            self.logger.info("Illumio Monitoring Tool - Starting")
            self.logger.info("=" * 60)
            self.logger.info(f"Configuration loaded from: {self.config_path}")
            self.logger.info(f"Extracts output: {self.extracts_path}")
            self.logger.info(f"Reports output: {self.reports_path}")
            self.logger.info(f"Operating Entity filter: {self.config.filtering.operating_entity_contains}")
            
            return True
            
        except FileNotFoundError as e:
            print(f"ERROR: Configuration file not found: {e}")
            return False
        except ValueError as e:
            print(f"ERROR: Configuration validation failed: {e}")
            return False
        except Exception as e:
            print(f"ERROR: Initialization failed: {e}")
            return False
    
    async def fetch_illumio_data(self) -> bool:
        """Fetch workloads from Illumio PCE."""
        self.logger.info("-" * 40)
        self.logger.info("Fetching data from Illumio PCE...")
        
        start_time = datetime.now()
        
        try:
            connector = await create_illumio_connector(self.config)
            
            async with connector:
                if not await connector.test_connection():
                    self.logger.error("Failed to connect to Illumio PCE")
                    self.execution_stats['errors'].append("Illumio connection failed")
                    return False
                
                self.logger.info("Connected to Illumio PCE successfully")
                
                self.workloads = await connector.fetch_all_data()
                
                stats = connector.get_stats()
                self.logger.info(f"Illumio fetch stats: {stats}")
            
            self.workloads = normalize_workloads(
                self.workloads, 
                self.config.normalization.hostname_uppercase
            )
            
            self.execution_stats['illumio_fetch_time'] = (
                datetime.now() - start_time
            ).total_seconds()
            
            self.logger.info(f"Fetched {len(self.workloads)} workloads from Illumio")
            return True
            
        except Exception as e:
            self.logger.error(f"Error fetching Illumio data: {e}", exc_info=True)
            self.execution_stats['errors'].append(f"Illumio fetch error: {str(e)}")
            return False
    
    async def fetch_servicenow_data(self) -> bool:
        """Fetch servers from ServiceNow CMDB."""
        self.logger.info("-" * 40)
        self.logger.info("Fetching data from ServiceNow CMDB...")
        
        start_time = datetime.now()
        
        try:
            connector = await create_servicenow_connector(self.config)
            
            async with connector:
                if not await connector.test_connection():
                    self.logger.warning("Failed to connect to ServiceNow CMDB")
                    self.logger.warning("Continuing with Illumio-only analysis")
                    self.servicenow_available = False
                    self.execution_stats['errors'].append("ServiceNow connection failed - continuing without CMDB")
                    return True
                
                self.logger.info("Connected to ServiceNow successfully")
                
                self.servers = await connector.fetch_all_data()
                
                discovered_fields = connector.get_discovered_fields()
                self.logger.info(f"Discovered {len(discovered_fields)} fields in CMDB")
                
                stats = connector.get_stats()
                self.logger.info(f"ServiceNow fetch stats: {stats}")
            
            self.servers = normalize_servers(
                self.servers,
                self.config.normalization.hostname_uppercase
            )
            
            self.execution_stats['servicenow_fetch_time'] = (
                datetime.now() - start_time
            ).total_seconds()
            
            self.logger.info(f"Fetched {len(self.servers)} servers from ServiceNow")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error fetching ServiceNow data: {e}")
            self.logger.warning("Continuing with Illumio-only analysis")
            self.servicenow_available = False
            self.execution_stats['errors'].append(f"ServiceNow fetch error: {str(e)}")
            return True
    
    def reconcile_data(self) -> bool:
        """Reconcile Illumio and ServiceNow data."""
        self.logger.info("-" * 40)
        self.logger.info("Reconciling data...")
        
        start_time = datetime.now()
        
        try:
            reconciler = DataReconciliation()
            
            servers_to_reconcile = self.servers if self.servicenow_available else None
            
            self.reconciled, stats_obj = reconciler.reconcile(
                self.workloads,
                servers_to_reconcile
            )
            
            self.stats = stats_obj.to_dict()
            
            self.execution_stats['reconciliation_time'] = (
                datetime.now() - start_time
            ).total_seconds()
            
            self.logger.info(f"Reconciliation complete: {len(self.reconciled)} records")
            self.logger.info(f"Coverage rate: {self.stats.get('coverage_rate', 0):.2f}%")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during reconciliation: {e}", exc_info=True)
            self.execution_stats['errors'].append(f"Reconciliation error: {str(e)}")
            return False
    
    def generate_exports(self) -> bool:
        """Generate Excel extractions."""
        self.logger.info("-" * 40)
        self.logger.info("Generating Excel extractions...")
        
        start_time = datetime.now()
        
        try:
            branding = {
                'primary_color': self.config.branding.primary_color,
                'secondary_color': self.config.branding.secondary_color
            }
            
            exporter = ExcelExporter(
                output_path=self.extracts_path,
                file_prefix=self.config.output.file_prefix,
                branding=branding
            )
            
            if self.workloads:
                exporter.export_workloads(self.workloads)
            
            if self.servers and self.servicenow_available:
                exporter.export_servers(self.servers)
            
            if self.reconciled:
                exporter.export_reconciliation(self.reconciled, self.stats)
                
                reconciler = DataReconciliation()
                
                not_deployed = reconciler.get_not_deployed(self.reconciled)
                if not_deployed:
                    exporter.export_gap_analysis(not_deployed)
                
                shadow_it = reconciler.get_shadow_it(self.reconciled)
                if shadow_it:
                    exporter.export_shadow_it(shadow_it)
                
                offline = reconciler.get_offline_agents(self.reconciled)
                suspended = reconciler.get_suspended_agents(self.reconciled)
                if offline or suspended:
                    exporter.export_health_issues(offline, suspended)
            
            self.execution_stats['export_time'] = (
                datetime.now() - start_time
            ).total_seconds()
            
            self.logger.info("Excel extractions generated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error generating exports: {e}", exc_info=True)
            self.execution_stats['errors'].append(f"Export error: {str(e)}")
            return False
    
    def generate_reports(self) -> bool:
        """Generate PDF reports."""
        self.logger.info("-" * 40)
        self.logger.info("Generating PDF reports...")
        
        start_time = datetime.now()
        
        try:
            branding = {
                'company': self.config.branding.company,
                'primary_color': self.config.branding.primary_color,
                'secondary_color': self.config.branding.secondary_color,
                'accent_color': self.config.branding.accent_color,
                'chart_colors': self.config.branding.chart_colors,
                'logo_path': self.config.branding.logo_path,
                'font_family': self.config.branding.font_family,
                'footer_text': self.config.branding.footer_text
            }
            
            generator = PDFReportGenerator(
                output_path=self.reports_path,
                file_prefix=self.config.output.file_prefix,
                branding=branding
            )
            
            reconciler = DataReconciliation()
            not_deployed = reconciler.get_not_deployed(self.reconciled) if self.reconciled else []
            shadow_it = reconciler.get_shadow_it(self.reconciled) if self.reconciled else []
            offline = reconciler.get_offline_agents(self.reconciled) if self.reconciled else []
            suspended = reconciler.get_suspended_agents(self.reconciled) if self.reconciled else []
            
            report_config = self.config.reports.generate
            
            if report_config.get('executive_summary', True):
                generator.generate_executive_summary(
                    self.stats,
                    cmdb_available=self.servicenow_available
                )
            
            if report_config.get('deployment_dashboard', True):
                generator.generate_deployment_dashboard(
                    self.stats,
                    self.reconciled
                )
            
            if report_config.get('agent_health', True):
                generator.generate_agent_health_report(
                    self.stats,
                    offline,
                    suspended
                )
            
            if report_config.get('gap_analysis', True):
                generator.generate_gap_analysis_report(
                    self.stats,
                    not_deployed,
                    shadow_it
                )
            
            report_time = (datetime.now() - start_time).total_seconds()
            self.execution_stats['export_time'] = (
                self.execution_stats.get('export_time', 0) + report_time
            )
            
            self.logger.info("PDF reports generated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error generating reports: {e}", exc_info=True)
            self.execution_stats['errors'].append(f"Report generation error: {str(e)}")
            return False
    
    async def run(self) -> bool:
        """Run the complete monitoring workflow."""
        self.execution_stats['start_time'] = datetime.now()
        
        if not self.initialize():
            return False
        
        try:
            if not await self.fetch_illumio_data():
                self.logger.error("Failed to fetch Illumio data. Aborting.")
                return False
            
            await self.fetch_servicenow_data()
            
            if not self.reconcile_data():
                self.logger.error("Failed to reconcile data. Aborting.")
                return False
            
            if not self.generate_exports():
                self.logger.warning("Excel export had errors but continuing...")
            
            if not self.generate_reports():
                self.logger.warning("PDF report generation had errors but continuing...")
            
            self.execution_stats['end_time'] = datetime.now()
            
            self._log_summary()
            
            return len(self.execution_stats['errors']) == 0
            
        except Exception as e:
            self.logger.error(f"Unexpected error during execution: {e}", exc_info=True)
            return False
    
    def _log_summary(self):
        """Log execution summary."""
        duration = (
            self.execution_stats['end_time'] - 
            self.execution_stats['start_time']
        ).total_seconds()
        
        self.logger.info("=" * 60)
        self.logger.info("EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total duration: {duration:.2f} seconds")
        self.logger.info(f"Illumio fetch: {self.execution_stats.get('illumio_fetch_time') or 0:.2f}s")
        self.logger.info(f"ServiceNow fetch: {self.execution_stats.get('servicenow_fetch_time') or 0:.2f}s")
        self.logger.info(f"Reconciliation: {self.execution_stats.get('reconciliation_time') or 0:.2f}s")
        self.logger.info(f"Export generation: {self.execution_stats.get('export_time') or 0:.2f}s")
        self.logger.info("-" * 40)
        self.logger.info(f"Workloads fetched: {len(self.workloads)}")
        self.logger.info(f"Servers fetched: {len(self.servers)}")
        self.logger.info(f"Records reconciled: {len(self.reconciled)}")
        self.logger.info(f"Coverage rate: {self.stats.get('coverage_rate') or 0:.2f}%")
        self.logger.info("-" * 40)
        self.logger.info(f"Extracts saved to: {self.extracts_path}")
        self.logger.info(f"Reports saved to: {self.reports_path}")
        
        if self.execution_stats['errors']:
            self.logger.warning("-" * 40)
            self.logger.warning(f"Errors encountered: {len(self.execution_stats['errors'])}")
            for error in self.execution_stats['errors']:
                self.logger.warning(f"  - {error}")
        
        self.logger.info("=" * 60)
        self.logger.info("Execution complete")
        self.logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Illumio Monitoring Tool - Extract, reconcile, and report on Illumio deployment"
    )
    parser.add_argument(
        "--config", "-c",
        default="config/config.yaml",
        help="Path to configuration file (default: config/config.yaml)"
    )
    
    args = parser.parse_args()
    
    tool = IllumioMonitoringTool(config_path=args.config)
    
    success = asyncio.run(tool.run())
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
