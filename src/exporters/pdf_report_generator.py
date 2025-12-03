"""
PDF report generator with Accenture branding.
Uses reportlab for PDF generation and matplotlib for charts.
"""

import io
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


class PDFReportGenerator:
    """
    Generates PDF reports with Accenture branding.
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
        
        self.primary_color = self._hex_to_rgb(
            self.branding.get('primary_color', '#A100FF')
        )
        self.secondary_color = self._hex_to_rgb(
            self.branding.get('secondary_color', '#000000')
        )
        self.accent_color = self._hex_to_rgb(
            self.branding.get('accent_color', '#FFFFFF')
        )
        
        self.chart_colors = [
            self._hex_to_rgb(c) for c in 
            self.branding.get('chart_colors', [
                '#A100FF', '#7B00C4', '#460073', '#000000', '#808080', '#B3B3B3'
            ])
        ]
        
        self.company = self.branding.get('company', 'Accenture')
        self.footer_text = self.branding.get(
            'footer_text', 
            'Confidential - Accenture Internal Use Only'
        )
        self.logo_path = self.branding.get('logo_path')
        
        self._setup_styles()
    
    def _hex_to_rgb(self, hex_color: str) -> Tuple[float, float, float]:
        """Convert hex color to RGB tuple (0-1 range)."""
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) / 255 for i in (0, 2, 4))
    
    def _hex_to_reportlab(self, hex_color: str) -> colors.Color:
        """Convert hex color to reportlab Color."""
        rgb = self._hex_to_rgb(hex_color)
        return colors.Color(rgb[0], rgb[1], rgb[2])
    
    def _setup_styles(self):
        """Set up paragraph styles."""
        self.styles = getSampleStyleSheet()
        
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Title'],
            fontSize=24,
            textColor=self._hex_to_reportlab(
                self.branding.get('primary_color', '#A100FF')
            ),
            spaceAfter=30,
            alignment=TA_CENTER
        ))
        
        self.styles.add(ParagraphStyle(
            name='CustomHeading1',
            parent=self.styles['Heading1'],
            fontSize=16,
            textColor=self._hex_to_reportlab(
                self.branding.get('primary_color', '#A100FF')
            ),
            spaceBefore=20,
            spaceAfter=10
        ))
        
        self.styles.add(ParagraphStyle(
            name='CustomHeading2',
            parent=self.styles['Heading2'],
            fontSize=14,
            textColor=self._hex_to_reportlab(
                self.branding.get('secondary_color', '#000000')
            ),
            spaceBefore=15,
            spaceAfter=8
        ))
        
        self.styles.add(ParagraphStyle(
            name='CustomBody',
            parent=self.styles['Normal'],
            fontSize=10,
            leading=14,
            spaceAfter=6
        ))
        
        self.styles.add(ParagraphStyle(
            name='KPIValue',
            parent=self.styles['Normal'],
            fontSize=28,
            textColor=self._hex_to_reportlab(
                self.branding.get('primary_color', '#A100FF')
            ),
            alignment=TA_CENTER,
            spaceAfter=2
        ))
        
        self.styles.add(ParagraphStyle(
            name='KPILabel',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.gray,
            alignment=TA_CENTER
        ))
    
    def _get_filename(self, name: str) -> Path:
        """Generate filename with date."""
        date_str = datetime.now().strftime("%d-%m-%Y")
        return self.output_path / f"{self.file_prefix}_{name}_{date_str}.pdf"
    
    def _add_header_footer(self, canvas, doc):
        """Add header and footer to each page."""
        canvas.saveState()
        
        page_width, page_height = A4
        
        canvas.setFillColor(self._hex_to_reportlab(
            self.branding.get('primary_color', '#A100FF')
        ))
        canvas.rect(0, page_height - 25*mm, page_width, 25*mm, fill=True, stroke=False)
        
        canvas.setFillColor(colors.white)
        canvas.setFont('Helvetica-Bold', 14)
        canvas.drawString(15*mm, page_height - 17*mm, self.company)
        
        date_str = datetime.now().strftime("%d-%m-%Y")
        canvas.setFont('Helvetica', 10)
        canvas.drawRightString(page_width - 15*mm, page_height - 17*mm, date_str)
        
        canvas.setFillColor(colors.gray)
        canvas.setFont('Helvetica', 8)
        canvas.drawString(15*mm, 15*mm, self.footer_text)
        canvas.drawRightString(page_width - 15*mm, 15*mm, f"Page {doc.page}")
        
        canvas.restoreState()
    
    def _create_pie_chart(
        self,
        data: Dict[str, int],
        title: str,
        width: float = 4,
        height: float = 3
    ) -> Image:
        """Create a pie chart and return as reportlab Image."""
        fig, ax = plt.subplots(figsize=(width, height))
        
        labels = list(data.keys())
        sizes = list(data.values())
        
        chart_colors_hex = self.branding.get('chart_colors', [
            '#A100FF', '#7B00C4', '#460073', '#000000', '#808080', '#B3B3B3'
        ])
        pie_colors = chart_colors_hex[:len(labels)]
        
        if sizes and sum(sizes) > 0:
            wedges, texts, autotexts = ax.pie(
                sizes,
                labels=None,
                autopct='%1.1f%%',
                colors=pie_colors,
                startangle=90
            )
            
            ax.legend(
                wedges, 
                [f"{l} ({s:,})" for l, s in zip(labels, sizes)],
                loc='center left',
                bbox_to_anchor=(1, 0.5),
                fontsize=8
            )
        
        ax.set_title(title, fontsize=11, fontweight='bold', 
                    color=self.branding.get('primary_color', '#A100FF'))
        
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        
        return Image(buf, width=width*inch, height=height*inch)
    
    def _create_bar_chart(
        self,
        data: Dict[str, int],
        title: str,
        xlabel: str = "",
        ylabel: str = "Count",
        width: float = 6,
        height: float = 3,
        horizontal: bool = False,
        top_n: int = 10
    ) -> Image:
        """Create a bar chart and return as reportlab Image."""
        sorted_data = dict(sorted(data.items(), key=lambda x: x[1], reverse=True)[:top_n])
        
        fig, ax = plt.subplots(figsize=(width, height))
        
        labels = list(sorted_data.keys())
        values = list(sorted_data.values())
        
        primary_color = self.branding.get('primary_color', '#A100FF')
        
        if horizontal:
            bars = ax.barh(labels, values, color=primary_color)
            ax.set_xlabel(ylabel)
            ax.set_ylabel(xlabel)
            ax.invert_yaxis()
        else:
            bars = ax.bar(labels, values, color=primary_color)
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            plt.xticks(rotation=45, ha='right', fontsize=8)
        
        ax.set_title(title, fontsize=11, fontweight='bold', color=primary_color)
        
        for bar, val in zip(bars, values):
            if horizontal:
                ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                       f'{val:,}', va='center', fontsize=8)
            else:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                       f'{val:,}', ha='center', fontsize=8)
        
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        
        return Image(buf, width=width*inch, height=height*inch)
    
    def _create_kpi_table(self, kpis: List[Tuple[str, str]]) -> Table:
        """Create a KPI display table."""
        data = []
        row_values = []
        row_labels = []
        
        for value, label in kpis:
            row_values.append(Paragraph(str(value), self.styles['KPIValue']))
            row_labels.append(Paragraph(label, self.styles['KPILabel']))
        
        data.append(row_values)
        data.append(row_labels)
        
        col_width = 450 / len(kpis)
        table = Table(data, colWidths=[col_width] * len(kpis))
        
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
            ('BACKGROUND', (0, 0), (-1, -1), colors.whitesmoke),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ]))
        
        return table
    
    def _create_data_table(
        self,
        headers: List[str],
        data: List[List[Any]],
        col_widths: Optional[List[float]] = None
    ) -> Table:
        """Create a styled data table."""
        table_data = [headers] + data
        
        table = Table(table_data, colWidths=col_widths)
        
        primary_color = self._hex_to_reportlab(
            self.branding.get('primary_color', '#A100FF')
        )
        
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), primary_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.whitesmoke]),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        return table
    
    def generate_deployment_dashboard(
        self,
        stats: Dict[str, Any],
        reconciled: List[Dict[str, Any]]
    ) -> Path:
        """Generate the Deployment Progress Dashboard report."""
        filename = self._get_filename("deployment_dashboard")
        self.logger.info(f"Generating deployment dashboard: {filename}")
        
        doc = SimpleDocTemplate(
            str(filename),
            pagesize=A4,
            topMargin=35*mm,
            bottomMargin=25*mm,
            leftMargin=15*mm,
            rightMargin=15*mm
        )
        
        story = []
        
        story.append(Paragraph("Illumio Deployment Progress Dashboard", self.styles['CustomTitle']))
        story.append(Spacer(1, 10))
        
        coverage = stats.get('coverage_rate', 0)
        total_cmdb = stats.get('total_cmdb_servers', 0)
        deployed = stats.get('deployed_active', 0) + stats.get('deployed_offline', 0)
        pending = stats.get('not_deployed', 0)
        
        kpis = [
            (f"{coverage:.1f}%", "Coverage Rate"),
            (f"{total_cmdb:,}", "Total Servers"),
            (f"{deployed:,}", "Deployed"),
            (f"{pending:,}", "Pending")
        ]
        story.append(self._create_kpi_table(kpis))
        story.append(Spacer(1, 20))
        
        story.append(Paragraph("Deployment Status Distribution", self.styles['CustomHeading2']))
        
        status_data = {
            'Active': stats.get('deployed_active', 0),
            'Offline': stats.get('deployed_offline', 0),
            'Suspended': stats.get('deployed_suspended', 0),
            'Not Deployed': stats.get('not_deployed', 0),
            'Shadow IT': stats.get('not_in_cmdb', 0)
        }
        status_data = {k: v for k, v in status_data.items() if v > 0}
        
        if status_data:
            story.append(self._create_pie_chart(status_data, "Deployment Status"))
        
        story.append(Spacer(1, 20))
        
        by_env = stats.get('by_environment', {})
        if by_env:
            story.append(Paragraph("Coverage by Environment", self.styles['CustomHeading2']))
            
            env_coverage = {}
            for env, statuses in by_env.items():
                if env and env != 'Unknown':
                    total = sum(statuses.values())
                    deployed = statuses.get('deployed_active', 0) + statuses.get('deployed_offline', 0)
                    env_coverage[env] = deployed
            
            if env_coverage:
                story.append(self._create_bar_chart(
                    env_coverage,
                    "Deployed Servers by Environment",
                    ylabel="Servers"
                ))
        
        story.append(PageBreak())
        
        by_app = stats.get('by_application', {})
        if by_app:
            story.append(Paragraph("Least Covered Applications (Top 20)", self.styles['CustomHeading2']))
            
            app_gap = {}
            for app, statuses in by_app.items():
                if app and app != 'Unknown':
                    not_deployed = statuses.get('not_deployed', 0)
                    if not_deployed > 0:
                        app_gap[app] = not_deployed
            
            if app_gap:
                story.append(self._create_bar_chart(
                    app_gap,
                    "Servers Not Deployed by Application",
                    ylabel="Not Deployed",
                    horizontal=True,
                    top_n=20
                ))
        
        doc.build(story, onFirstPage=self._add_header_footer, 
                 onLaterPages=self._add_header_footer)
        
        self.logger.info(f"Deployment dashboard generated: {filename}")
        return filename
    
    def generate_agent_health_report(
        self,
        stats: Dict[str, Any],
        offline: List[Dict[str, Any]],
        suspended: List[Dict[str, Any]]
    ) -> Path:
        """Generate the Agent Health Status report."""
        filename = self._get_filename("agent_health")
        self.logger.info(f"Generating agent health report: {filename}")
        
        doc = SimpleDocTemplate(
            str(filename),
            pagesize=A4,
            topMargin=35*mm,
            bottomMargin=25*mm,
            leftMargin=15*mm,
            rightMargin=15*mm
        )
        
        story = []
        
        story.append(Paragraph("Agent Health Status Report", self.styles['CustomTitle']))
        story.append(Spacer(1, 10))
        
        total_agents = (
            stats.get('deployed_active', 0) + 
            stats.get('deployed_offline', 0) + 
            stats.get('deployed_suspended', 0)
        )
        online_rate = 0
        if total_agents > 0:
            online_rate = (stats.get('deployed_active', 0) / total_agents) * 100
        
        kpis = [
            (f"{online_rate:.1f}%", "Online Rate"),
            (f"{stats.get('deployed_active', 0):,}", "Active Agents"),
            (f"{len(offline):,}", "Offline"),
            (f"{len(suspended):,}", "Suspended")
        ]
        story.append(self._create_kpi_table(kpis))
        story.append(Spacer(1, 20))
        
        ven_status = stats.get('by_ven_status', {})
        if ven_status:
            ven_status_clean = {k: v for k, v in ven_status.items() if k and k != 'N/A'}
            if ven_status_clean:
                story.append(Paragraph("VEN Status Distribution", self.styles['CustomHeading2']))
                story.append(self._create_pie_chart(ven_status_clean, "VEN Status"))
                story.append(Spacer(1, 15))
        
        enforcement = stats.get('by_enforcement_mode', {})
        if enforcement:
            enforcement_clean = {k: v for k, v in enforcement.items() if k and k != 'N/A'}
            if enforcement_clean:
                story.append(Paragraph("Enforcement Mode Distribution", self.styles['CustomHeading2']))
                story.append(self._create_pie_chart(enforcement_clean, "Enforcement Mode"))
                story.append(Spacer(1, 15))
        
        versions = stats.get('by_ven_version', {})
        if versions:
            versions_clean = {k: v for k, v in versions.items() if k and k != 'N/A' and k != ''}
            if versions_clean:
                story.append(PageBreak())
                story.append(Paragraph("VEN Version Distribution", self.styles['CustomHeading2']))
                story.append(self._create_bar_chart(
                    versions_clean,
                    "Agents by VEN Version",
                    ylabel="Count",
                    horizontal=True
                ))
        
        if offline:
            story.append(PageBreak())
            story.append(Paragraph("Offline Agents (Sample)", self.styles['CustomHeading2']))
            
            headers = ['Hostname', 'IP', 'Last Heartbeat', 'Environment']
            table_data = []
            
            for agent in offline[:25]:
                table_data.append([
                    agent.get('hostname_normalized', '')[:30],
                    agent.get('illumio_primary_ip', ''),
                    agent.get('illumio_last_heartbeat', '')[:19],
                    agent.get('cmdb_environment', '') or agent.get('illumio_label_env', '')
                ])
            
            if table_data:
                story.append(self._create_data_table(
                    headers, 
                    table_data,
                    col_widths=[120, 80, 120, 100]
                ))
                
                if len(offline) > 25:
                    story.append(Paragraph(
                        f"... and {len(offline) - 25} more offline agents. See Excel export for full list.",
                        self.styles['CustomBody']
                    ))
        
        doc.build(story, onFirstPage=self._add_header_footer,
                 onLaterPages=self._add_header_footer)
        
        self.logger.info(f"Agent health report generated: {filename}")
        return filename
    
    def generate_gap_analysis_report(
        self,
        stats: Dict[str, Any],
        not_deployed: List[Dict[str, Any]],
        shadow_it: List[Dict[str, Any]]
    ) -> Path:
        """Generate the Gap Analysis report."""
        filename = self._get_filename("gap_analysis")
        self.logger.info(f"Generating gap analysis report: {filename}")
        
        doc = SimpleDocTemplate(
            str(filename),
            pagesize=A4,
            topMargin=35*mm,
            bottomMargin=25*mm,
            leftMargin=15*mm,
            rightMargin=15*mm
        )
        
        story = []
        
        story.append(Paragraph("Gap Analysis Report", self.styles['CustomTitle']))
        story.append(Spacer(1, 10))
        
        kpis = [
            (f"{len(not_deployed):,}", "Not Deployed"),
            (f"{len(shadow_it):,}", "Shadow IT"),
            (f"{stats.get('matched_by_hostname', 0):,}", "Matched"),
            (f"{stats.get('coverage_rate', 0):.1f}%", "Coverage")
        ]
        story.append(self._create_kpi_table(kpis))
        story.append(Spacer(1, 20))
        
        if not_deployed:
            story.append(Paragraph("Servers Not Deployed (Top 50)", self.styles['CustomHeading2']))
            
            headers = ['Hostname', 'IP', 'OS', 'Environment', 'Application']
            table_data = []
            
            for server in not_deployed[:50]:
                table_data.append([
                    server.get('cmdb_name', '')[:25],
                    server.get('cmdb_ip_address', ''),
                    server.get('cmdb_os', '')[:15],
                    server.get('cmdb_environment', '')[:15],
                    server.get('cmdb_application', '')[:20]
                ])
            
            if table_data:
                story.append(self._create_data_table(
                    headers,
                    table_data,
                    col_widths=[90, 80, 70, 80, 100]
                ))
                
                if len(not_deployed) > 50:
                    story.append(Paragraph(
                        f"... and {len(not_deployed) - 50} more servers. See Excel export for full list.",
                        self.styles['CustomBody']
                    ))
        
        if shadow_it:
            story.append(PageBreak())
            story.append(Paragraph("Shadow IT - Workloads Not in CMDB (Top 50)", self.styles['CustomHeading2']))
            
            headers = ['Hostname', 'IP', 'OS', 'App Label', 'Env Label']
            table_data = []
            
            for workload in shadow_it[:50]:
                table_data.append([
                    workload.get('illumio_hostname', '')[:25],
                    workload.get('illumio_primary_ip', ''),
                    workload.get('illumio_os_type', '')[:15],
                    workload.get('illumio_label_app', '')[:20],
                    workload.get('illumio_label_env', '')[:15]
                ])
            
            if table_data:
                story.append(self._create_data_table(
                    headers,
                    table_data,
                    col_widths=[90, 80, 70, 100, 80]
                ))
        
        doc.build(story, onFirstPage=self._add_header_footer,
                 onLaterPages=self._add_header_footer)
        
        self.logger.info(f"Gap analysis report generated: {filename}")
        return filename
    
    def generate_executive_summary(
        self,
        stats: Dict[str, Any],
        cmdb_available: bool = True
    ) -> Path:
        """Generate the Executive Summary report (1 page)."""
        filename = self._get_filename("executive_summary")
        self.logger.info(f"Generating executive summary: {filename}")
        
        doc = SimpleDocTemplate(
            str(filename),
            pagesize=A4,
            topMargin=35*mm,
            bottomMargin=25*mm,
            leftMargin=15*mm,
            rightMargin=15*mm
        )
        
        story = []
        
        story.append(Paragraph("Illumio Deployment - Executive Summary", self.styles['CustomTitle']))
        story.append(Paragraph(
            f"Report Date: {datetime.now().strftime('%d-%m-%Y %H:%M')}",
            self.styles['CustomBody']
        ))
        story.append(Spacer(1, 15))
        
        coverage = stats.get('coverage_rate', 0)
        active_rate = stats.get('active_rate', 0)
        enforcement = stats.get('enforcement_rate', 0)
        
        kpis = [
            (f"{coverage:.1f}%", "Coverage"),
            (f"{active_rate:.1f}%", "Active"),
            (f"{enforcement:.1f}%", "Enforced"),
            (f"{stats.get('total_illumio_workloads', 0):,}", "Workloads")
        ]
        story.append(self._create_kpi_table(kpis))
        story.append(Spacer(1, 20))
        
        story.append(Paragraph("Deployment Overview", self.styles['CustomHeading2']))
        
        overview_data = [
            ['Metric', 'Count'],
            ['Total CMDB Servers', f"{stats.get('total_cmdb_servers', 0):,}"],
            ['Total Illumio Workloads', f"{stats.get('total_illumio_workloads', 0):,}"],
            ['Deployed & Active', f"{stats.get('deployed_active', 0):,}"],
            ['Deployed & Offline', f"{stats.get('deployed_offline', 0):,}"],
            ['Deployed & Suspended', f"{stats.get('deployed_suspended', 0):,}"],
            ['Not Yet Deployed', f"{stats.get('not_deployed', 0):,}"],
            ['Shadow IT (Not in CMDB)', f"{stats.get('not_in_cmdb', 0):,}"]
        ]
        
        table = Table(overview_data, colWidths=[250, 100])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self._hex_to_reportlab(
                self.branding.get('primary_color', '#A100FF')
            )),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.whitesmoke]),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(table)
        story.append(Spacer(1, 20))
        
        story.append(Paragraph("Key Findings", self.styles['CustomHeading2']))
        
        findings = []
        
        if coverage >= 80:
            findings.append("✓ Deployment coverage is on track (≥80%)")
        elif coverage >= 50:
            findings.append("⚠ Deployment coverage needs acceleration (50-80%)")
        else:
            findings.append("✗ Deployment coverage is behind schedule (<50%)")
        
        offline_count = stats.get('deployed_offline', 0)
        if offline_count > 0:
            findings.append(f"⚠ {offline_count:,} agents are currently offline")
        
        shadow_count = stats.get('not_in_cmdb', 0)
        if shadow_count > 0:
            findings.append(f"⚠ {shadow_count:,} workloads not found in CMDB (potential shadow IT)")
        
        not_deployed = stats.get('not_deployed', 0)
        if not_deployed > 0:
            findings.append(f"• {not_deployed:,} servers pending deployment")
        
        if enforcement < 50:
            findings.append("⚠ Enforcement rate is low - most agents in visibility mode")
        
        if not cmdb_available:
            findings.append("⚠ CMDB data unavailable - report based on Illumio data only")
        
        for finding in findings:
            story.append(Paragraph(f"  {finding}", self.styles['CustomBody']))
        
        story.append(Spacer(1, 15))
        story.append(Paragraph("Recommendations", self.styles['CustomHeading2']))
        
        recommendations = []
        
        if not_deployed > 0:
            recommendations.append("1. Prioritize deployment to remaining servers, starting with production environments")
        
        if offline_count > 0:
            recommendations.append("2. Investigate and remediate offline agents to ensure continuous protection")
        
        if shadow_count > 0:
            recommendations.append("3. Review shadow IT workloads and update CMDB accordingly")
        
        if enforcement < 50:
            recommendations.append("4. Plan transition from visibility to enforcement mode for stable workloads")
        
        if not recommendations:
            recommendations.append("Continue monitoring and maintain current deployment pace")
        
        for rec in recommendations:
            story.append(Paragraph(f"  {rec}", self.styles['CustomBody']))
        
        doc.build(story, onFirstPage=self._add_header_footer,
                 onLaterPages=self._add_header_footer)
        
        self.logger.info(f"Executive summary generated: {filename}")
        return filename
