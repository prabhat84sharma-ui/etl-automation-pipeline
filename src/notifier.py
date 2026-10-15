"""
Pipeline Alerting & Observability Notifier.
Supports Slack Webhook notifications (Block Kit formatting),
Automated HTML/Plaintext SMTP emails, and executive pipeline run summary generation.

Author: Prabhat Sharma (prabhat84sharma-ui)
"""

import os
import smtplib
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional
import requests

from src.config import get_config

logger = logging.getLogger(__name__)


class AlertNotificationError(Exception):
    """Raised when critical notification dispatch fails."""
    pass


class PipelineNotifier:
    """
    Dispatches alerts and run summaries across Slack and Email channels.
    """

    def __init__(self):
        self.config = get_config()
        self.alert_cfg = self.config.alerting
        self.slack_cfg = self.alert_cfg.get("slack", {})
        self.email_cfg = self.alert_cfg.get("email", {})
        self.env = self.config.env_name

    # =========================================================================
    # Summary Generators
    # =========================================================================
    @staticmethod
    def generate_summary_text(
        run_id: str,
        pipeline_name: str,
        environment: str,
        status: str,
        duration_seconds: float,
        records_extracted: int,
        records_loaded: int,
        dq_score: float,
        error_message: Optional[str] = None,
    ) -> str:
        """Generates a clean plaintext operational summary."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        divider = "=" * 65
        lines = [
            divider,
            f"  ETL PIPELINE RUN SUMMARY: {pipeline_name.upper()}",
            divider,
            f"  Run ID:            {run_id}",
            f"  Environment:       {environment.upper()}",
            f"  Execution Status:  {status.upper()}",
            f"  Timestamp:         {timestamp}",
            f"  Duration:          {duration_seconds:.2f} seconds",
            f"  Records Ingested:  {records_extracted:,}",
            f"  Records Loaded:    {records_loaded:,}",
            f"  DQ Health Score:   {dq_score:.2f}%",
            divider,
        ]
        if error_message:
            lines.append(f"  CRITICAL ERROR: {error_message}")
            lines.append(divider)

        return "\n".join(lines)

    @staticmethod
    def generate_summary_html(
        run_id: str,
        pipeline_name: str,
        environment: str,
        status: str,
        duration_seconds: float,
        records_extracted: int,
        records_loaded: int,
        dq_score: float,
        error_message: Optional[str] = None,
    ) -> str:
        """Generates an executive-ready responsive HTML email body."""
        status_colors = {
            "SUCCESS": "#28a745",
            "WARNING": "#ffc107",
            "FAIL": "#dc3545",
            "FAILED": "#dc3545",
        }
        accent_color = status_colors.get(status.upper(), "#007bff")

        error_section = ""
        if error_message:
            error_section = f"""
            <div style="background-color: #f8d7da; border-left: 5px solid #dc3545; padding: 12px; margin-top: 15px; border-radius: 4px;">
                <h4 style="color: #721c24; margin: 0 0 6px 0;">Error Details:</h4>
                <pre style="color: #721c24; font-family: monospace; white-space: pre-wrap; margin: 0;">{error_message}</pre>
            </div>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; }}
                .card {{ max-width: 650px; margin: 0 auto; background: #ffffff; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); overflow: hidden; }}
                .header {{ background-color: {accent_color}; color: #ffffff; padding: 20px; text-align: center; }}
                .content {{ padding: 25px; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #e9ecef; font-size: 14px; }}
                th {{ background-color: #f8f9fa; color: #495057; }}
                .badge {{ display: inline-block; padding: 4px 10px; font-weight: bold; border-radius: 4px; color: #fff; background-color: {accent_color}; }}
                .footer {{ text-align: center; font-size: 12px; color: #6c757d; padding: 15px; border-top: 1px solid #e9ecef; }}
            </style>
        </head>
        <body>
            <div class="card">
                <div class="header">
                    <h2 style="margin: 0;">ETL Pipeline Run Notification</h2>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">Pipeline: {pipeline_name} | {environment.upper()}</p>
                </div>
                <div class="content">
                    <table>
                        <tr><th>Run ID</th><td><strong>{run_id}</strong></td></tr>
                        <tr><th>Overall Status</th><td><span class="badge">{status.upper()}</span></td></tr>
                        <tr><th>Execution Duration</th><td>{duration_seconds:.2f} seconds</td></tr>
                        <tr><th>Records Extracted</th><td>{records_extracted:,}</td></tr>
                        <tr><th>Records Loaded</th><td>{records_loaded:,}</td></tr>
                        <tr><th>Data Quality Score</th><td><strong>{dq_score:.2f}%</strong></td></tr>
                        <tr><th>Completed At</th><td>{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
                    </table>
                    {error_section}
                </div>
                <div class="footer">
                    Automated Alert from Prabhat Sharma's ETL Automation Platform &bull; Apache Airflow &amp; PostgreSQL
                </div>
            </div>
        </body>
        </html>
        """
        return html

    # =========================================================================
    # Slack Webhook Dispatcher
    # =========================================================================
    def send_slack_alert(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        duration_seconds: float,
        records_loaded: int,
        dq_score: float,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Dispatches rich Slack Block Kit notification via Incoming Webhook.
        """
        webhook_url = self.slack_cfg.get("webhook_url", "")
        if not self.slack_cfg.get("enabled", True) or not webhook_url or "EXAMPLE" in webhook_url:
            logger.info("Slack notifications disabled or mock URL detected. Logging payload locally.")
            summary = self.generate_summary_text(
                run_id, pipeline_name, self.env, status, duration_seconds, 0, records_loaded, dq_score, error_message
            )
            logger.info("Slack Alert Preview:\n%s", summary)
            return True

        status_emojis = {
            "SUCCESS": "🟢",
            "WARNING": "🟡",
            "FAIL": "🔴",
            "FAILED": "🔴",
        }
        emoji = status_emojis.get(status.upper(), "ℹ️")

        payload = {
            "text": f"{emoji} ETL Pipeline {pipeline_name} [{self.env.upper()}]: {status.upper()}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} ETL Pipeline Alert: {pipeline_name}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Environment:*\n`{self.env.upper()}`"},
                        {"type": "mrkdwn", "text": f"*Status:*\n*{status.upper()}*"},
                        {"type": "mrkdwn", "text": f"*Run ID:*\n`{run_id}`"},
                        {"type": "mrkdwn", "text": f"*Duration:*\n{duration_seconds:.2f}s"},
                        {"type": "mrkdwn", "text": f"*Records Loaded:*\n{records_loaded:,}"},
                        {"type": "mrkdwn", "text": f"*DQ Quality Index:*\n*{dq_score:.2f}%*"},
                    ],
                },
            ],
        }

        if error_message:
            payload["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*:warning: Critical Error:*\n```{error_message[:1000]}```",
                },
            })

        try:
            resp = requests.post(webhook_url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("Successfully dispatched Slack alert for run %s.", run_id)
                return True
            logger.warning("Slack webhook returned non-200 status code: %d", resp.status_code)
            return False
        except Exception as e:
            logger.warning("Failed to dispatch Slack alert: %s", str(e))
            return False

    # =========================================================================
    # SMTP Email Dispatcher
    # =========================================================================
    def send_email_alert(
        self,
        subject: str,
        html_body: str,
        recipients: Optional[List[str]] = None,
    ) -> bool:
        """
        Dispatches multipart HTML/plain-text email via standard SMTP.
        """
        if not self.email_cfg.get("enabled", False):
            logger.info("Email alerting is disabled in config. Skipping email dispatch.")
            return True

        smtp_host = self.email_cfg.get("smtp_host", "smtp.gmail.com")
        smtp_port = int(self.email_cfg.get("smtp_port", 587))
        sender = self.email_cfg.get("sender", "alerts@prabhatsharma.dev")
        target_recipients = recipients or self.email_cfg.get("recipients", [])

        if not target_recipients:
            logger.warning("No email recipients configured.")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(target_recipients)
        msg.attach(MIMEText(html_body, "html"))

        try:
            logger.info("Connecting to SMTP server: %s:%d...", smtp_host, smtp_port)
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
                server.starttls()
                # Optional auth if password provided in environment
                pwd = os.environ.get("SMTP_PASSWORD")
                if pwd:
                    server.login(sender, pwd)
                server.sendmail(sender, target_recipients, msg.as_string())
            logger.info("Successfully sent email alert to: %s", target_recipients)
            return True
        except Exception as e:
            logger.warning("Failed to dispatch email alert: %s (Continuing)", str(e))
            return False

    # =========================================================================
    # Master Unified Alert Dispatcher
    # =========================================================================
    def notify_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str,
        status: str,
        duration_seconds: float,
        records_extracted: int,
        records_loaded: int,
        dq_score: float,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Broadcasts status notifications across all configured alerting channels.
        """
        logger.info("Broadcasting pipeline completion notifications for run %s...", run_id)

        # 1. Slack Alert
        self.send_slack_alert(
            run_id=run_id,
            pipeline_name=pipeline_name,
            status=status,
            duration_seconds=duration_seconds,
            records_loaded=records_loaded,
            dq_score=dq_score,
            error_message=error_message,
        )

        # 2. Email Alert
        subject = f"[{self.env.upper()}] ETL Pipeline {pipeline_name}: {status.upper()}"
        html_body = self.generate_summary_html(
            run_id=run_id,
            pipeline_name=pipeline_name,
            environment=self.env,
            status=status,
            duration_seconds=duration_seconds,
            records_extracted=records_extracted,
            records_loaded=records_loaded,
            dq_score=dq_score,
            error_message=error_message,
        )
        self.send_email_alert(subject=subject, html_body=html_body)
