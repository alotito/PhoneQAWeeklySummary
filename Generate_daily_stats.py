# email_agent_score_report.py

import sys
import os
import configparser
import smtplib
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, List, Tuple
from datetime import datetime

try:
    import pyodbc
except ImportError:
    print("FATAL: The 'pyodbc' library is not installed. Please install it using 'pip install pyodbc'", file=sys.stderr)
    sys.exit(1)

CONFIG_FILE_NAME = "config.ini"

def get_db_connection(config: configparser.ConfigParser) -> Optional[pyodbc.Connection]:
    """Reads config and establishes a database connection."""
    try:
        db_config = config['Database']
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_config['Server']};DATABASE={db_config['Database']};UID={db_config['User']};PWD={db_config['Password']};"
        print(f"Connecting to {db_config['Database']} on {db_config['Server']}...")
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"ERROR: Could not connect to the database.\n{e}")
        return None

def fetch_agent_stats(conn: pyodbc.Connection) -> Optional[Tuple[List[pyodbc.Row], Optional[datetime]]]:
    """Runs the aggregation query for the most recent date and returns the results and the date."""
    
    sql_query = """
    WITH LatestDate AS (
        SELECT MAX(CAST(AnalysisDateTime AS DATE)) AS MaxDate
        FROM dbo.IndividualCallAnalyses
    )
    SELECT
        A.AgentName,
        SUM(CASE WHEN IEI.Finding = 'Positive' THEN 1 ELSE 0 END) AS PositiveFindings,
        SUM(CASE WHEN IEI.Finding = 'Negative' THEN 1 ELSE 0 END) AS NegativeFindings,
        SUM(CASE WHEN IEI.Finding = 'Neutral' THEN 1 ELSE 0 END) AS NeutralFindings,
        COUNT(IEI.Finding) AS TotalFindings,
        -- Calculate the score, ensuring floating point division and handling divide-by-zero
        (
            (SUM(CASE WHEN IEI.Finding = 'Positive' THEN 1.0 ELSE 0 END) + (SUM(CASE WHEN IEI.Finding = 'Neutral' THEN 1.0 ELSE 0 END) / 2.0))
            / NULLIF(COUNT(IEI.Finding), 0)
        ) * 100 AS ScorePercentage,
        (SELECT MaxDate FROM LatestDate) as ReportDate
    FROM
        dbo.IndividualEvaluationItems AS IEI
    JOIN
        dbo.IndividualCallAnalyses AS ICA ON IEI.AnalysisID = ICA.AnalysisID
    JOIN
        dbo.Agents AS A ON ICA.AgentID = A.AgentID
    WHERE
        CAST(ICA.AnalysisDateTime AS DATE) = (SELECT MaxDate FROM LatestDate)
    GROUP BY
        A.AgentName
    HAVING COUNT(IEI.Finding) > 0
    -- =======================================================
    -- === MODIFIED SORT ORDER
    -- =======================================================
    ORDER BY
        A.AgentName;
    """
    try:
        print("Fetching statistics for the most recent analysis date...")
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No data found for the most recent analysis date.")
            return None, None

        report_date = rows[0].ReportDate
        print(f"Found statistics for {len(rows)} agents on {report_date.strftime('%Y-%m-%d')}.")
        return rows, report_date
    except Exception as e:
        print(f"ERROR: An error occurred while running the query: {e}")
        return None, None

def create_html_report(rows: List[pyodbc.Row], report_date: datetime) -> str:
    """Creates a formatted HTML string from the query results."""
    print("Generating HTML report...")
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; }}
            table {{ border-collapse: collapse; width: 95%; margin: 20px auto; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
            th, td {{ border: 1px solid #dddddd; text-align: left; padding: 12px; }}
            th {{ background-color: #004a99; color: white; font-weight: bold; text-align: center; }}
            tr:nth-child(even) {{ background-color: #f8f8f8; }}
            tr:hover {{ background-color: #f1f1f1; }}
            h2 {{ text-align: center; color: #004a99; }}
            p {{ text-align: center; color: #666; }}
            .score {{ font-weight: bold; background-color: #eaf3ff; }}
            .positive {{ color: green; }}
            .negative {{ color: red; }}
        </style>
    </head>
    <body>
        <h2>Agent QA Score Report</h2>
        <p>Displaying Findings for: {report_date.strftime('%A, %B %d, %Y')}</p>
        <table>
            <thead>
                <tr>
                    <th>Agent Name</th>
                    <th>Score</th>
                    <th>Positive</th>
                    <th>Negative</th>
                    <th>Neutral</th>
                    <th>Total Reviewed</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for row in rows:
        score_str = f"{row.ScorePercentage:.1f}%" if row.ScorePercentage is not None else "N/A"
        html += f"""
        <tr>
            <td>{row.AgentName}</td>
            <td style="text-align: center;" class="score">{score_str}</td>
            <td style="text-align: center;" class="positive">{row.PositiveFindings}</td>
            <td style="text-align: center;" class="negative">{row.NegativeFindings}</td>
            <td style="text-align: center;">{row.NeutralFindings}</td>
            <td style="text-align: center;">{row.TotalFindings}</td>
        </tr>
        """
    
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    print("HTML report generated successfully.")
    return html

def send_email(config: configparser.ConfigParser, html_report: str, report_date: datetime):
    """Constructs and sends an email with the HTML report."""
    try:
        smtp_cfg = config['SMTP']
        email_cfg = config['Report Emails']

        from_addr = email_cfg.get('FROM', 'noreply@yourcompany.com')
        to_addrs = [addr.strip() for addr in email_cfg.get('TO', '').split(';') if addr.strip()]
        cc_addrs = [addr.strip() for addr in email_cfg.get('CC', '').split(';') if addr.strip()]
        
        if not to_addrs:
            print("ERROR: No 'TO' recipients found in [Report Emails] section of config.ini.")
            return

        recipients = to_addrs + cc_addrs
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Agent QA Score Report for {report_date.strftime('%Y-%m-%d')}"
        msg['From'] = from_addr
        msg['To'] = ', '.join(to_addrs)
        if cc_addrs:
            msg['Cc'] = ', '.join(cc_addrs)
            
        msg.attach(MIMEText(html_report, 'html'))
        
        pwd = base64.b64decode(smtp_cfg['Password_B64'].encode('utf-8')).decode('utf-8')

        print(f"Connecting to SMTP server: {smtp_cfg['Server']}:{smtp_cfg['Port']}...")
        with smtplib.SMTP(smtp_cfg['Server'], int(smtp_cfg['Port'])) as server:
            if smtp_cfg.getboolean('UseSTARTTLS', True):
                server.starttls()
            server.login(smtp_cfg['UID'], pwd)
            print(f"Sending email to: {recipients}...")
            server.sendmail(from_addr, recipients, msg.as_string())
            print("Email sent successfully!")
            
    except Exception as e:
        print(f"ERROR: Failed to send email: {e}")

def main():
    """Main execution function."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, CONFIG_FILE_NAME)
    if not os.path.exists(config_path):
        print(f"CRITICAL: Configuration file not found at '{config_path}'.")
        return

    config = configparser.ConfigParser()
    config.read(config_path)
    
    conn = get_db_connection(config)
    if not conn:
        return
        
    try:
        report_data, report_date = fetch_agent_stats(conn)
        if report_data and report_date:
            html_content = create_html_report(report_data, report_date)
            send_email(config, html_content, report_date)
        else:
            print("No data found to generate a report.")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()