import smtplib
import ssl
import os
import pandas as pd
import psycopg2
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from io import BytesIO

# Try to load .env for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Database configuration
def get_config(key):
    return os.getenv(key)

def get_connection():
    return psycopg2.connect(
        host=get_config("DB_HOST"),
        user=get_config("DB_USER"),
        password=get_config("DB_PASSWORD"),
        database=get_config("DB_NAME"),
        port=get_config("DB_PORT") or 5432
    )

def load_unbilled_data():
    """Load unbilled CN data from database"""
    conn = get_connection()
    query = """
        SELECT
            cn_no, cn_date, branch, billing_party, bill_no,
            vehicle_no, qty, basic_freight, pod_receipt_no
        FROM cn_data
        WHERE is_active = 'Yes'
        ORDER BY cn_date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df['cn_date'] = pd.to_datetime(df['cn_date'])

    # Filter unbilled CNs with POD received
    unbilled_df = df[
        (df['bill_no'].isna() | (df['bill_no'] == '') | (df['bill_no'].astype(str).str.strip() == '')) &
        (df['pod_receipt_no'].notna()) &
        (df['pod_receipt_no'] != '') &
        (df['pod_receipt_no'].astype(str).str.strip() != '')
    ].copy()

    return unbilled_df

def get_parent_group(party):
    """Get parent company group for a billing party"""
    party_lower = party.lower() if party else ""
    if 'honda' in party_lower:
        return 'Honda'
    elif 'mahindra' in party_lower or 'mstc' in party_lower:
        return 'M & M'
    elif 'toyota' in party_lower or 'transystem' in party_lower:
        return 'Toyota'
    elif 'glovis' in party_lower:
        return 'Glovis'
    elif 'tata' in party_lower:
        return 'Tata'
    elif 'skoda' in party_lower or 'volkswagen' in party_lower:
        return 'Skoda VW'
    elif 'john deere' in party_lower:
        return 'John Deere'
    elif 'valuedrive' in party_lower or 'spinny' in party_lower:
        return 'ValueDrive'
    else:
        return 'Others'

def format_currency(val):
    """Format currency in Lakhs"""
    if abs(val) >= 100000:
        return f"₹{val/100000:.2f}L"
    elif abs(val) >= 1000:
        return f"₹{val/1000:.2f}K"
    return f"₹{val:.0f}"

def generate_summary_html(unbilled_df):
    """Generate HTML summary table for email"""
    if len(unbilled_df) == 0:
        return "<p>No unbilled CNs with POD received found.</p>"

    # Add group column
    unbilled_df['Group'] = unbilled_df['billing_party'].apply(get_parent_group)
    unbilled_df['cn_month'] = unbilled_df['cn_date'].dt.to_period('M')

    # Get last 6 months
    valid_months = unbilled_df[unbilled_df['cn_month'].notna()]['cn_month'].unique()
    months_sorted = sorted(valid_months, reverse=True)[:6]
    month_labels = [datetime.strptime(str(m), "%Y-%m").strftime("%b'%y") for m in months_sorted]

    # Build summary by party
    summary_data = []
    parties = unbilled_df['billing_party'].dropna().unique()

    for party in parties:
        party_data = unbilled_df[unbilled_df['billing_party'] == party]
        row = {
            'Billing Party': party,
            'Group': get_parent_group(party),
            'Total CN': len(party_data),
            'Total Qty': int(party_data['qty'].sum()),
            'Total Amount': party_data['basic_freight'].sum()
        }
        summary_data.append(row)

    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values('Total Amount', ascending=False)

    # Calculate totals
    total_cn = len(unbilled_df)
    total_qty = int(unbilled_df['qty'].sum())
    total_amount = unbilled_df['basic_freight'].sum()

    # Build HTML
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f5f5f5; padding: 20px; }}
            .container {{ max-width: 900px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #1e3a5f; text-align: center; }}
            h2 {{ color: #3b82f6; border-bottom: 2px solid #3b82f6; padding-bottom: 10px; }}
            .summary-cards {{ display: flex; justify-content: space-around; margin: 20px 0; }}
            .card {{ background: linear-gradient(135deg, #1e3a5f, #2d4a6f); color: white; padding: 20px; border-radius: 10px; text-align: center; min-width: 150px; }}
            .card.green {{ background: linear-gradient(135deg, #065f46, #047857); }}
            .card-title {{ font-size: 12px; color: #94a3b8; margin-bottom: 5px; }}
            .card-value {{ font-size: 28px; font-weight: bold; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th {{ background: #1e3a5f; color: white; padding: 12px 8px; text-align: left; font-weight: 600; }}
            td {{ padding: 10px 8px; border-bottom: 1px solid #e2e8f0; }}
            tr:nth-child(even) {{ background-color: #f8fafc; }}
            tr:hover {{ background-color: #e2e8f0; }}
            .amount {{ text-align: right; font-weight: 500; }}
            .total-row {{ background: #065f46 !important; color: white; font-weight: bold; }}
            .total-row td {{ border-bottom: none; }}
            .footer {{ margin-top: 30px; text-align: center; color: #64748b; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Swift Billing - Unbilled CN Report</h1>
            <p style="text-align: center; color: #64748b;">Report Generated: {datetime.now().strftime('%d-%m-%Y %H:%M')}</p>

            <div class="summary-cards">
                <div class="card">
                    <div class="card-title">Total No. of CN</div>
                    <div class="card-value">{total_cn:,}</div>
                </div>
                <div class="card">
                    <div class="card-title">Total Qty</div>
                    <div class="card-value">{total_qty:,}</div>
                </div>
                <div class="card green">
                    <div class="card-title">Total Unbilled Amount</div>
                    <div class="card-value">{format_currency(total_amount)}</div>
                </div>
            </div>

            <h2>Party-wise Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Billing Party</th>
                        <th>Group</th>
                        <th style="text-align: right;">No. of CN</th>
                        <th style="text-align: right;">Qty</th>
                        <th style="text-align: right;">Unbilled Amount</th>
                    </tr>
                </thead>
                <tbody>
    """

    for _, row in summary_df.iterrows():
        html += f"""
                    <tr>
                        <td>{row['Billing Party']}</td>
                        <td>{row['Group']}</td>
                        <td style="text-align: right;">{row['Total CN']:,}</td>
                        <td style="text-align: right;">{row['Total Qty']:,}</td>
                        <td class="amount">₹{row['Total Amount']:,.0f}</td>
                    </tr>
        """

    html += f"""
                    <tr class="total-row">
                        <td colspan="2"><strong>Grand Total</strong></td>
                        <td style="text-align: right;"><strong>{total_cn:,}</strong></td>
                        <td style="text-align: right;"><strong>{total_qty:,}</strong></td>
                        <td class="amount"><strong>₹{total_amount:,.0f}</strong></td>
                    </tr>
                </tbody>
            </table>

            <div class="footer">
                <p>This is an automated report from Swift Billing Dashboard</p>
                <p>Dashboard: <a href="https://swiftbilling-iocaaipkggpttjxjo9lgcm.streamlit.app/">View Live Dashboard</a></p>
            </div>
        </div>
    </body>
    </html>
    """

    return html

def generate_excel_report(unbilled_df):
    """Generate Excel report as bytes"""
    if len(unbilled_df) == 0:
        return None

    # Prepare data for Excel
    unbilled_df['Group'] = unbilled_df['billing_party'].apply(get_parent_group)

    # Summary by party
    summary = unbilled_df.groupby(['billing_party', 'Group']).agg({
        'cn_no': 'count',
        'qty': 'sum',
        'basic_freight': 'sum'
    }).reset_index()
    summary.columns = ['Billing Party', 'Group', 'No. of CN', 'Qty', 'Unbilled Amount']
    summary = summary.sort_values('Unbilled Amount', ascending=False)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary.to_excel(writer, sheet_name='Summary', index=False)
        unbilled_df[['cn_no', 'cn_date', 'branch', 'billing_party', 'vehicle_no', 'qty', 'basic_freight', 'pod_receipt_no']].to_excel(
            writer, sheet_name='Details', index=False
        )

    output.seek(0)
    return output.getvalue()

def send_email_report():
    """Send the unbilled CN report via email"""
    # Email configuration from environment variables
    smtp_server = get_config("SMTP_SERVER") or "smtp.gmail.com"
    smtp_port = int(get_config("SMTP_PORT") or 587)
    sender_email = get_config("SENDER_EMAIL")
    sender_password = get_config("SENDER_PASSWORD")
    recipients = get_config("REPORT_RECIPIENTS")  # Comma-separated emails

    if not all([sender_email, sender_password, recipients]):
        print("ERROR: Email configuration missing. Please set SENDER_EMAIL, SENDER_PASSWORD, and REPORT_RECIPIENTS in .env")
        return False

    recipient_list = [email.strip() for email in recipients.split(',')]

    try:
        # Load data and generate reports
        print("Loading unbilled CN data...")
        unbilled_df = load_unbilled_data()

        print(f"Found {len(unbilled_df)} unbilled CNs")

        # Generate HTML content
        html_content = generate_summary_html(unbilled_df)

        # Generate Excel attachment
        excel_data = generate_excel_report(unbilled_df)

        # Create email message
        message = MIMEMultipart("alternative")
        message["Subject"] = f"Swift Billing - Unbilled CN Report ({datetime.now().strftime('%d-%m-%Y')})"
        message["From"] = sender_email
        message["To"] = ", ".join(recipient_list)

        # Attach HTML content
        html_part = MIMEText(html_content, "html")
        message.attach(html_part)

        # Attach Excel file
        if excel_data:
            excel_attachment = MIMEBase("application", "octet-stream")
            excel_attachment.set_payload(excel_data)
            encoders.encode_base64(excel_attachment)
            excel_attachment.add_header(
                "Content-Disposition",
                f"attachment; filename=unbilled_cn_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
            )
            message.attach(excel_attachment)

        # Send email
        print(f"Sending email to: {', '.join(recipient_list)}")
        context = ssl.create_default_context()

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_list, message.as_string())

        print("Email sent successfully!")
        return True

    except Exception as e:
        print(f"ERROR sending email: {str(e)}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("Swift Billing - Automated Email Report")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    success = send_email_report()

    if success:
        print("\nReport sent successfully!")
    else:
        print("\nFailed to send report. Check configuration and try again.")
