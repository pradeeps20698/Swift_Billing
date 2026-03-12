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

def filter_data_by_parties(unbilled_df, include_parties=None, exclude_parties=None):
    """Filter data based on party names"""
    filtered_df = unbilled_df.copy()

    if include_parties:
        # Include only parties containing these keywords
        mask = filtered_df['billing_party'].str.lower().str.contains('|'.join([p.lower() for p in include_parties]), na=False)
        filtered_df = filtered_df[mask]

    if exclude_parties:
        # Exclude parties containing these keywords
        mask = ~filtered_df['billing_party'].str.lower().str.contains('|'.join([p.lower() for p in exclude_parties]), na=False)
        filtered_df = filtered_df[mask]

    return filtered_df

def generate_summary_html(unbilled_df):
    """Generate HTML summary table for email with grouping"""
    if len(unbilled_df) == 0:
        return "<p>No unbilled CNs with POD received found.</p>"

    # Add group column
    unbilled_df['Group'] = unbilled_df['billing_party'].apply(get_parent_group)

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

    # Calculate totals
    total_cn = len(unbilled_df)
    total_qty = int(unbilled_df['qty'].sum())
    total_amount = unbilled_df['basic_freight'].sum()

    # Build grouped rows with subtotals (same logic as dashboard)
    grouped_rows = []
    single_parties = []
    group_order = ['M & M', 'Toyota', 'Glovis', 'Tata', 'Honda', 'Skoda VW', 'John Deere', 'ValueDrive']

    for group in group_order:
        group_data = summary_df[summary_df['Group'] == group].sort_values('Total Amount', ascending=False)
        if len(group_data) > 1:
            # Multiple parties - show with subtotal
            for _, row in group_data.iterrows():
                grouped_rows.append({
                    'Billing Party': row['Billing Party'],
                    'No. of CN': row['Total CN'],
                    'Qty': row['Total Qty'],
                    'Amount': row['Total Amount'],
                    'is_subtotal': False
                })
            # Add group subtotal
            grouped_rows.append({
                'Billing Party': f"{group} - Total",
                'No. of CN': int(group_data['Total CN'].sum()),
                'Qty': int(group_data['Total Qty'].sum()),
                'Amount': group_data['Total Amount'].sum(),
                'is_subtotal': True
            })
        elif len(group_data) == 1:
            # Single party - add to single parties list
            row = group_data.iloc[0]
            single_parties.append({
                'Billing Party': row['Billing Party'],
                'No. of CN': row['Total CN'],
                'Qty': row['Total Qty'],
                'Amount': row['Total Amount'],
                'is_subtotal': False
            })

    # Add Others group to single parties
    others_data = summary_df[summary_df['Group'] == 'Others'].sort_values('Total Amount', ascending=False)
    for _, row in others_data.iterrows():
        single_parties.append({
            'Billing Party': row['Billing Party'],
            'No. of CN': row['Total CN'],
            'Qty': row['Total Qty'],
            'Amount': row['Total Amount'],
            'is_subtotal': False
        })

    # Sort single parties by amount and add to grouped_rows
    single_parties_sorted = sorted(single_parties, key=lambda x: x['Amount'], reverse=True)
    grouped_rows.extend(single_parties_sorted)

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
            .card {{ background: #1e3a5f; color: #ffffff; padding: 20px; border-radius: 10px; text-align: center; min-width: 150px; }}
            .card.green {{ background: #065f46; }}
            .card-title {{ font-size: 12px; color: #94a3b8; margin-bottom: 8px; }}
            .card-value {{ font-size: 28px; font-weight: bold; color: #ffffff; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; border: 2px solid #1e3a5f; }}
            th {{ background: #1e3a5f; color: white; padding: 12px 8px; text-align: left; font-weight: 600; border: 1px solid #1e3a5f; }}
            td {{ padding: 10px 8px; border: 1px solid #cbd5e1; color: #333333; }}
            .grand-total {{ background: #065f46 !important; color: white !important; font-weight: bold; }}
            .grand-total td {{ color: white !important; border-color: #065f46; }}
            .subtotal {{ background: #d4a017 !important; color: #000000 !important; font-weight: bold; }}
            .subtotal td {{ color: #000000 !important; border-color: #b8860b; }}
            .amount {{ text-align: right; font-weight: 500; }}
            .footer {{ margin-top: 30px; text-align: center; color: #64748b; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Swift Billing: Unbilled CN - POD Received Report</h1>
            <p style="text-align: center; color: #64748b;">Report Generated: {datetime.now().strftime('%d-%m-%Y %H:%M')}</p>

            <div style="margin: 20px 0; padding: 15px; background-color: #f8fafc; border-radius: 8px; border-left: 4px solid #3b82f6;">
                <p style="margin: 0 0 10px 0; color: #1e3a5f;"><strong>Hello Team,</strong></p>
                <p style="margin: 0; color: #475569;">Kindly find attached the Unbilled CN Report where POD has been received but billing is pending. Please review and take necessary action.</p>
            </div>

            <div class="summary-cards">
                <div class="card">
                    <div class="card-title">Total No. of CN</div>
                    <div class="card-value" style="color: #ffffff;">{total_cn:,}</div>
                </div>
                <div class="card">
                    <div class="card-title">Total Qty</div>
                    <div class="card-value" style="color: #ffffff;">{total_qty:,}</div>
                </div>
                <div class="card green">
                    <div class="card-title">Total Unbilled Amount</div>
                    <div class="card-value" style="color: #ffffff;">{format_currency(total_amount)}</div>
                </div>
            </div>

            <h2>Party-wise Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Billing Party</th>
                        <th style="text-align: right;">No. of CN</th>
                        <th style="text-align: right;">Qty</th>
                        <th style="text-align: right;">Unbilled Amount</th>
                    </tr>
                </thead>
                <tbody>
                    <tr class="grand-total">
                        <td><strong>Grand Total</strong></td>
                        <td style="text-align: right;"><strong>{total_cn:,}</strong></td>
                        <td style="text-align: right;"><strong>{total_qty:,}</strong></td>
                        <td class="amount"><strong>₹{total_amount:,.0f}</strong></td>
                    </tr>
    """

    row_idx = 0
    for row in grouped_rows:
        if row['is_subtotal']:
            html += f"""
                    <tr class="subtotal">
                        <td><strong>{row['Billing Party']}</strong></td>
                        <td style="text-align: right;"><strong>{row['No. of CN']:,}</strong></td>
                        <td style="text-align: right;"><strong>{row['Qty']:,}</strong></td>
                        <td class="amount"><strong>₹{row['Amount']:,.0f}</strong></td>
                    </tr>
            """
        else:
            bg_color = "#f8fafc" if row_idx % 2 == 0 else "#ffffff"
            html += f"""
                    <tr style="background-color: {bg_color};">
                        <td>{row['Billing Party']}</td>
                        <td style="text-align: right;">{row['No. of CN']:,}</td>
                        <td style="text-align: right;">{row['Qty']:,}</td>
                        <td class="amount">₹{row['Amount']:,.0f}</td>
                    </tr>
            """
            row_idx += 1

    html += f"""
                </tbody>
            </table>

            <div style="margin-top: 25px; padding: 15px; background-color: #f8fafc; border-radius: 8px;">
                <p style="margin: 0; color: #475569;">For any queries, please contact the MIS team.</p>
                <p style="margin: 10px 0 0 0; color: #1e3a5f;"><strong>Regards,</strong><br>Swift Road Link Pvt. Ltd.</p>
            </div>

            <div class="footer">
                <p>This is an automated report from Swift Billing Dashboard</p>
                <p>Dashboard: <a href="https://swiftbilling-iocaaipkggpttjxjo9lgcm.streamlit.app/">View Live Dashboard</a></p>
            </div>
        </div>
    </body>
    </html>
    """

    return html

def generate_csv_report(unbilled_df):
    """Generate CSV report with CN-wise details"""
    if len(unbilled_df) == 0:
        return None

    # Prepare data for CSV - CN-wise details
    export_df = unbilled_df[['cn_no', 'cn_date', 'branch', 'billing_party', 'vehicle_no', 'qty', 'basic_freight', 'pod_receipt_no']].copy()
    export_df.columns = ['CN No', 'CN Date', 'Branch', 'Billing Party', 'Vehicle No', 'Qty', 'Basic Freight', 'POD Receipt No']

    # Format date
    export_df['CN Date'] = pd.to_datetime(export_df['CN Date']).dt.strftime('%d-%m-%Y')

    # Sort by billing party and CN date
    export_df = export_df.sort_values(['Billing Party', 'CN Date'], ascending=[True, False])

    # Generate CSV
    csv_data = export_df.to_csv(index=False)
    return csv_data.encode('utf-8')

def send_email_report(to_emails, cc_emails, unbilled_df, subject_suffix=""):
    """Send the unbilled CN report via email"""
    # Email configuration from environment variables
    smtp_server = get_config("SMTP_SERVER") or "smtp.gmail.com"
    smtp_port = int(get_config("SMTP_PORT") or 587)
    sender_email = get_config("SENDER_EMAIL")
    sender_password = get_config("SENDER_PASSWORD")

    if not all([sender_email, sender_password]):
        print("ERROR: Email configuration missing. Please set SENDER_EMAIL and SENDER_PASSWORD in .env")
        return False

    try:
        print(f"Processing {len(unbilled_df)} unbilled CNs")

        # Generate HTML content
        html_content = generate_summary_html(unbilled_df)

        # Generate CSV attachment
        csv_data = generate_csv_report(unbilled_df)

        # Create email message
        message = MIMEMultipart("mixed")
        subject = f"Swift Billing: Unbilled CN - POD Received Report ({datetime.now().strftime('%d-%m-%Y')})"
        if subject_suffix:
            subject += f" - {subject_suffix}"
        message["Subject"] = subject
        message["From"] = f"Swift Road Link <{sender_email}>"
        message["To"] = ", ".join(to_emails) if isinstance(to_emails, list) else to_emails
        if cc_emails:
            message["Cc"] = ", ".join(cc_emails) if isinstance(cc_emails, list) else cc_emails

        # Add headers to help avoid spam/junk folder
        message["Reply-To"] = sender_email
        message["X-Priority"] = "3"
        message["X-Mailer"] = "Swift Billing System"

        # Attach HTML content
        html_part = MIMEText(html_content, "html")
        message.attach(html_part)

        # Attach CSV file
        if csv_data:
            csv_attachment = MIMEBase("text", "csv")
            csv_attachment.set_payload(csv_data)
            encoders.encode_base64(csv_attachment)
            csv_attachment.add_header(
                "Content-Disposition",
                f"attachment; filename=unbilled_cn_{datetime.now().strftime('%Y%m%d')}.csv"
            )
            message.attach(csv_attachment)

        # Combine all recipients
        all_recipients = to_emails if isinstance(to_emails, list) else [to_emails]
        if cc_emails:
            all_recipients += cc_emails if isinstance(cc_emails, list) else [cc_emails]

        # Send email
        print(f"Sending email to: {', '.join(all_recipients)}")
        context = ssl.create_default_context()

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, all_recipients, message.as_string())

        print("Email sent successfully!")
        return True

    except Exception as e:
        print(f"ERROR sending email: {str(e)}")
        return False

def send_all_reports():
    """Send both email reports with filtered data"""
    print("=" * 60)
    print("Swift Billing - Automated Email Reports")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load all unbilled data
    print("\nLoading unbilled CN data...")
    unbilled_df = load_unbilled_data()
    print(f"Total unbilled CNs: {len(unbilled_df)}")

    # Common CC recipients
    cc_emails = [
        "shyam.wadhwa@srlpl.in",
        "ns@srlpl.in",
        "headops@srlpl.in",
        "finance01@srlpl.in",
        "mis03@srlpl.in",
        "mis@srlpl.in"
    ]

    # ========== EMAIL 1: All parties EXCEPT Mahindra and John Deere ==========
    print("\n" + "-" * 50)
    print("EMAIL 1: All parties EXCEPT Mahindra & John Deere")
    print("-" * 50)

    filtered_df_1 = filter_data_by_parties(
        unbilled_df,
        exclude_parties=['mahindra', 'john deere']
    )
    print(f"Filtered CNs: {len(filtered_df_1)}")

    if len(filtered_df_1) > 0:
        send_email_report(
            to_emails=["billing@srlpl.in"],
            cc_emails=cc_emails,
            unbilled_df=filtered_df_1,
            subject_suffix=""
        )
    else:
        print("No data to send for Email 1")

    # ========== EMAIL 2: Only Mahindra and John Deere ==========
    print("\n" + "-" * 50)
    print("EMAIL 2: Only Mahindra & John Deere")
    print("-" * 50)

    filtered_df_2 = filter_data_by_parties(
        unbilled_df,
        include_parties=['mahindra', 'john deere']
    )
    print(f"Filtered CNs: {len(filtered_df_2)}")

    if len(filtered_df_2) > 0:
        send_email_report(
            to_emails=["billing01@srlpl.in"],
            cc_emails=cc_emails,
            unbilled_df=filtered_df_2,
            subject_suffix="Mahindra & John Deere"
        )
    else:
        print("No data to send for Email 2")

    print("\n" + "=" * 60)
    print("All reports processed!")
    print("=" * 60)

# For backward compatibility - single test email
def send_test_email():
    """Send a test email to mis@srlpl.in"""
    unbilled_df = load_unbilled_data()
    send_email_report(
        to_emails=["mis@srlpl.in"],
        cc_emails=None,
        unbilled_df=unbilled_df
    )

def send_reports_no_cc():
    """Send both email reports WITHOUT CC recipients"""
    print("=" * 60)
    print("Swift Billing - Email Reports (No CC)")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load all unbilled data
    print("\nLoading unbilled CN data...")
    unbilled_df = load_unbilled_data()
    print(f"Total unbilled CNs: {len(unbilled_df)}")

    # ========== EMAIL 1: All parties EXCEPT Mahindra and John Deere ==========
    print("\n" + "-" * 50)
    print("EMAIL 1: All parties EXCEPT Mahindra & John Deere")
    print("-" * 50)

    filtered_df_1 = filter_data_by_parties(
        unbilled_df,
        exclude_parties=['mahindra', 'john deere']
    )
    print(f"Filtered CNs: {len(filtered_df_1)}")

    if len(filtered_df_1) > 0:
        send_email_report(
            to_emails=["billing@srlpl.in"],
            cc_emails=None,
            unbilled_df=filtered_df_1,
            subject_suffix=""
        )
    else:
        print("No data to send for Email 1")

    # ========== EMAIL 2: Only Mahindra and John Deere ==========
    print("\n" + "-" * 50)
    print("EMAIL 2: Only Mahindra & John Deere")
    print("-" * 50)

    filtered_df_2 = filter_data_by_parties(
        unbilled_df,
        include_parties=['mahindra', 'john deere']
    )
    print(f"Filtered CNs: {len(filtered_df_2)}")

    if len(filtered_df_2) > 0:
        send_email_report(
            to_emails=["billing01@srlpl.in"],
            cc_emails=None,
            unbilled_df=filtered_df_2,
            subject_suffix="Mahindra & John Deere"
        )
    else:
        print("No data to send for Email 2")

    print("\n" + "=" * 60)
    print("All reports sent (without CC)!")
    print("=" * 60)

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Send test email
        print("Sending test email...")
        send_test_email()
    elif len(sys.argv) > 1 and sys.argv[1] == "nocc":
        # Send both reports without CC
        send_reports_no_cc()
    else:
        # Send all scheduled reports
        send_all_reports()
