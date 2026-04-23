#!/usr/bin/env python3
"""
Email Report for Pending POD (POD Not Received) - Vehicle Group Based
Sends reports filtered by vehicle groups loaded from fleet_manager_mapping table.
Update fleet_manager_mapping table in database to change vehicle-to-manager assignments.
"""

import smtplib
import ssl
import os
import pandas as pd
import psycopg2
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta

# Try to load .env for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Fleet Manager to Email mapping
FLEET_MANAGER_EMAILS = {
    'VISHAL': 'operations05@srlpl.in',
    'GOPI': 'tracking@srlpl.in',
    'JAGDISH': 'operations03@srlpl.in',
    'PRAVEEN': 'operations08@srlpl.in',
}

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

def load_fleet_manager_mapping():
    """Load fleet manager to vehicle mapping from database"""
    conn = get_connection()
    query = "SELECT vehicle_no, fleet_manager FROM fleet_manager_mapping"
    df = pd.read_sql(query, conn)
    conn.close()

    # Build dict: {fleet_manager: [list of normalized vehicle_nos]}
    mapping = {}
    for _, row in df.iterrows():
        manager = row['fleet_manager'].upper().strip()
        vehicle = normalize_vehicle_no(row['vehicle_no'])
        if manager not in mapping:
            mapping[manager] = []
        mapping[manager].append(vehicle)

    print(f"Loaded fleet manager mapping: {', '.join(f'{k}={len(v)} vehicles' for k, v in mapping.items())}")
    return mapping

def load_pending_pod_data():
    """Load pending POD data (POD not received) from database"""
    conn = get_connection()
    query = """
        SELECT
            cn_no, cn_date, branch, billing_party, origin, destination,
            route, vehicle_no, qty, basic_freight, other_charges, consignee, eta,
            bill_no, pod_receipt_no
        FROM cn_data
        WHERE (is_active = true OR is_active::text = 'Yes')
        ORDER BY cn_date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df['cn_date'] = pd.to_datetime(df['cn_date'])
    df['eta'] = pd.to_datetime(df['eta'], errors='coerce')

    d_minus_4 = (datetime.now() - timedelta(days=4)).date()

    # Filter pending POD
    pending_df = df[
        ((df['bill_no'].isna()) | (df['bill_no'] == '') | (df['bill_no'].astype(str).str.strip() == '')) &
        ((df['pod_receipt_no'].isna()) | (df['pod_receipt_no'] == '') | (df['pod_receipt_no'].astype(str).str.strip() == '')) &
        (df['eta'].notna()) & (df['eta'].dt.date < d_minus_4) &
        (df['vehicle_no'].notna()) & (df['vehicle_no'] != '') & (df['vehicle_no'].astype(str).str.strip() != '')
    ].copy()

    return pending_df

def normalize_vehicle_no(vehicle_no):
    """Normalize vehicle number for comparison"""
    if not vehicle_no:
        return ''
    return vehicle_no.upper().strip().replace(' ', '')

def get_group_for_vehicle(vehicle_no, fleet_mapping):
    """Determine group for a vehicle using DB mapping"""
    if not vehicle_no:
        return None

    vehicle_normalized = normalize_vehicle_no(vehicle_no)

    for manager, vehicles in fleet_mapping.items():
        if vehicle_normalized in vehicles:
            return manager

    return None

def filter_by_vehicle_group(df, group_name, fleet_mapping):
    """Filter dataframe by vehicle group using DB mapping"""
    normalized_list = fleet_mapping.get(group_name, [])
    if not normalized_list:
        return pd.DataFrame()

    df['vehicle_normalized'] = df['vehicle_no'].apply(normalize_vehicle_no)
    filtered = df[df['vehicle_normalized'].isin(normalized_list)].copy()
    filtered = filtered.drop(columns=['vehicle_normalized'])

    return filtered

def format_currency(val):
    """Format currency in Lakhs"""
    if abs(val) >= 100000:
        return f"₹{val/100000:.2f}L"
    elif abs(val) >= 1000:
        return f"₹{val/1000:.2f}K"
    return f"₹{val:.0f}"

def generate_pending_pod_html(pending_df, group_name):
    """Generate HTML summary for pending POD email"""
    if len(pending_df) == 0:
        return f"<p>No pending POD CNs found for {group_name}.</p>"

    # Calculate totals (for John Deere India Private Limited, include other_charges)
    total_cn = len(pending_df)
    total_qty = int(pending_df['qty'].sum())
    jd_pending = pending_df[pending_df['billing_party'] == 'John Deere India Private Limited']
    other_pending = pending_df[pending_df['billing_party'] != 'John Deere India Private Limited']
    total_amount = other_pending['basic_freight'].sum() + jd_pending['basic_freight'].sum() + jd_pending['other_charges'].fillna(0).sum()

    # Build summary by vehicle
    summary_data = []
    vehicles = pending_df['vehicle_no'].dropna().unique()

    for vehicle in vehicles:
        vehicle_data = pending_df[pending_df['vehicle_no'] == vehicle]
        # For John Deere India Private Limited records, include other_charges
        jd_vehicle = vehicle_data[vehicle_data['billing_party'] == 'John Deere India Private Limited']
        other_vehicle = vehicle_data[vehicle_data['billing_party'] != 'John Deere India Private Limited']
        amount = other_vehicle['basic_freight'].sum() + jd_vehicle['basic_freight'].sum() + jd_vehicle['other_charges'].fillna(0).sum()
        row = {
            'Vehicle No': vehicle,
            'Total CN': len(vehicle_data),
            'Total Qty': int(vehicle_data['qty'].sum()),
            'Total Amount': amount
        }
        summary_data.append(row)

    # Sort by amount
    summary_data = sorted(summary_data, key=lambda x: x['Total Amount'], reverse=True)

    # Build HTML
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f5f5f5; padding: 20px; }}
            .container {{ max-width: 900px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #1e3a5f; text-align: center; }}
            h2 {{ color: #3b82f6; border-bottom: 2px solid #3b82f6; padding-bottom: 10px; }}
            .group-badge {{ background: #7c3aed; color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; display: inline-block; margin-bottom: 15px; }}
            .summary-cards {{ display: flex; justify-content: space-around; margin: 20px 0; }}
            .card {{ background: #1e3a5f; color: #ffffff; padding: 20px; border-radius: 10px; text-align: center; min-width: 150px; }}
            .card.purple {{ background: #7c3aed; }}
            .card-title {{ font-size: 12px; color: #94a3b8; margin-bottom: 8px; }}
            .card-value {{ font-size: 28px; font-weight: bold; color: #ffffff; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; border: 2px solid #1e3a5f; }}
            th {{ background: #1e3a5f; color: white; padding: 12px 8px; text-align: left; font-weight: 600; border: 1px solid #1e3a5f; }}
            td {{ padding: 10px 8px; border: 1px solid #cbd5e1; color: #333333; }}
            .grand-total {{ background: #7c3aed !important; color: white !important; font-weight: bold; }}
            .grand-total td {{ color: white !important; border-color: #6d28d9; }}
            .amount {{ text-align: right; font-weight: 500; }}
            .footer {{ margin-top: 30px; text-align: center; color: #64748b; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Swift Billing: Pending POD Report</h1>
            <p style="text-align: center;"><span class="group-badge">{group_name}</span></p>
            <p style="text-align: center; color: #64748b;">Report Generated: {datetime.now().strftime('%d-%m-%Y %H:%M')}</p>

            <div style="margin: 20px 0; padding: 15px; background-color: #f3e8ff; border-radius: 8px; border-left: 4px solid #7c3aed;">
                <p style="margin: 0 0 10px 0; color: #5b21b6;"><strong>Hello {group_name.title()},</strong></p>
                <p style="margin: 0; color: #6b21a8;">Kindly find attached the Pending POD Report for your vehicles where POD has NOT been received and ETA is more than 4 days old. Please follow up and take necessary action.</p>
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
                <div class="card purple">
                    <div class="card-title">Total Pending Amount</div>
                    <div class="card-value" style="color: #ffffff;">{format_currency(total_amount)}</div>
                </div>
            </div>

            <h2>Vehicle-wise Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Vehicle No</th>
                        <th style="text-align: right;">No. of CN</th>
                        <th style="text-align: right;">Qty</th>
                        <th style="text-align: right;">Pending Amount</th>
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
    for row in summary_data:
        bg_color = "#f8fafc" if row_idx % 2 == 0 else "#ffffff"
        html += f"""
                    <tr style="background-color: {bg_color};">
                        <td>{row['Vehicle No']}</td>
                        <td style="text-align: right;">{row['Total CN']:,}</td>
                        <td style="text-align: right;">{row['Total Qty']:,}</td>
                        <td class="amount">₹{row['Total Amount']:,.0f}</td>
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

def generate_csv_report(pending_df):
    """Generate CSV report with CN-wise details"""
    if len(pending_df) == 0:
        return None

    export_df = pending_df[['cn_no', 'cn_date', 'branch', 'billing_party', 'origin', 'destination', 'vehicle_no', 'qty', 'basic_freight', 'consignee']].copy()
    export_df.columns = ['CN No', 'CN Date', 'Branch', 'Billing Party', 'Origin', 'Destination', 'Vehicle No', 'Qty', 'Basic Freight', 'Consignee']

    export_df['CN Date'] = pd.to_datetime(export_df['CN Date']).dt.strftime('%d-%m-%Y')
    export_df = export_df.sort_values(['Vehicle No', 'CN Date'], ascending=[True, False])

    csv_data = export_df.to_csv(index=False)
    return csv_data.encode('utf-8')

def send_email_report(to_emails, cc_emails, pending_df, group_name):
    """Send the pending POD report via email"""
    smtp_server = get_config("SMTP_SERVER") or "smtp.gmail.com"
    smtp_port = int(get_config("SMTP_PORT") or 587)
    sender_email = get_config("SENDER_EMAIL")
    sender_password = get_config("SENDER_PASSWORD")

    if not all([sender_email, sender_password]):
        print("ERROR: Email configuration missing.")
        return False

    try:
        print(f"Processing {len(pending_df)} pending POD CNs for {group_name}")

        html_content = generate_pending_pod_html(pending_df, group_name)
        csv_data = generate_csv_report(pending_df)

        message = MIMEMultipart("mixed")
        subject = f"Swift Billing: Pending POD Report - {group_name} ({datetime.now().strftime('%d-%m-%Y')})"
        message["Subject"] = subject
        message["From"] = f"Swift Road Link <{sender_email}>"
        message["To"] = ", ".join(to_emails) if isinstance(to_emails, list) else to_emails
        if cc_emails:
            message["Cc"] = ", ".join(cc_emails) if isinstance(cc_emails, list) else cc_emails

        message["Reply-To"] = sender_email
        message["X-Priority"] = "3"
        message["X-Mailer"] = "Swift Billing System"

        html_part = MIMEText(html_content, "html")
        message.attach(html_part)

        if csv_data:
            csv_attachment = MIMEBase("text", "csv")
            csv_attachment.set_payload(csv_data)
            encoders.encode_base64(csv_attachment)
            csv_attachment.add_header(
                "Content-Disposition",
                f"attachment; filename=pending_pod_{group_name.lower()}_{datetime.now().strftime('%Y%m%d')}.csv"
            )
            message.attach(csv_attachment)

        all_recipients = to_emails if isinstance(to_emails, list) else [to_emails]
        if cc_emails:
            all_recipients += cc_emails if isinstance(cc_emails, list) else [cc_emails]

        print(f"Sending email to: {', '.join(all_recipients)}")
        context = ssl.create_default_context()

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, all_recipients, message.as_string())

        print(f"Email sent successfully for {group_name}!")
        return True

    except Exception as e:
        print(f"ERROR sending email: {str(e)}")
        return False

def send_all_vehicle_reports():
    """Send all vehicle group pending POD reports"""
    print("=" * 60)
    print("Swift Billing - Pending POD Vehicle Group Reports")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load fleet manager mapping from database
    print("\nLoading fleet manager mapping from database...")
    fleet_mapping = load_fleet_manager_mapping()

    # Load pending POD data
    print("\nLoading pending POD data...")
    pending_df = load_pending_pod_data()
    print(f"Total pending POD CNs: {len(pending_df)}")

    # Common CC recipients
    cc_emails = [
        "shyam.wadhwa@srlpl.in",
        "rg@srlpl.in",
        "mis@srlpl.in",
        "hr01@srlpl.in"
    ]

    # Send reports for each fleet manager from DB mapping
    for manager_name in sorted(fleet_mapping.keys()):
        print("\n" + "-" * 50)
        print(f"MAIL: {manager_name}")
        print("-" * 50)

        manager_df = filter_by_vehicle_group(pending_df, manager_name, fleet_mapping)
        print(f"{manager_name} CNs: {len(manager_df)}")

        if len(manager_df) > 0:
            to_email = FLEET_MANAGER_EMAILS.get(manager_name)
            if not to_email:
                print(f"WARNING: No email configured for {manager_name} in FLEET_MANAGER_EMAILS. Skipping.")
                continue
            send_email_report(
                to_emails=[to_email],
                cc_emails=cc_emails,
                pending_df=manager_df,
                group_name=manager_name
            )
        else:
            print(f"No data to send for {manager_name}")

    print("\n" + "=" * 60)
    print("All vehicle group reports processed!")
    print("=" * 60)

def send_test_reports():
    """Send test reports to mis@srlpl.in"""
    print("=" * 60)
    print("Swift Billing - Pending POD Vehicle Group Reports (TEST)")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load fleet manager mapping from database
    print("\nLoading fleet manager mapping from database...")
    fleet_mapping = load_fleet_manager_mapping()

    # Load pending POD data
    print("\nLoading pending POD data...")
    pending_df = load_pending_pod_data()
    print(f"Total pending POD CNs: {len(pending_df)}")

    test_email = "mis@srlpl.in"

    # Send test reports for each fleet manager from DB mapping
    for manager_name in sorted(fleet_mapping.keys()):
        print("\n" + "-" * 50)
        print(f"MAIL: {manager_name} (TEST)")
        print("-" * 50)

        manager_df = filter_by_vehicle_group(pending_df, manager_name, fleet_mapping)
        print(f"{manager_name} CNs: {len(manager_df)}")

        if len(manager_df) > 0:
            send_email_report(
                to_emails=[test_email],
                cc_emails=None,
                pending_df=manager_df,
                group_name=manager_name
            )
        else:
            print(f"No data to send for {manager_name}")

    print("\n" + "=" * 60)
    print("All TEST reports sent to mis@srlpl.in!")
    print("=" * 60)

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_test_reports()
    else:
        send_all_vehicle_reports()
