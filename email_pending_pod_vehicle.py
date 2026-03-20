#!/usr/bin/env python3
"""
Email Report for Pending POD (POD Not Received) - Vehicle Group Based
Sends reports filtered by vehicle groups (Vishal, Gopi, Jagdish, Praveen)
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

# Vehicle Group Mappings
VISHAL_VEHICLES = [
    '0020 NL01AJ', '0167 NL01AH', '0219 NL01AH', '0283 NL01AH', '0523 GJ08AU',
    '0536 GJ08AU', '0570 GJ08AU', '0572 GJ08AU', '0639 GJ08AU', '0699 GJ08AU',
    '1797 PB11BR', '2081NL01AJ', '2082NL01AJ', '2083NL01AJ', '2084NL01AJ',
    '2209NL01AJ', '2625 NL01AG', '3137 NL01AG', '4061 NL01N', '4062 NL01N',
    '4064 NL01N', '4066 NL01N', '4067 NL01N', '4079 NL01AG', '4385 NL01AJ',
    '4387 NL01AJ', '4389 NL01AJ', '4521 NL01AH', '4522 NL01AH', '4523 NL01AH',
    '4524 NL01AH', '4525 NL01AH', '4526 NL01AH', '4527 NL01AH', '4528 NL01AH',
    '4529 NL01AH', '4530 NL01AH', '7178NL01AJ', '7225 NL01AF', '8314 NL01AG',
    '9392 NL01AH', '9450 NL01L', '9455 NL01L', '9457 NL01L', '9458 NL01L',
    '9566 NL01AH', '9889 NL01AF', '9890 NL01AF', '9891 NL01AF', '9991 NL01AG',
    'HR55AM 1370'
]

GOPI_VEHICLES = [
    '0218 NL01AH', '0628 GJ08AU', '0740 GJ08AU', '0863 GJ08AU', '0908 GJ08AU',
    '0951 GJ08AU', '0983 GJ08AU', '0986 GJ08AU', '1107 NL01AH', '1108 NL01AH',
    '1109 NL01AH', '1110 NL01AH', '1111 NL01AH', '1112 NL01AH', '1113 NL01AH',
    '1114 NL01AH', '1115 NL01AH', '2210NL01AJ', '2211NL01AJ', '2396 NL01N',
    '2397 NL01N', '2398 NL01N', '2399 NL01N', '2400 NL01N', '3431 NL01AG',
    '3432 NL01AG', '3433 NL01AG', '3748 HR55AR', '3906 NL01N', '3907 NL01N',
    '3908 NL01N', '3909 NL01N', '3910 NL01N', '4065 NL01N', '4068 NL01N',
    '4069 NL01N', '4388 NL01AJ', '4390 NL01AJ', '4531 HR55AR', '4531 NL01AH',
    '4532 NL01AH', '4533 NL01AH', '4534 NL01AH', '4535 NL01AH', '4536 NL01AH',
    '4537 NL01AH', '4538 NL01AH', '4539 NL01AH', '5825NL01AJ', '5826NL01AJ',
    '5827NL01AJ', '5828NL01AJ', '6158 HR55AQ', '6429 HR55AQ', '6456NL01AJ',
    '6457NL01AJ', '6458NL01AJ', '6459NL01AJ', '6460NL01AJ', '6469 HR55AQ',
    '6484HR55AQ', '7175NL01AJ', '7176 NL01AJ', '7177NL01AJ', '7220 NL01AF',
    '7222 NL01AF', '7223 NL01AF', '7224 NL01AF', '7226 NL01AF', '8204 NL01AH',
    '8224 HR55AQ', '8315 NL01AG', '8450 HR55AQ', '8593 HR55AR', '8597 HR55AR',
    '8739 HR55AQ', '8752 HR55AR', '8795 HR55AR', '9452 NL01L', '9460 NL01L',
    '9494 HR55AQ'
]

JAGDISH_VEHICLES = [
    '0284 NL01AH', '0285 NL01AH', '0286 NL01AH', '0722 GJ08AU', '0739 GJ08AU',
    '0764 GJ08AU', '0814 GJ08AU', '0815 GJ08AU', '0816 GJ08AU', '0824 GJ08AU',
    '4063 NL01N', '8630 NL01AG', '9451NL01L', 'NL01Q 8157', 'HR55AP 1974',
    'HR55AM 2340', 'HR55AM 9667', 'HR55AM 0907', 'HR55AM 8703', 'HR55AN 5406',
    'HR55AN 5307', 'HR55AM 4278', 'HR55AM 6059', 'NL01Q8150', 'NL01Q9547'
]

PRAVEEN_VEHICLES = [
    '0959 HR55AQ', '1171 HR55AR', '1564 HR55AQ', '1652 NL01AH', '1741 HR55AR',
    '2206NL01AJ', '2207NL01AJ', '2208NL01AJ', '2623 NL01AG', '2624 NL01AG',
    '2829 HR55AR', '2885 HR55AQ', '2942 HR55AQ', '3135 NL01AG', '3136 NL01AG',
    '4078 NL01AG', '4080 NL01AG', '4149 HR55AQ', '4180 HR55AQ', '4274 HR55AR',
    '4540 NL01AH', '4849 NL01AH', '4850 NL01AH', '4851 NL01AH', '4852 NL01AH',
    '4853 NL01AH', '4854 NL01AH', '4855 NL01AH', '4856 NL01AH', '4857 NL01AH',
    '4858 NL01AH', '5077 HR55AQ', '5305 NL01N', '5306 NL01N', '5307 NL01N',
    '5309 NL01N', '5417 HR55AQ', '5495 HR55AR', '5578 HR55AR', '5709 HR55AR',
    '5819NL01AJ', '5820NL01AJ', '5821NL01AJ', '5822NL01AJ', '5823NL01AJ',
    '5824 HR55AQ', '5824NL01AJ', '6017 HR55AR', '7169NL01AJ', '7170NL01AJ',
    '7171NL01AJ', '7172NL01AJ', '7173NL01AJ', '7174NL01AJ', '7219 NL01AF',
    '7221 NL01AF', '7521 NL01N', '7522 NL01N', '7523 NL01N', '7524 NL01N',
    '7525 NL01N', '7526 NL01N', '7527 NL01N', '7528 NL01N', '7529 NL01N',
    '7530 NL01N', '7553 HR55AR', '7745 HR55AR', '8008 HR55AR', '8078 HR55AR',
    '8193 NL01AH', '9080 HR55AQ', '9104 HR55AR', '9244 HR55AQ', '9256 HR55AQ',
    '9453 NL01L', '9454 NL01L', '9456 NL01L', '9702 HR55AR', '9851 NL01AH'
]

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

def load_pending_pod_data():
    """Load pending POD data (POD not received) from database"""
    conn = get_connection()
    query = """
        SELECT
            cn_no, cn_date, branch, billing_party, origin, destination,
            route, vehicle_no, qty, basic_freight, consignee, eta,
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

def get_group_for_vehicle(vehicle_no):
    """Determine group for a vehicle"""
    if not vehicle_no:
        return None

    vehicle_normalized = normalize_vehicle_no(vehicle_no)

    for v in VISHAL_VEHICLES:
        if normalize_vehicle_no(v) == vehicle_normalized:
            return 'VISHAL'

    for v in GOPI_VEHICLES:
        if normalize_vehicle_no(v) == vehicle_normalized:
            return 'GOPI'

    for v in JAGDISH_VEHICLES:
        if normalize_vehicle_no(v) == vehicle_normalized:
            return 'JAGDISH'

    for v in PRAVEEN_VEHICLES:
        if normalize_vehicle_no(v) == vehicle_normalized:
            return 'PRAVEEN'

    return None

def filter_by_vehicle_group(df, group_name):
    """Filter dataframe by vehicle group"""
    if group_name == 'VISHAL':
        vehicle_list = VISHAL_VEHICLES
    elif group_name == 'GOPI':
        vehicle_list = GOPI_VEHICLES
    elif group_name == 'JAGDISH':
        vehicle_list = JAGDISH_VEHICLES
    elif group_name == 'PRAVEEN':
        vehicle_list = PRAVEEN_VEHICLES
    else:
        return pd.DataFrame()

    # Normalize vehicle numbers for comparison
    normalized_list = [normalize_vehicle_no(v) for v in vehicle_list]
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

    # Calculate totals
    total_cn = len(pending_df)
    total_qty = int(pending_df['qty'].sum())
    total_amount = pending_df['basic_freight'].sum()

    # Build summary by vehicle
    summary_data = []
    vehicles = pending_df['vehicle_no'].dropna().unique()

    for vehicle in vehicles:
        vehicle_data = pending_df[pending_df['vehicle_no'] == vehicle]
        row = {
            'Vehicle No': vehicle,
            'Total CN': len(vehicle_data),
            'Total Qty': int(vehicle_data['qty'].sum()),
            'Total Amount': vehicle_data['basic_freight'].sum()
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

    # Load pending POD data
    print("\nLoading pending POD data...")
    pending_df = load_pending_pod_data()
    print(f"Total pending POD CNs: {len(pending_df)}")

    # Common CC recipients (different from zone emails)
    cc_emails = [
        "shyam.wadhwa@srlpl.in",
        "headops@srlpl.in",
        "mis@srlpl.in",
        "hr01@srlpl.in"
    ]

    # ========== MAIL 6: VISHAL ==========
    print("\n" + "-" * 50)
    print("MAIL 6: VISHAL")
    print("-" * 50)

    vishal_df = filter_by_vehicle_group(pending_df, 'VISHAL')
    print(f"VISHAL CNs: {len(vishal_df)}")

    if len(vishal_df) > 0:
        send_email_report(
            to_emails=["operations05@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=vishal_df,
            group_name="VISHAL"
        )
    else:
        print("No data to send for VISHAL")

    # ========== MAIL 7: GOPI ==========
    print("\n" + "-" * 50)
    print("MAIL 7: GOPI")
    print("-" * 50)

    gopi_df = filter_by_vehicle_group(pending_df, 'GOPI')
    print(f"GOPI CNs: {len(gopi_df)}")

    if len(gopi_df) > 0:
        send_email_report(
            to_emails=["tracking@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=gopi_df,
            group_name="GOPI"
        )
    else:
        print("No data to send for GOPI")

    # ========== MAIL 8: JAGDISH ==========
    print("\n" + "-" * 50)
    print("MAIL 8: JAGDISH")
    print("-" * 50)

    jagdish_df = filter_by_vehicle_group(pending_df, 'JAGDISH')
    print(f"JAGDISH CNs: {len(jagdish_df)}")

    if len(jagdish_df) > 0:
        send_email_report(
            to_emails=["operations03@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=jagdish_df,
            group_name="JAGDISH"
        )
    else:
        print("No data to send for JAGDISH")

    # ========== MAIL 9: PRAVEEN ==========
    print("\n" + "-" * 50)
    print("MAIL 9: PRAVEEN")
    print("-" * 50)

    praveen_df = filter_by_vehicle_group(pending_df, 'PRAVEEN')
    print(f"PRAVEEN CNs: {len(praveen_df)}")

    if len(praveen_df) > 0:
        send_email_report(
            to_emails=["operations08@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=praveen_df,
            group_name="PRAVEEN"
        )
    else:
        print("No data to send for PRAVEEN")

    print("\n" + "=" * 60)
    print("All vehicle group reports processed!")
    print("=" * 60)

def send_test_reports():
    """Send test reports to mis@srlpl.in"""
    print("=" * 60)
    print("Swift Billing - Pending POD Vehicle Group Reports (TEST)")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load pending POD data
    print("\nLoading pending POD data...")
    pending_df = load_pending_pod_data()
    print(f"Total pending POD CNs: {len(pending_df)}")

    test_email = "mis@srlpl.in"

    # ========== MAIL 6: VISHAL ==========
    print("\n" + "-" * 50)
    print("MAIL 6: VISHAL (TEST)")
    print("-" * 50)

    vishal_df = filter_by_vehicle_group(pending_df, 'VISHAL')
    print(f"VISHAL CNs: {len(vishal_df)}")

    if len(vishal_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=vishal_df,
            group_name="VISHAL"
        )
    else:
        print("No data to send for VISHAL")

    # ========== MAIL 7: GOPI ==========
    print("\n" + "-" * 50)
    print("MAIL 7: GOPI (TEST)")
    print("-" * 50)

    gopi_df = filter_by_vehicle_group(pending_df, 'GOPI')
    print(f"GOPI CNs: {len(gopi_df)}")

    if len(gopi_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=gopi_df,
            group_name="GOPI"
        )
    else:
        print("No data to send for GOPI")

    # ========== MAIL 8: JAGDISH ==========
    print("\n" + "-" * 50)
    print("MAIL 8: JAGDISH (TEST)")
    print("-" * 50)

    jagdish_df = filter_by_vehicle_group(pending_df, 'JAGDISH')
    print(f"JAGDISH CNs: {len(jagdish_df)}")

    if len(jagdish_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=jagdish_df,
            group_name="JAGDISH"
        )
    else:
        print("No data to send for JAGDISH")

    # ========== MAIL 9: PRAVEEN ==========
    print("\n" + "-" * 50)
    print("MAIL 9: PRAVEEN (TEST)")
    print("-" * 50)

    praveen_df = filter_by_vehicle_group(pending_df, 'PRAVEEN')
    print(f"PRAVEEN CNs: {len(praveen_df)}")

    if len(praveen_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=praveen_df,
            group_name="PRAVEEN"
        )
    else:
        print("No data to send for PRAVEEN")

    print("\n" + "=" * 60)
    print("All TEST reports sent to mis@srlpl.in!")
    print("=" * 60)

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_test_reports()
    else:
        send_all_vehicle_reports()
