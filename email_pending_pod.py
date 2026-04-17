#!/usr/bin/env python3
"""
Email Report for Pending POD (POD Not Received) - Zone Based
Sends reports filtered by origin zones
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

# Zone definitions based on origin
WEST_ZONE = [
    'pune', 'mumbai', 'nashik', 'sanand', 'halol', 'ahmedabad', 'chakan',
    'ranjangaon', 'aurangabad', 'bhiwandi', 'kolhapur', 'nagpur', 'surat',
    'rajkot', 'jamnagar', 'indore', 'bhopal', 'pipavav', 'palanpur',
    'bechraji', 'bilaspur', 'pune_ranjangaon'
]

SOUTH_ZONE = [
    'bangalore', 'chennai', 'kia', 'toyota banglore', 'toyota bangalore',
    'hyderabad', 'bidadi', 'pondicherry', 'zaheerabad', 'warangal',
    'guntur', 'vishakhapatnam', 'kurnool', 'glovis', 'bengaluru',
    'bangalore[c]', 'chennai ti', 'chennai bmw'
]

NORTH_EAST_ZONE = [
    # North
    'delhi', 'gurgaon', 'noida', 'faridabad', 'haridwar', 'chandigarh',
    'manesar', 'sonipat', 'palwal', 'meerut', 'agra', 'lucknow', 'kanpur',
    'allahabad', 'varanasi', 'gorakhpur', 'jaipur', 'kota', 'jodhpur',
    'udaipur', 'dehradun', 'roorkee', 'jammu', 'tapukera', 'mathura',
    'aligarh', 'sitapur', 'hardoi', 'faizabad', 'azamgarh', 'mirzapur',
    'moradabad', 'jhansi', 'gwalior', 'kurukshetra', 'kharkhoda', 'pirthla',
    'amritsar', 'bhatinda', 'bikaner', 'ajmer', 'bhilwara', 'sikar',
    'nagaur', 'bhartpur', 'sri ganganagar', 'sriganganagar', 'sujangarh',
    'delhi/ncr', 'farrukhnagar', 'farukhnagar', 'ghaziabad', 'kathua',
    # East
    'patna', 'kolkata', 'ranchi', 'jamshedpur', 'dhanbad', 'guwahati',
    'siliguri', 'cuttack', 'bardhaman', 'asansol', 'bokaro', 'jamalpur',
    'kharagpur', 'hooghly', 'purnia', 'jeypore', 'north lakhimpur', 'siwan',
    'barhi'
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
            route, vehicle_no, qty, basic_freight, consignee, eta
        FROM cn_data
        WHERE (is_active = true OR is_active::text = 'Yes')
        ORDER BY cn_date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df['cn_date'] = pd.to_datetime(df['cn_date'])
    df['eta'] = pd.to_datetime(df['eta'], errors='coerce')

    # Filter: Bill No is blank AND POD Receipt No is blank AND ETA < D-4
    # Since we don't have bill_no and pod_receipt_no in query, let's add them
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
        (df['origin'].notna()) & (df['origin'] != '') & (df['origin'].astype(str).str.strip() != '')
    ].copy()

    return pending_df

def get_zone_for_origin(origin):
    """Determine zone for an origin - check longest match first to avoid substring issues"""
    if not origin:
        return 'Unknown'
    origin_lower = origin.lower().strip()

    # Build list of all zone matches with zone name
    all_zones = [
        (keyword, 'West') for keyword in WEST_ZONE
    ] + [
        (keyword, 'South') for keyword in SOUTH_ZONE
    ] + [
        (keyword, 'North & East') for keyword in NORTH_EAST_ZONE
    ]

    # Find all matching keywords
    matches = []
    for keyword, zone in all_zones:
        if keyword == origin_lower or keyword in origin_lower:
            matches.append((keyword, zone, len(keyword)))

    # If matches found, return the zone with longest keyword match
    if matches:
        matches.sort(key=lambda x: x[2], reverse=True)  # Sort by keyword length
        return matches[0][1]

    return 'Unknown'

def filter_by_zone(df, zone):
    """Filter dataframe by zone"""
    df['zone'] = df['origin'].apply(get_zone_for_origin)
    return df[df['zone'] == zone].copy()

def format_currency(val):
    """Format currency in Lakhs"""
    if abs(val) >= 100000:
        return f"₹{val/100000:.2f}L"
    elif abs(val) >= 1000:
        return f"₹{val/1000:.2f}K"
    return f"₹{val:.0f}"

def generate_pending_pod_html(pending_df, zone_name):
    """Generate HTML summary for pending POD email"""
    if len(pending_df) == 0:
        return f"<p>No pending POD CNs found for {zone_name} Zone.</p>"

    # Calculate totals (for John Deere India Private Limited, include other_charges)
    total_cn = len(pending_df)
    total_qty = int(pending_df['qty'].sum())
    jd_pending = pending_df[pending_df['billing_party'] == 'John Deere India Private Limited']
    other_pending = pending_df[pending_df['billing_party'] != 'John Deere India Private Limited']
    total_amount = other_pending['basic_freight'].sum() + jd_pending['basic_freight'].sum() + jd_pending['other_charges'].fillna(0).sum()

    # Build summary by party
    summary_data = []
    parties = pending_df['billing_party'].dropna().unique()

    for party in parties:
        party_data = pending_df[pending_df['billing_party'] == party]
        # For John Deere India Private Limited, include other_charges
        if party == 'John Deere India Private Limited':
            amount = party_data['basic_freight'].sum() + party_data['other_charges'].fillna(0).sum()
        else:
            amount = party_data['basic_freight'].sum()
        row = {
            'Billing Party': party,
            'Total CN': len(party_data),
            'Total Qty': int(party_data['qty'].sum()),
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
            .zone-badge {{ background: #f59e0b; color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; display: inline-block; margin-bottom: 15px; }}
            .summary-cards {{ display: flex; justify-content: space-around; margin: 20px 0; }}
            .card {{ background: #1e3a5f; color: #ffffff; padding: 20px; border-radius: 10px; text-align: center; min-width: 150px; }}
            .card.orange {{ background: #f59e0b; }}
            .card-title {{ font-size: 12px; color: #94a3b8; margin-bottom: 8px; }}
            .card-value {{ font-size: 28px; font-weight: bold; color: #ffffff; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; border: 2px solid #1e3a5f; }}
            th {{ background: #1e3a5f; color: white; padding: 12px 8px; text-align: left; font-weight: 600; border: 1px solid #1e3a5f; }}
            td {{ padding: 10px 8px; border: 1px solid #cbd5e1; color: #333333; }}
            .grand-total {{ background: #f59e0b !important; color: white !important; font-weight: bold; }}
            .grand-total td {{ color: white !important; border-color: #d97706; }}
            .amount {{ text-align: right; font-weight: 500; }}
            .footer {{ margin-top: 30px; text-align: center; color: #64748b; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Swift Billing: Pending POD Report</h1>
            <p style="text-align: center;"><span class="zone-badge">{zone_name} Zone</span></p>
            <p style="text-align: center; color: #64748b;">Report Generated: {datetime.now().strftime('%d-%m-%Y %H:%M')}</p>

            <div style="margin: 20px 0; padding: 15px; background-color: #fef3c7; border-radius: 8px; border-left: 4px solid #f59e0b;">
                <p style="margin: 0 0 10px 0; color: #92400e;"><strong>Hello Team,</strong></p>
                <p style="margin: 0; color: #78350f;">Kindly find attached the Pending POD Report where POD has NOT been received and ETA is more than 4 days old. Please follow up and take necessary action.</p>
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
                <div class="card orange">
                    <div class="card-title">Total Pending Amount</div>
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
                        <td>{row['Billing Party']}</td>
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
    export_df = export_df.sort_values(['Billing Party', 'CN Date'], ascending=[True, False])

    csv_data = export_df.to_csv(index=False)
    return csv_data.encode('utf-8')

def send_email_report(to_emails, cc_emails, pending_df, zone_name):
    """Send the pending POD report via email"""
    smtp_server = get_config("SMTP_SERVER") or "smtp.gmail.com"
    smtp_port = int(get_config("SMTP_PORT") or 587)
    sender_email = get_config("SENDER_EMAIL")
    sender_password = get_config("SENDER_PASSWORD")

    if not all([sender_email, sender_password]):
        print("ERROR: Email configuration missing.")
        return False

    try:
        print(f"Processing {len(pending_df)} pending POD CNs for {zone_name} Zone")

        html_content = generate_pending_pod_html(pending_df, zone_name)
        csv_data = generate_csv_report(pending_df)

        message = MIMEMultipart("mixed")
        subject = f"Swift Billing: Pending POD Report - {zone_name} Zone ({datetime.now().strftime('%d-%m-%Y')})"
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
                f"attachment; filename=pending_pod_{zone_name.lower().replace(' & ', '_').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv"
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

        print(f"Email sent successfully for {zone_name} Zone!")
        return True

    except Exception as e:
        print(f"ERROR sending email: {str(e)}")
        return False

def send_all_zone_reports():
    """Send all zone-based pending POD reports"""
    print("=" * 60)
    print("Swift Billing - Pending POD Zone Reports")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load pending POD data
    print("\nLoading pending POD data...")
    pending_df = load_pending_pod_data()
    print(f"Total pending POD CNs: {len(pending_df)}")

    # Common CC recipients
    cc_emails = [
        "shyam.wadhwa@srlpl.in",
        "ns@srlpl.in",
        "rg@srlpl.in",
        "mis@srlpl.in",
        "mis03@srlpl.in",
        "hr01@srlpl.in"
    ]

    # ========== MAIL 3: West Zone ==========
    print("\n" + "-" * 50)
    print("MAIL 3: West Zone")
    print("-" * 50)

    west_df = filter_by_zone(pending_df, 'West')
    print(f"West Zone CNs: {len(west_df)}")

    if len(west_df) > 0:
        send_email_report(
            to_emails=["raj.tiwari@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=west_df,
            zone_name="West"
        )
    else:
        print("No data to send for West Zone")

    # ========== MAIL 4: South Zone ==========
    print("\n" + "-" * 50)
    print("MAIL 4: South Zone")
    print("-" * 50)

    south_df = filter_by_zone(pending_df, 'South')
    print(f"South Zone CNs: {len(south_df)}")

    if len(south_df) > 0:
        send_email_report(
            to_emails=["operations07@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=south_df,
            zone_name="South"
        )
    else:
        print("No data to send for South Zone")

    # ========== MAIL 5: North & East Zone ==========
    print("\n" + "-" * 50)
    print("MAIL 5: North & East Zone")
    print("-" * 50)

    north_east_df = filter_by_zone(pending_df, 'North & East')
    print(f"North & East Zone CNs: {len(north_east_df)}")

    if len(north_east_df) > 0:
        send_email_report(
            to_emails=["operations07@srlpl.in"],
            cc_emails=cc_emails,
            pending_df=north_east_df,
            zone_name="North & East"
        )
    else:
        print("No data to send for North & East Zone")

    print("\n" + "=" * 60)
    print("All zone reports processed!")
    print("=" * 60)

def send_test_reports():
    """Send test reports to mis@srlpl.in"""
    print("=" * 60)
    print("Swift Billing - Pending POD Zone Reports (TEST)")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load pending POD data
    print("\nLoading pending POD data...")
    pending_df = load_pending_pod_data()
    print(f"Total pending POD CNs: {len(pending_df)}")

    test_email = "mis@srlpl.in"

    # ========== MAIL 3: West Zone ==========
    print("\n" + "-" * 50)
    print("MAIL 3: West Zone (TEST)")
    print("-" * 50)

    west_df = filter_by_zone(pending_df, 'West')
    print(f"West Zone CNs: {len(west_df)}")

    if len(west_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=west_df,
            zone_name="West"
        )
    else:
        print("No data to send for West Zone")

    # ========== MAIL 4: South Zone ==========
    print("\n" + "-" * 50)
    print("MAIL 4: South Zone (TEST)")
    print("-" * 50)

    south_df = filter_by_zone(pending_df, 'South')
    print(f"South Zone CNs: {len(south_df)}")

    if len(south_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=south_df,
            zone_name="South"
        )
    else:
        print("No data to send for South Zone")

    # ========== MAIL 5: North & East Zone ==========
    print("\n" + "-" * 50)
    print("MAIL 5: North & East Zone (TEST)")
    print("-" * 50)

    north_east_df = filter_by_zone(pending_df, 'North & East')
    print(f"North & East Zone CNs: {len(north_east_df)}")

    if len(north_east_df) > 0:
        send_email_report(
            to_emails=[test_email],
            cc_emails=None,
            pending_df=north_east_df,
            zone_name="North & East"
        )
    else:
        print("No data to send for North & East Zone")

    print("\n" + "=" * 60)
    print("All TEST reports sent to mis@srlpl.in!")
    print("=" * 60)

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_test_reports()
    else:
        send_all_zone_reports()
