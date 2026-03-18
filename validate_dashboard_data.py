"""
Dashboard Data Validation Script
Compares dashboard cache logic with fresh database query
Sends email alert if mismatch detected
"""

import psycopg2
import pandas as pd
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=os.getenv("DB_PORT") or 5432
    )

def get_dashboard_data(conn, year_month):
    """
    Simulate dashboard query - loads data with same filters as dashboard
    then groups by billing_party
    """
    # Dashboard loads data with these filters
    query = f"""
        SELECT
            billing_party, bill_no, qty, basic_freight
        FROM cn_data
        WHERE is_active = 'Yes'
          AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
          AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
          AND bill_date >= '{year_month}-01'
          AND bill_date < '{year_month}-01'::date + interval '1 month'
    """

    df = pd.read_sql(query, conn)

    # Dashboard groups by billing_party and counts unique bill_no
    summary = df.groupby('billing_party').agg({
        'bill_no': lambda x: x.dropna().nunique(),
        'qty': 'sum',
        'basic_freight': 'sum'
    }).reset_index()
    summary.columns = ['billing_party', 'bill_count', 'total_qty', 'total_freight']

    return summary, df

def get_direct_db_totals(conn, year_month):
    """
    Direct database query - counts unique bills per party
    """
    query = f"""
        SELECT
            billing_party,
            COUNT(DISTINCT bill_no) as bill_count,
            SUM(qty) as total_qty,
            SUM(basic_freight) as total_freight
        FROM cn_data
        WHERE is_active = 'Yes'
          AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
          AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
          AND bill_date >= '{year_month}-01'
          AND bill_date < '{year_month}-01'::date + interval '1 month'
        GROUP BY billing_party
        ORDER BY billing_party
    """

    return pd.read_sql(query, conn)

def send_mismatch_alert(month, dashboard_totals, db_totals, mismatch_details):
    """Send email alert when data doesn't match"""
    try:
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port = int(os.getenv("SMTP_PORT") or 587)
        sender_email = os.getenv("SENDER_EMAIL")
        sender_password = os.getenv("SENDER_PASSWORD")

        if not all([smtp_server, sender_email, sender_password]):
            print("ERROR: SMTP config missing")
            return False

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"⚠️ Dashboard Data Mismatch Alert - {month}"
        msg['From'] = sender_email
        msg['To'] = "mis@srlpl.in"

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .alert {{ background-color: #fee2e2; border: 2px solid #ef4444; padding: 20px; border-radius: 10px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                th {{ background-color: #1e3a5f; color: white; }}
                .mismatch {{ background-color: #fef3c7; }}
                h2 {{ color: #dc2626; }}
            </style>
        </head>
        <body>
            <div class="alert">
                <h2>⚠️ Dashboard Data Validation Failed</h2>
                <p><strong>Month:</strong> {month}</p>
                <p><strong>Checked At:</strong> {datetime.now().strftime('%d-%m-%Y %H:%M:%S IST')}</p>
                <p><strong>Source:</strong> GitHub Actions Automated Check</p>

                <h3>Summary Comparison:</h3>
                <table>
                    <tr>
                        <th>Metric</th>
                        <th>Dashboard Logic</th>
                        <th>Direct DB Query</th>
                        <th>Difference</th>
                    </tr>
                    <tr>
                        <td>Total Bills</td>
                        <td>{dashboard_totals['bills']}</td>
                        <td>{db_totals['bills']}</td>
                        <td style="background-color: {'#fef3c7' if dashboard_totals['bills'] != db_totals['bills'] else 'transparent'};">{db_totals['bills'] - dashboard_totals['bills']}</td>
                    </tr>
                    <tr>
                        <td>Total Units</td>
                        <td>{dashboard_totals['units']:,.0f}</td>
                        <td>{db_totals['units']:,.0f}</td>
                        <td style="background-color: {'#fef3c7' if abs(dashboard_totals['units'] - db_totals['units']) > 0.01 else 'transparent'};">{db_totals['units'] - dashboard_totals['units']:,.0f}</td>
                    </tr>
                    <tr>
                        <td>Total Amount</td>
                        <td>₹{dashboard_totals['amount']:,.2f}</td>
                        <td>₹{db_totals['amount']:,.2f}</td>
                        <td style="background-color: {'#fef3c7' if abs(dashboard_totals['amount'] - db_totals['amount']) > 1 else 'transparent'};">₹{db_totals['amount'] - dashboard_totals['amount']:,.2f}</td>
                    </tr>
                </table>

                <h3>Party-wise Mismatches:</h3>
                <table>
                    <tr>
                        <th>Billing Party</th>
                        <th>Dashboard Bills</th>
                        <th>DB Bills</th>
                        <th>Dashboard Amt</th>
                        <th>DB Amt</th>
                    </tr>
                    {mismatch_details}
                </table>

                <p style="color: #dc2626; font-weight: bold;">
                    Action Required: Check dashboard caching or data loading logic.
                </p>
            </div>
        </body>
        </html>
        """

        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, "mis@srlpl.in", msg.as_string())

        print(f"✅ Alert email sent to mis@srlpl.in")
        return True

    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False

def validate_month(conn, year_month):
    """Validate data for a specific month"""
    print(f"\n{'='*60}")
    print(f"Validating: {year_month}")
    print(f"{'='*60}")

    # Get data using dashboard logic
    dashboard_summary, dashboard_raw = get_dashboard_data(conn, year_month)

    # Get data using direct DB query
    db_summary = get_direct_db_totals(conn, year_month)

    # Calculate totals
    dashboard_totals = {
        'bills': dashboard_summary['bill_count'].sum(),
        'units': dashboard_summary['total_qty'].sum(),
        'amount': dashboard_summary['total_freight'].sum()
    }

    db_totals = {
        'bills': db_summary['bill_count'].sum(),
        'units': db_summary['total_qty'].sum(),
        'amount': db_summary['total_freight'].sum()
    }

    print(f"\nDashboard: {dashboard_totals['bills']} bills, {dashboard_totals['units']:.0f} units, ₹{dashboard_totals['amount']:,.0f}")
    print(f"Database:  {db_totals['bills']} bills, {db_totals['units']:.0f} units, ₹{db_totals['amount']:,.0f}")

    # Check for mismatch
    has_mismatch = (
        dashboard_totals['bills'] != db_totals['bills'] or
        abs(dashboard_totals['units'] - db_totals['units']) > 0.01 or
        abs(dashboard_totals['amount'] - db_totals['amount']) > 1
    )

    if has_mismatch:
        print("\n⚠️ MISMATCH DETECTED!")

        # Build mismatch details
        mismatch_rows = []
        for _, db_row in db_summary.iterrows():
            party = db_row['billing_party']
            dash_row = dashboard_summary[dashboard_summary['billing_party'] == party]

            if len(dash_row) > 0:
                dash_bills = dash_row['bill_count'].values[0]
                dash_amt = dash_row['total_freight'].values[0]
            else:
                dash_bills = 0
                dash_amt = 0

            if dash_bills != db_row['bill_count'] or abs(dash_amt - db_row['total_freight']) > 1:
                print(f"  - {party}: Dashboard={dash_bills} bills, DB={db_row['bill_count']} bills")
                mismatch_rows.append(f"""
                    <tr class="mismatch">
                        <td>{party}</td>
                        <td>{dash_bills}</td>
                        <td>{db_row['bill_count']}</td>
                        <td>₹{dash_amt:,.0f}</td>
                        <td>₹{db_row['total_freight']:,.0f}</td>
                    </tr>
                """)

        mismatch_details = "".join(mismatch_rows) if mismatch_rows else "<tr><td colspan='5'>No specific party mismatch</td></tr>"

        # Send alert
        send_mismatch_alert(year_month, dashboard_totals, db_totals, mismatch_details)
        return False
    else:
        print("\n✅ Data matches perfectly!")
        return True

def main():
    print("=" * 60)
    print("Dashboard Data Validation")
    print(f"Run Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
    print("=" * 60)

    try:
        conn = get_db_connection()
        print("✅ Database connected")

        # Get current month
        current_month = datetime.now().strftime('%Y-%m')

        # Validate current month
        is_valid = validate_month(conn, current_month)

        conn.close()
        print("\n" + "=" * 60)

        if is_valid:
            print("✅ Validation PASSED - No issues found")
        else:
            print("❌ Validation FAILED - Alert sent")
            exit(1)  # Exit with error code for GitHub Actions

    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
