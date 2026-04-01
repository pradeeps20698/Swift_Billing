import streamlit as st
import pandas as pd
import psycopg2
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# Try to load .env for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Get config from st.secrets (Streamlit Cloud) or environment variables (local)
def get_config(key):
    try:
        return st.secrets["database"][key]
    except:
        return os.getenv(key)

# Check if database has new data (uncached - for auto-refresh detection)
# Defined early so it's available before the auto-refresh logic runs
def check_for_new_data():
    """Check database for new data without using cache"""
    try:
        conn = psycopg2.connect(
            host=get_config("DB_HOST"),
            user=get_config("DB_USER"),
            password=get_config("DB_PASSWORD"),
            database=get_config("DB_NAME"),
            port=get_config("DB_PORT") or 5432
        )
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(updated_at) FROM cn_data WHERE (is_active = true OR is_active::text = 'Yes')")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        print(f"Error checking for new data: {e}")
        return None

# Send data mismatch alert email
def send_mismatch_alert(month, dashboard_data, db_data, mismatch_details):
    """Send email alert when dashboard and database data don't match"""
    try:
        smtp_server = get_config("SMTP_SERVER")
        smtp_port = int(get_config("SMTP_PORT") or 587)
        sender_email = get_config("SENDER_EMAIL")
        sender_password = get_config("SENDER_PASSWORD")

        if not all([smtp_server, sender_email, sender_password]):
            print("SMTP config missing, cannot send alert")
            return False

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"⚠️ Dashboard Data Mismatch Alert - {month}"
        msg['From'] = sender_email
        msg['To'] = "mis@srlpl.in"

        # Pre-compute mismatch classes
        bills_class = "mismatch" if dashboard_data['bills'] != db_data['bills'] else ""
        units_class = "mismatch" if dashboard_data['units'] != db_data['units'] else ""
        amount_class = "mismatch" if abs(dashboard_data['amount'] - db_data['amount']) > 1 else ""

        # Pre-compute differences
        bills_diff = db_data['bills'] - dashboard_data['bills']
        units_diff = db_data['units'] - dashboard_data['units']
        amount_diff = db_data['amount'] - dashboard_data['amount']

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
                <h2>⚠️ Dashboard Data Mismatch Detected</h2>
                <p><strong>Month:</strong> {month}</p>
                <p><strong>Time:</strong> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}</p>

                <h3>Summary Comparison:</h3>
                <table>
                    <tr>
                        <th>Metric</th>
                        <th>Dashboard</th>
                        <th>Database</th>
                        <th>Difference</th>
                    </tr>
                    <tr>
                        <td>Total Bills</td>
                        <td>{dashboard_data['bills']}</td>
                        <td>{db_data['bills']}</td>
                        <td class="{bills_class}">{bills_diff}</td>
                    </tr>
                    <tr>
                        <td>Total Units</td>
                        <td>{dashboard_data['units']:,.0f}</td>
                        <td>{db_data['units']:,.0f}</td>
                        <td class="{units_class}">{units_diff:,.0f}</td>
                    </tr>
                    <tr>
                        <td>Total Amount</td>
                        <td>₹{dashboard_data['amount']:,.2f}</td>
                        <td>₹{db_data['amount']:,.2f}</td>
                        <td class="{amount_class}">₹{amount_diff:,.2f}</td>
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

                <p style="color: #dc2626; font-weight: bold;">Please check the dashboard caching or data loading logic.</p>
            </div>
        </body>
        </html>
        """

        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, "mis@srlpl.in", msg.as_string())

        return True
    except Exception as e:
        print(f"Failed to send mismatch alert: {e}")
        return False

# Validate dashboard data against database
def validate_dashboard_data(dashboard_df, selected_month):
    """Compare dashboard totals with fresh database query"""
    try:
        if selected_month is None or dashboard_df is None or len(dashboard_df) == 0:
            return True  # Skip validation if no data

        # Parse month for query
        year_month = selected_month  # Format: "2026-03"
        start_date = f"{year_month}-01"
        # Calculate end date (first day of next month)
        year, month = map(int, year_month.split('-'))
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"

        # Fresh database query with same filters as dashboard
        conn = psycopg2.connect(
            host=get_config("DB_HOST"),
            user=get_config("DB_USER"),
            password=get_config("DB_PASSWORD"),
            database=get_config("DB_NAME"),
            port=get_config("DB_PORT") or 5432
        )

        query = f"""
            SELECT
                billing_party,
                COUNT(DISTINCT bill_no) as bill_count,
                SUM(qty) as total_qty,
                SUM(basic_freight) as total_freight
            FROM cn_data
            WHERE (is_active = true OR is_active::text = 'Yes')
              AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
              AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
              AND bill_date >= '{start_date}'
              AND bill_date < '{end_date}'
            GROUP BY billing_party
            ORDER BY billing_party
        """

        db_df = pd.read_sql(query, conn)
        conn.close()

        # Calculate dashboard totals
        dashboard_summary = dashboard_df.groupby('billing_party').agg({
            'bill_no': lambda x: x.dropna().nunique(),
            'qty': 'sum',
            'basic_freight': 'sum'
        }).reset_index()

        dashboard_totals = {
            'bills': dashboard_df['bill_no'].dropna().nunique(),
            'units': dashboard_df['qty'].sum(),
            'amount': dashboard_df['basic_freight'].sum()
        }

        db_totals = {
            'bills': db_df['bill_count'].sum(),
            'units': db_df['total_qty'].sum(),
            'amount': db_df['total_freight'].sum()
        }

        # Check for mismatch (allow small floating point differences)
        has_mismatch = (
            dashboard_totals['bills'] != db_totals['bills'] or
            abs(dashboard_totals['units'] - db_totals['units']) > 0.01 or
            abs(dashboard_totals['amount'] - db_totals['amount']) > 1
        )

        if has_mismatch:
            # Build mismatch details
            mismatch_rows = []
            for _, db_row in db_df.iterrows():
                party = db_row['billing_party']
                dash_row = dashboard_summary[dashboard_summary['billing_party'] == party]

                if len(dash_row) > 0:
                    dash_bills = dash_row['bill_no'].values[0]
                    dash_amt = dash_row['basic_freight'].values[0]
                else:
                    dash_bills = 0
                    dash_amt = 0

                if dash_bills != db_row['bill_count'] or abs(dash_amt - db_row['total_freight']) > 1:
                    mismatch_rows.append(f"""
                        <tr class="mismatch">
                            <td>{party}</td>
                            <td>{dash_bills}</td>
                            <td>{db_row['bill_count']}</td>
                            <td>₹{dash_amt:,.0f}</td>
                            <td>₹{db_row['total_freight']:,.0f}</td>
                        </tr>
                    """)

            mismatch_details = "".join(mismatch_rows) if mismatch_rows else "<tr><td colspan='5'>No specific party mismatch found</td></tr>"

            # Send alert email
            send_mismatch_alert(selected_month, dashboard_totals, db_totals, mismatch_details)
            return False

        return True

    except Exception as e:
        print(f"Validation error: {e}")
        return True  # Don't block on validation errors

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Swift Billing Dashboard",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh check every 5 minutes (300000 milliseconds)
# This checks if database has new data, only refreshes when needed
refresh_count = st_autorefresh(interval=300000, limit=None, key="billing_auto_refresh")

# Initialize session state for filters
if 'selected_month' not in st.session_state:
    st.session_state.selected_month = None
if 'selected_party' not in st.session_state:
    st.session_state.selected_party = "All"
if 'selected_branch' not in st.session_state:
    st.session_state.selected_branch = "All"
if 'search_query' not in st.session_state:
    st.session_state.search_query = ""
if 'last_db_refresh' not in st.session_state:
    st.session_state.last_db_refresh = None
if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0
if 'df' not in st.session_state:
    st.session_state.df = None
if 'db_api_update' not in st.session_state:
    st.session_state.db_api_update = None
if 'validation_pending' not in st.session_state:
    st.session_state.validation_pending = True  # Validate on first load

# Smart auto-refresh: Check if database has new data, only refresh when needed
if refresh_count > st.session_state.refresh_count:
    st.session_state.refresh_count = refresh_count

    # Check if database has newer data than what we're displaying
    latest_db_update = check_for_new_data()
    current_displayed = st.session_state.db_api_update

    # Compare timestamps - refresh only if database has newer data
    should_refresh = False
    if latest_db_update is not None:
        if current_displayed is None:
            should_refresh = True  # First load
        elif latest_db_update > current_displayed:
            should_refresh = True  # Database has newer data

    if should_refresh:
        st.session_state.df = None  # Clear cached data
        st.cache_resource.clear()  # Clear DB connection cache
        st.cache_data.clear()  # Clear cached data (filter_data, get_filter_options)
        st.session_state.validation_pending = True  # Trigger validation after auto-refresh
        st.rerun()  # Force rerun to reload fresh data from database

# Custom CSS for dark theme
st.markdown("""
<style>
    .stApp {
        background-color: #0a1628;
    }
    [data-testid="stSidebar"] {
        background-color: #0f1f3d;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
    }
    p, span, label, .stMarkdown {
        color: #e2e8f0 !important;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #162544 100%);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        border: 1px solid #2d4a6f;
    }
    .metric-card-green {
        background: linear-gradient(135deg, #065f46 0%, #064e3b 100%);
        border: 1px solid #10b981;
    }
    .metric-card-orange {
        background: linear-gradient(135deg, #9a3412 0%, #7c2d12 100%);
        border: 1px solid #f97316;
    }
    .metric-title {
        color: #94a3b8;
        font-size: 14px;
        margin-bottom: 8px;
    }
    .metric-value {
        color: #ffffff;
        font-size: 32px;
        font-weight: bold;
        margin-bottom: 12px;
    }
    .metric-breakdown {
        display: flex;
        justify-content: space-around;
        margin-top: 10px;
        padding-top: 10px;
        border-top: 1px solid #2d4a6f;
    }
    .breakdown-item {
        text-align: center;
    }
    .breakdown-label {
        color: #94a3b8;
        font-size: 12px;
    }
    .breakdown-value {
        color: #ffffff;
        font-size: 18px;
        font-weight: 600;
    }
    .breakdown-value-orange {
        color: #f97316;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #0f1f3d;
        padding: 8px;
        border-radius: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #162544;
        color: #94a3b8;
        border-radius: 6px;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #3b82f6 !important;
        color: #ffffff !important;
    }
    .section-header {
        color: #ffffff;
        font-size: 20px;
        font-weight: 600;
        margin: 20px 0 15px 0;
        padding-bottom: 10px;
        border-bottom: 2px solid #3b82f6;
    }
    thead tr th {
        background-color: #1e3a5f !important;
        color: #ffffff !important;
    }
    tbody tr {
        background-color: #162544 !important;
    }
    tbody tr:hover {
        background-color: #1e3a5f !important;
    }
    .filter-container {
        background-color: #162544;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 10px;
    }
    .stButton > button {
        background-color: #3b82f6;
        color: white;
        border: none;
        border-radius: 6px;
        width: 100%;
    }
    .stButton > button:hover {
        background-color: #2563eb;
    }
    div[data-testid="stTextInput"] input {
        background-color: #162544;
        color: #ffffff;
        border: 1px solid #2d4a6f;
    }
    div[data-testid="stSelectbox"] > div > div {
        background-color: #162544;
        color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=get_config("DB_HOST"),
        user=get_config("DB_USER"),
        password=get_config("DB_PASSWORD"),
        database=get_config("DB_NAME"),
        port=get_config("DB_PORT") or 5432
    )

# Load data function
def load_data():
    conn = get_connection()
    query = """
        SELECT
            cn_no, cn_date, branch, state, billing_party, bill_no, bill_date,
            route, origin, destination, km, eta, vehicle_no, vehicle_type,
            charge_weight, actual_weight, qty, basic_freight, detention,
            hamali, other_charges, penalty, total_freight, payment_received,
            deduction_lr, balance, consignee, material_detail, pod_status,
            pod_receipt_no, pod_date, unload_date
        FROM cn_data
        WHERE (is_active = true OR is_active::text = 'Yes')
          AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
          AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
        ORDER BY bill_date DESC NULLS LAST
    """
    df = pd.read_sql(query, conn)
    df['cn_date'] = pd.to_datetime(df['cn_date'])
    df['bill_date'] = pd.to_datetime(df['bill_date'])
    df['eta'] = pd.to_datetime(df['eta'])
    df['pod_date'] = pd.to_datetime(df['pod_date'])
    df['unload_date'] = pd.to_datetime(df['unload_date'])
    df['month'] = df['bill_date'].dt.to_period('M')
    df['is_own'] = df['vehicle_type'].str.lower().str.contains('own', na=False)
    return df

# Get last API update time from database
def get_db_last_update():
    conn = get_connection()
    query = "SELECT MAX(updated_at) FROM cn_data WHERE (is_active = true OR is_active::text = 'Yes')"
    result = pd.read_sql(query, conn)
    last_update = result.iloc[0, 0]
    return last_update

# Format currency (always in Lakhs)
def format_currency(val):
    if abs(val) >= 100000:
        return f"₹{val/100000:.2f}L"
    elif abs(val) >= 1000:
        return f"₹{val/1000:.2f}K"
    return f"₹{val:.0f}"

# Callback functions for filters
def on_month_change():
    st.session_state.selected_month = st.session_state.month_select

def on_party_change():
    st.session_state.selected_party = st.session_state.party_select

def on_branch_change():
    st.session_state.selected_branch = st.session_state.branch_select

def on_search_change():
    st.session_state.search_query = st.session_state.search_input

def refresh_data():
    st.cache_resource.clear()  # Clear cached connection
    st.cache_data.clear()  # Clear cached data (filter_data, get_filter_options)
    st.session_state.df = load_data()
    st.session_state.last_db_refresh = datetime.now()
    st.session_state.db_api_update = get_db_last_update()
    st.session_state.validation_pending = True  # Trigger validation after refresh
    st.success("Data refreshed from database!")

def clear_filters():
    st.session_state.selected_party = "All"
    st.session_state.selected_branch = "All"
    st.session_state.search_query = ""

# Cached function to get filter options from data (no DB call)
@st.cache_data
def get_filter_options(_df):
    valid_months = _df[_df['month'].notna()]['month'].unique()
    months = sorted(valid_months, reverse=True)
    month_options = [str(m) for m in months]
    parties = ["All"] + sorted(_df['billing_party'].dropna().unique().tolist())
    branches = ["All"] + sorted(_df['branch'].dropna().unique().tolist())
    return month_options, parties, branches

# Cached function to filter data (no DB call, works on cached data)
@st.cache_data
def filter_data(_df, selected_month, selected_party, selected_branch, search_query):
    filtered = _df.copy()

    # Apply month filter
    if selected_month:
        filtered = filtered[filtered['month'] == selected_month]

    # Apply party filter
    if selected_party != "All":
        filtered = filtered[filtered['billing_party'] == selected_party]

    # Apply branch filter
    if selected_branch != "All":
        filtered = filtered[filtered['branch'] == selected_branch]

    # Apply search filter
    if search_query:
        search_term = search_query.lower()
        filtered = filtered[
            filtered['cn_no'].str.lower().str.contains(search_term, na=False) |
            filtered['vehicle_no'].str.lower().str.contains(search_term, na=False) |
            filtered['billing_party'].str.lower().str.contains(search_term, na=False) |
            filtered['consignee'].str.lower().str.contains(search_term, na=False)
        ]

    return filtered

# Load data from database if not in session state (cache)
if st.session_state.df is None:
    st.session_state.df = load_data()
    st.session_state.last_db_refresh = datetime.now()
    st.session_state.db_api_update = get_db_last_update()

df = st.session_state.df

# Get filter options from cached data (fast, no DB call)
month_options, parties, branches = get_filter_options(df)

if st.session_state.selected_month is None:
    st.session_state.selected_month = month_options[0] if month_options else None

# Sidebar
st.sidebar.markdown("## 🎛️ Filters")

# Month filter
st.sidebar.markdown("### 📅 Bill Month")
month_index = month_options.index(st.session_state.selected_month) if st.session_state.selected_month in month_options else 0
st.sidebar.selectbox(
    "Select Month",
    month_options,
    index=month_index,
    key="month_select",
    on_change=on_month_change,
    label_visibility="collapsed"
)

# Party filter
st.sidebar.markdown("### 🏢 Billing Party")
party_index = parties.index(st.session_state.selected_party) if st.session_state.selected_party in parties else 0
st.sidebar.selectbox(
    "Select Party",
    parties,
    index=party_index,
    key="party_select",
    on_change=on_party_change,
    label_visibility="collapsed"
)

st.sidebar.markdown("---")

# Action buttons
col_btn1, col_btn2 = st.sidebar.columns(2)
with col_btn1:
    if st.button("🔄 Refresh", use_container_width=True):
        refresh_data()
        st.rerun()

with col_btn2:
    if st.button("🗑️ Clear", use_container_width=True):
        clear_filters()
        st.rerun()

# Show active filters
st.sidebar.markdown("---")
st.sidebar.markdown("### 📋 Active Filters")
active_filters = []
if st.session_state.selected_month:
    active_filters.append(f"Month: {st.session_state.selected_month}")
if st.session_state.selected_party != "All":
    active_filters.append(f"Party: {st.session_state.selected_party[:20]}...")

if active_filters:
    for f in active_filters:
        st.sidebar.markdown(f"- {f}")
else:
    st.sidebar.markdown("_No filters applied_")

# Filter data from cached session state (fast, no DB call)
filtered_df = filter_data(
    df,
    st.session_state.selected_month,
    st.session_state.selected_party,
    st.session_state.selected_branch,
    st.session_state.search_query
)

# Validate data against database (only when validation is pending and no party/branch filter)
if st.session_state.validation_pending and st.session_state.selected_party == "All" and st.session_state.selected_branch == "All":
    is_valid = validate_dashboard_data(filtered_df, st.session_state.selected_month)
    st.session_state.validation_pending = False
    if not is_valid:
        st.warning("⚠️ Data mismatch detected! Alert email sent to mis@srlpl.in")

# Main content
st.markdown("<h1 style='text-align: center; color: white;'>🧾 Swift Billing Dashboard</h1>", unsafe_allow_html=True)

# Month display
if st.session_state.selected_month:
    month_display = datetime.strptime(st.session_state.selected_month, "%Y-%m").strftime("%B %Y")
    st.markdown(f"<h3 style='color: #94a3b8;'>Billing Summary ({month_display})</h3>", unsafe_allow_html=True)

# Calculate metrics - Count unique bill numbers
total_bills = filtered_df['bill_no'].dropna().nunique()
own_bills = filtered_df[filtered_df['is_own'] == True]['bill_no'].dropna().nunique()
hire_bills = filtered_df[filtered_df['is_own'] == False]['bill_no'].dropna().nunique()

# For John Deere India Private Limited, include other_charges in freight calculation
john_deere_filtered = filtered_df[filtered_df['billing_party'] == 'John Deere India Private Limited']
other_filtered = filtered_df[filtered_df['billing_party'] != 'John Deere India Private Limited']
total_freight = other_filtered['basic_freight'].sum() + john_deere_filtered['basic_freight'].sum() + john_deere_filtered['other_charges'].fillna(0).sum()

own_df = filtered_df[filtered_df['is_own'] == True]
john_deere_own = own_df[own_df['billing_party'] == 'John Deere India Private Limited']
other_own = own_df[own_df['billing_party'] != 'John Deere India Private Limited']
own_freight = other_own['basic_freight'].sum() + john_deere_own['basic_freight'].sum() + john_deere_own['other_charges'].fillna(0).sum()

hire_df = filtered_df[filtered_df['is_own'] == False]
john_deere_hire = hire_df[hire_df['billing_party'] == 'John Deere India Private Limited']
other_hire = hire_df[hire_df['billing_party'] != 'John Deere India Private Limited']
hire_freight = other_hire['basic_freight'].sum() + john_deere_hire['basic_freight'].sum() + john_deere_hire['other_charges'].fillna(0).sum()

total_qty = filtered_df['qty'].sum()
own_qty = filtered_df[filtered_df['is_own'] == True]['qty'].sum()
hire_qty = filtered_df[filtered_df['is_own'] == False]['qty'].sum()

# Metric cards row
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">No of Bills</div>
        <div class="metric-value">{total_bills:,}</div>
        <div class="metric-breakdown">
            <div class="breakdown-item">
                <div class="breakdown-label">Own</div>
                <div class="breakdown-value">{own_bills:,}</div>
            </div>
            <div class="breakdown-item">
                <div class="breakdown-label">Hire</div>
                <div class="breakdown-value breakdown-value-orange">{hire_bills:,}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Cars/Units Lifted</div>
        <div class="metric-value">{int(total_qty):,}</div>
        <div class="metric-breakdown">
            <div class="breakdown-item">
                <div class="breakdown-label">Own</div>
                <div class="breakdown-value">{int(own_qty):,}</div>
            </div>
            <div class="breakdown-item">
                <div class="breakdown-label">Hire</div>
                <div class="breakdown-value breakdown-value-orange">{int(hire_qty):,}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card metric-card-green">
        <div class="metric-title">Billed Amount (Basic Freight)</div>
        <div class="metric-value">{format_currency(total_freight)}</div>
        <div class="metric-breakdown">
            <div class="breakdown-item">
                <div class="breakdown-label">Own</div>
                <div class="breakdown-value">{format_currency(own_freight)}</div>
            </div>
            <div class="breakdown-item">
                <div class="breakdown-label">Hire</div>
                <div class="breakdown-value breakdown-value-orange">{format_currency(hire_freight)}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Client-Wise Summary",
    "📈 Monthly/Day Trend",
    "💰 Unbilled CNs",
    "📦 Pending POD"
])

with tab1:
    st.markdown("<div class='section-header'>Client-Wise Billing Summary</div>", unsafe_allow_html=True)

    if len(filtered_df) > 0:
        # Calculate Pooja and Rohit billing summaries
        pooja_df = filtered_df[~filtered_df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]
        rohit_df = filtered_df[filtered_df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]

        pooja_bills = pooja_df['bill_no'].nunique()
        pooja_amount = pooja_df['basic_freight'].sum()
        rohit_bills = rohit_df['bill_no'].nunique()
        # For Rohit billing (includes John Deere), add other_charges for John Deere India Private Limited
        john_deere_df = rohit_df[rohit_df['billing_party'] == 'John Deere India Private Limited']
        other_rohit_df = rohit_df[rohit_df['billing_party'] != 'John Deere India Private Limited']
        rohit_amount = other_rohit_df['basic_freight'].sum() + john_deere_df['basic_freight'].sum() + john_deere_df['other_charges'].fillna(0).sum()

        # Display Pooja and Rohit billing boxes
        col_pooja, col_rohit = st.columns(2)

        with col_pooja:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #7c3aed 0%, #5b21b6 100%); padding: 20px; border-radius: 12px; margin-bottom: 20px;">
                <div style="color: #e9d5ff; font-size: 14px; font-weight: 600; margin-bottom: 8px;">👩 Pooja Ma'am Billed</div>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="text-align: center;">
                        <div style="color: #c4b5fd; font-size: 12px;">No of Bills</div>
                        <div style="color: white; font-size: 28px; font-weight: bold;">{pooja_bills:,}</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="color: #c4b5fd; font-size: 12px;">Billed Amount</div>
                        <div style="color: white; font-size: 28px; font-weight: bold;">₹{pooja_amount/100000:.2f}L</div>
                    </div>
                </div>
                <div style="color: #a78bfa; font-size: 10px; margin-top: 8px;">Excludes: Mahindra, John Deere</div>
            </div>
            """, unsafe_allow_html=True)

        with col_rohit:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #0891b2 0%, #0e7490 100%); padding: 20px; border-radius: 12px; margin-bottom: 20px;">
                <div style="color: #cffafe; font-size: 14px; font-weight: 600; margin-bottom: 8px;">👨 Rohit Sir Billed</div>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="text-align: center;">
                        <div style="color: #a5f3fc; font-size: 12px;">No of Bills</div>
                        <div style="color: white; font-size: 28px; font-weight: bold;">{rohit_bills:,}</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="color: #a5f3fc; font-size: 12px;">Billed Amount</div>
                        <div style="color: white; font-size: 28px; font-weight: bold;">₹{rohit_amount/100000:.2f}L</div>
                    </div>
                </div>
                <div style="color: #67e8f9; font-size: 10px; margin-top: 8px;">Includes: Mahindra, John Deere</div>
            </div>
            """, unsafe_allow_html=True)

        # Define parent company groupings
        def get_parent_group(party):
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

        # Get client summary - count unique bill numbers
        # For John Deere India Private Limited, use basic_freight + other_charges
        filtered_df['calc_amount'] = filtered_df.apply(
            lambda row: row['basic_freight'] + (row['other_charges'] if pd.notna(row['other_charges']) else 0)
            if row['billing_party'] == 'John Deere India Private Limited'
            else row['basic_freight'],
            axis=1
        )
        client_summary = filtered_df.groupby('billing_party').agg({
            'bill_no': 'nunique',
            'qty': 'sum',
            'calc_amount': 'sum'
        }).reset_index()
        client_summary.columns = ['Billing Party', 'No of Bills', 'Units', 'Basic Freight']

        # Add parent group
        client_summary['Group'] = client_summary['Billing Party'].apply(get_parent_group)

        # Build grouped table with subtotals
        grouped_rows = []
        single_parties = []  # Collect single parties to show at bottom

        # First: Groups with multiple parties (show subtotals)
        group_order = ['Honda', 'M & M', 'Toyota', 'Glovis', 'Tata', 'Skoda VW', 'John Deere', 'ValueDrive']

        for group in group_order:
            group_data = client_summary[client_summary['Group'] == group].sort_values('Basic Freight', ascending=False)
            if len(group_data) > 1:
                # Multiple parties in group - show with subtotal
                for _, row in group_data.iterrows():
                    grouped_rows.append({
                        'Billing Party': row['Billing Party'],
                        'No of Bills': int(row['No of Bills']),
                        'Units': int(row['Units']),
                        'Basic Freight': row['Basic Freight'],
                        'is_total': False
                    })
                # Add group subtotal
                grouped_rows.append({
                    'Billing Party': f'{group} - Total',
                    'No of Bills': int(group_data['No of Bills'].sum()),
                    'Units': int(group_data['Units'].sum()),
                    'Basic Freight': group_data['Basic Freight'].sum(),
                    'is_total': True
                })
            elif len(group_data) == 1:
                # Single party in group - add to single parties list
                row = group_data.iloc[0]
                single_parties.append({
                    'Billing Party': row['Billing Party'],
                    'No of Bills': int(row['No of Bills']),
                    'Units': int(row['Units']),
                    'Basic Freight': row['Basic Freight'],
                    'is_total': False
                })

        # Second: Add Others to single parties
        others_data = client_summary[client_summary['Group'] == 'Others'].sort_values('Basic Freight', ascending=False)
        for _, row in others_data.iterrows():
            single_parties.append({
                'Billing Party': row['Billing Party'],
                'No of Bills': int(row['No of Bills']),
                'Units': int(row['Units']),
                'Basic Freight': row['Basic Freight'],
                'is_total': False
            })

        # Sort single parties by Basic Freight and add to grouped_rows
        single_parties_sorted = sorted(single_parties, key=lambda x: x['Basic Freight'], reverse=True)
        grouped_rows.extend(single_parties_sorted)

        # Add Grand Total
        grouped_rows.append({
            'Billing Party': 'Grand Total',
            'No of Bills': int(client_summary['No of Bills'].sum()),
            'Units': int(client_summary['Units'].sum()),
            'Basic Freight': client_summary['Basic Freight'].sum(),
            'is_total': True
        })

        # Create display dataframe
        display_df = pd.DataFrame(grouped_rows)

        # Format for display with highlighting
        def highlight_totals(row):
            if row['is_total']:
                return ['background-color: #b8860b; color: white; font-weight: bold'] * len(row)
            return [''] * len(row)

        # Prepare display columns
        display_df['Basic Freight'] = display_df['Basic Freight'].apply(lambda x: f"₹{x:,.0f}")
        styled_df = display_df[['Billing Party', 'No of Bills', 'Units', 'Basic Freight']].copy()
        styled_df['_is_total'] = display_df['is_total']

        # Build HTML table with improved styling
        html_table = """
        <div style='max-height: 500px; overflow-y: auto; border-radius: 10px;'>
        <table style='width:100%; border-collapse: collapse; color: white; font-size: 14px;'>
        <thead>
            <tr style='background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%); position: sticky; top: 0;'>
                <th style='padding: 12px 15px; text-align: left; font-weight: 600; border-bottom: 2px solid #3b82f6;'>Billing Party</th>
                <th style='padding: 12px 15px; text-align: right; font-weight: 600; border-bottom: 2px solid #3b82f6;'>No of Bills</th>
                <th style='padding: 12px 15px; text-align: right; font-weight: 600; border-bottom: 2px solid #3b82f6;'>Units</th>
                <th style='padding: 12px 15px; text-align: right; font-weight: 600; border-bottom: 2px solid #3b82f6;'>Billed Amount</th>
            </tr>
        </thead>
        <tbody>
        """

        row_idx = 0
        for _, row in display_df.iterrows():
            if row['Billing Party'] == 'Grand Total':
                # Grand Total - Green prominent row
                style = "background: linear-gradient(135deg, #065f46 0%, #047857 100%); color: white; font-weight: bold; font-size: 15px;"
                border_style = "border-top: 2px solid #10b981;"
            elif row['is_total']:
                # Group subtotals - Gold/Orange row
                style = "background: linear-gradient(135deg, #b8860b 0%, #d4a017 100%); color: #000000; font-weight: bold;"
                border_style = ""
            else:
                # Alternating rows
                if row_idx % 2 == 0:
                    style = "background-color: #162544;"
                else:
                    style = "background-color: #1a2d4d;"
                border_style = ""
                row_idx += 1

            html_table += f"<tr style='{style}'>"
            html_table += f"<td style='padding: 10px 15px; border-bottom: 1px solid #2d4a6f; {border_style}'>{row['Billing Party']}</td>"
            html_table += f"<td style='padding: 10px 15px; text-align: right; border-bottom: 1px solid #2d4a6f; {border_style}'>{row['No of Bills']:,}</td>"
            html_table += f"<td style='padding: 10px 15px; text-align: right; border-bottom: 1px solid #2d4a6f; {border_style}'>{row['Units']:,}</td>"
            html_table += f"<td style='padding: 10px 15px; text-align: right; border-bottom: 1px solid #2d4a6f; {border_style}'>{row['Basic Freight']}</td>"
            html_table += "</tr>"

        html_table += "</tbody></table></div>"

        st.markdown(html_table, unsafe_allow_html=True)
    else:
        st.info("No data found for the selected filters.")

with tab3:
    st.markdown("<div class='section-header'>Unbilled CN - POD Received</div>", unsafe_allow_html=True)
    st.markdown("<p style='color: #64748b; font-size: 12px;'>CNs where Bill No is blank but POD Receipt No exists - grouped by Party and Month</p>", unsafe_allow_html=True)

    # Get unbilled CNs with POD received from FULL dataset
    unbilled_df = df[
        (df['bill_no'].isna() | (df['bill_no'] == '') | (df['bill_no'].astype(str).str.strip() == '')) &
        (df['pod_receipt_no'].notna()) &
        (df['pod_receipt_no'] != '') &
        (df['pod_receipt_no'].astype(str).str.strip() != '')
    ].copy()

    unbilled_df['cn_month'] = unbilled_df['cn_date'].dt.to_period('M')

    col1, col2 = st.columns([1, 4])

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total No. of CN</div>
            <div class="metric-value">{len(unbilled_df):,}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Qty</div>
            <div class="metric-value">{int(unbilled_df['qty'].sum()):,}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # For John Deere India Private Limited, include other_charges
        jd_unbilled = unbilled_df[unbilled_df['billing_party'] == 'John Deere India Private Limited']
        other_unbilled = unbilled_df[unbilled_df['billing_party'] != 'John Deere India Private Limited']
        total_unbilled_amount = other_unbilled['basic_freight'].sum() + jd_unbilled['basic_freight'].sum() + jd_unbilled['other_charges'].fillna(0).sum()
        st.markdown(f"""
        <div class="metric-card metric-card-green">
            <div class="metric-title">Total Unbilled Amount</div>
            <div class="metric-value">{format_currency(total_unbilled_amount)}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        valid_months = unbilled_df[unbilled_df['cn_month'].notna()]['cn_month'].unique()
        months_sorted = sorted(valid_months, reverse=True)[:9]

        if len(unbilled_df) > 0:
            # Define parent company groupings (same as Client-Wise Summary)
            def get_parent_group_unbilled(party):
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

            # Build pivot data with group info
            pivot_data = []
            parties = unbilled_df['billing_party'].dropna().unique()

            for party in parties:
                party_data = unbilled_df[unbilled_df['billing_party'] == party]
                row = {'Billing Party': party, 'Group': get_parent_group_unbilled(party)}

                for month in months_sorted:
                    month_data = party_data[party_data['cn_month'] == month]
                    month_str = str(month)
                    month_label = datetime.strptime(month_str, "%Y-%m").strftime("%b'%y")

                    row[f'{month_label}_CN'] = len(month_data)
                    row[f'{month_label}_Qty'] = int(month_data['qty'].sum())
                    # For John Deere India Private Limited, use basic_freight + other_charges
                    if party == 'John Deere India Private Limited':
                        row[f'{month_label}_Amt'] = month_data['basic_freight'].sum() + month_data['other_charges'].fillna(0).sum()
                    else:
                        row[f'{month_label}_Amt'] = month_data['basic_freight'].sum()

                # Calculate total amount for sorting
                total_amt = 0
                for m in months_sorted:
                    ml = datetime.strptime(str(m), "%Y-%m").strftime("%b'%y")
                    total_amt += row.get(f'{ml}_Amt', 0)
                row['Total_Amt'] = total_amt
                pivot_data.append(row)

            pivot_df = pd.DataFrame(pivot_data)

            # Get month labels for columns
            month_labels = [datetime.strptime(str(m), "%Y-%m").strftime("%b'%y") for m in months_sorted]

            # Build grouped rows with subtotals
            grouped_rows = []
            single_parties = []  # Collect single parties to show at bottom
            group_order = ['M & M', 'Toyota', 'Glovis', 'Tata', 'Honda', 'Skoda VW', 'John Deere', 'ValueDrive']

            for group in group_order:
                group_data = pivot_df[pivot_df['Group'] == group].sort_values('Total_Amt', ascending=False)
                if len(group_data) > 1:
                    # Multiple parties - show with subtotal
                    for _, row in group_data.iterrows():
                        row_dict = {'Billing Party': row['Billing Party'], 'is_total': False}
                        for ml in month_labels:
                            row_dict[f'{ml}_CN'] = row.get(f'{ml}_CN', 0)
                            row_dict[f'{ml}_Qty'] = row.get(f'{ml}_Qty', 0)
                            row_dict[f'{ml}_Amt'] = row.get(f'{ml}_Amt', 0)
                        row_dict['_total_amt'] = row.get('Total_Amt', 0)
                        grouped_rows.append(row_dict)

                    # Add group subtotal
                    subtotal_row = {'Billing Party': f'{group} - Total', 'is_total': True}
                    for ml in month_labels:
                        subtotal_row[f'{ml}_CN'] = int(group_data[f'{ml}_CN'].sum())
                        subtotal_row[f'{ml}_Qty'] = int(group_data[f'{ml}_Qty'].sum())
                        subtotal_row[f'{ml}_Amt'] = group_data[f'{ml}_Amt'].sum()
                    grouped_rows.append(subtotal_row)
                elif len(group_data) == 1:
                    # Single party - add to single parties list for bottom
                    row = group_data.iloc[0]
                    row_dict = {'Billing Party': row['Billing Party'], 'is_total': False}
                    for ml in month_labels:
                        row_dict[f'{ml}_CN'] = row.get(f'{ml}_CN', 0)
                        row_dict[f'{ml}_Qty'] = row.get(f'{ml}_Qty', 0)
                        row_dict[f'{ml}_Amt'] = row.get(f'{ml}_Amt', 0)
                    row_dict['_total_amt'] = row.get('Total_Amt', 0)
                    single_parties.append(row_dict)

            # Add Others group to single parties
            others_data = pivot_df[pivot_df['Group'] == 'Others'].sort_values('Total_Amt', ascending=False)
            for _, row in others_data.iterrows():
                row_dict = {'Billing Party': row['Billing Party'], 'is_total': False}
                for ml in month_labels:
                    row_dict[f'{ml}_CN'] = row.get(f'{ml}_CN', 0)
                    row_dict[f'{ml}_Qty'] = row.get(f'{ml}_Qty', 0)
                    row_dict[f'{ml}_Amt'] = row.get(f'{ml}_Amt', 0)
                row_dict['_total_amt'] = row.get('Total_Amt', 0)
                single_parties.append(row_dict)

            # Sort single parties by total amount and add to grouped_rows
            single_parties_sorted = sorted(single_parties, key=lambda x: x.get('_total_amt', 0), reverse=True)
            grouped_rows.extend(single_parties_sorted)

            # Calculate Grand Total
            grand_total = {'Billing Party': 'Grand Total', 'is_total': True, 'is_grand': True}
            for ml in month_labels:
                grand_total[f'{ml}_CN'] = int(pivot_df[f'{ml}_CN'].sum())
                grand_total[f'{ml}_Qty'] = int(pivot_df[f'{ml}_Qty'].sum())
                grand_total[f'{ml}_Amt'] = pivot_df[f'{ml}_Amt'].sum()

            # Build HTML table with bold borders and sticky headers
            html_parts = []
            html_parts.append("<div style='max-height: 500px; overflow-x: auto; overflow-y: auto; border-radius: 10px; border: 2px solid #3b82f6;'>")
            html_parts.append("<table style='width:100%; border-collapse: collapse; color: white; font-size: 12px; min-width: 1200px;'>")
            html_parts.append("<thead>")
            # First header row - month names
            html_parts.append("<tr style='background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%); position: sticky; top: 0; z-index: 3;'>")
            html_parts.append("<th style='padding: 10px; text-align: left; font-weight: 600; border: 2px solid #3b82f6; min-width: 250px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>Billing Party</th>")

            # Add month column headers
            for ml in month_labels:
                html_parts.append(f"<th style='padding: 8px 4px; text-align: center; font-weight: 600; border: 2px solid #3b82f6; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);' colspan='3'>{ml}</th>")

            html_parts.append("</tr>")
            # Second header row - sub-columns
            html_parts.append("<tr style='background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%); position: sticky; top: 35px; z-index: 3;'>")
            html_parts.append("<th style='padding: 6px; border: 2px solid #3b82f6; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'></th>")

            for i, ml in enumerate(month_labels):
                html_parts.append("<th style='padding: 6px 4px; text-align: right; font-weight: 500; border-bottom: 2px solid #3b82f6; border-left: 2px solid #3b82f6; font-size: 10px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>No. of CN</th>")
                html_parts.append("<th style='padding: 6px 4px; text-align: right; font-weight: 500; border-bottom: 2px solid #3b82f6; font-size: 10px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>Qty</th>")
                html_parts.append("<th style='padding: 6px 4px; text-align: right; font-weight: 500; border-bottom: 2px solid #3b82f6; border-right: 2px solid #3b82f6; font-size: 10px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>Unbilled Amt</th>")

            html_parts.append("</tr>")
            # Grand Total row - sticky
            html_parts.append("<tr style='background: linear-gradient(135deg, #065f46 0%, #047857 100%); color: white; font-weight: bold; position: sticky; top: 62px; z-index: 2;'>")
            html_parts.append(f"<th style='padding: 10px; text-align: left; border: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{grand_total['Billing Party']}</th>")
            for i, ml in enumerate(month_labels):
                cn_val = grand_total[f'{ml}_CN']
                qty_val = grand_total[f'{ml}_Qty']
                amt_val = grand_total[f'{ml}_Amt']
                cn_display = cn_val if cn_val > 0 else '-'
                qty_display = qty_val if qty_val > 0 else '-'
                amt_display = f"₹{amt_val:,.0f}" if amt_val > 0 else '-'
                html_parts.append(f"<th style='padding: 8px 4px; text-align: right; border-bottom: 2px solid #10b981; border-left: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{cn_display}</th>")
                html_parts.append(f"<th style='padding: 8px 4px; text-align: right; border-bottom: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{qty_display}</th>")
                html_parts.append(f"<th style='padding: 8px 4px; text-align: right; border-bottom: 2px solid #10b981; border-right: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{amt_display}</th>")
            html_parts.append("</tr>")
            html_parts.append("</thead><tbody>")

            # Add data rows
            row_idx = 0
            for row in grouped_rows:
                if row.get('is_total'):
                    style = "background: linear-gradient(135deg, #b8860b 0%, #d4a017 100%); color: #000000; font-weight: bold;"
                    border_color = "#b8860b"
                else:
                    if row_idx % 2 == 0:
                        style = "background-color: #162544;"
                    else:
                        style = "background-color: #1a2d4d;"
                    row_idx += 1
                    border_color = "#3b82f6"

                html_parts.append(f"<tr style='{style}'>")
                html_parts.append(f"<td style='padding: 8px 10px; border: 1px solid {border_color}; border-left: 2px solid #3b82f6; border-right: 2px solid #3b82f6;'>{row['Billing Party']}</td>")

                for i, ml in enumerate(month_labels):
                    cn_val = row.get(f'{ml}_CN', 0)
                    qty_val = row.get(f'{ml}_Qty', 0)
                    amt_val = row.get(f'{ml}_Amt', 0)
                    cn_display = int(cn_val) if cn_val > 0 else '-'
                    qty_display = int(qty_val) if qty_val > 0 else '-'
                    amt_display = f"₹{amt_val:,.0f}" if amt_val > 0 else '-'
                    html_parts.append(f"<td style='padding: 6px 4px; text-align: right; border-bottom: 1px solid {border_color}; border-left: 2px solid #3b82f6;'>{cn_display}</td>")
                    html_parts.append(f"<td style='padding: 6px 4px; text-align: right; border-bottom: 1px solid {border_color};'>{qty_display}</td>")
                    html_parts.append(f"<td style='padding: 6px 4px; text-align: right; border-bottom: 1px solid {border_color}; border-right: 2px solid #3b82f6;'>{amt_display}</td>")

                html_parts.append("</tr>")

            html_parts.append("</tbody></table></div>")
            html_table = "".join(html_parts)

            st.markdown(html_table, unsafe_allow_html=True)

            # Download button - CN-wise details
            @st.cache_data
            def convert_df_to_csv(dataframe):
                return dataframe.to_csv(index=False).encode('utf-8')

            # Prepare CN-wise download data
            download_df = unbilled_df[['cn_no', 'cn_date', 'branch', 'billing_party', 'route', 'vehicle_no', 'qty', 'basic_freight', 'pod_receipt_no']].copy()
            download_df.columns = ['CN No', 'CN Date', 'Branch', 'Billing Party', 'Route', 'Vehicle No', 'Qty', 'Basic Freight', 'POD Receipt No']
            download_df['CN Date'] = download_df['CN Date'].dt.strftime('%d-%m-%Y')
            download_df = download_df.sort_values(['Billing Party', 'CN Date'], ascending=[True, False])

            csv = convert_df_to_csv(download_df)
            st.download_button(
                label="📥 Download Unbilled CN Data",
                data=csv,
                file_name=f"unbilled_cn_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No unbilled CNs with POD received found.")

        # POD Received Aging - Branch Wise Section
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<div class='section-header'>POD Received Aging - Branch Wise</div>", unsafe_allow_html=True)
        st.markdown("<p style='color: #64748b; font-size: 12px;'>Days taken to receive POD after Unload Date (Current Month - Most Frequent)</p>", unsafe_allow_html=True)

        # Filter data: POD received (not blank), current month based on ETA
        current_month = datetime.now().strftime('%Y-%m')

        pod_aging_df = df[
            (df['pod_receipt_no'].notna()) &
            (df['pod_receipt_no'] != '') &
            (df['pod_receipt_no'].astype(str).str.strip() != '') &
            (df['eta'].notna())
        ].copy()

        # Filter for current month based on ETA
        pod_aging_df['eta_date'] = pd.to_datetime(pod_aging_df['eta'], errors='coerce')
        pod_aging_df['eta_month'] = pod_aging_df['eta_date'].dt.to_period('M').astype(str)
        pod_aging_df = pod_aging_df[pod_aging_df['eta_month'] == current_month]

        if len(pod_aging_df) > 0:
            # Calculate aging: pod_date - unload_date
            pod_aging_df['pod_date_parsed'] = pd.to_datetime(pod_aging_df['pod_date'], errors='coerce')
            pod_aging_df['unload_date_parsed'] = pd.to_datetime(pod_aging_df['unload_date'], errors='coerce')
            pod_aging_df['aging_days'] = (pod_aging_df['pod_date_parsed'] - pod_aging_df['unload_date_parsed']).dt.days

            # Remove outliers (keep only reasonable values, e.g., -10 to 30 days)
            pod_aging_df = pod_aging_df[
                (pod_aging_df['aging_days'].notna()) &
                (pod_aging_df['aging_days'] >= -10) &
                (pod_aging_df['aging_days'] <= 30)
            ]

            # Group by branch and find most frequent (mode) aging days
            branch_aging = []
            for branch in pod_aging_df['branch'].dropna().unique():
                branch_data = pod_aging_df[pod_aging_df['branch'] == branch]['aging_days']
                if len(branch_data) > 0:
                    # Get mode (most frequent value)
                    mode_val = branch_data.mode()
                    if len(mode_val) > 0:
                        most_frequent = int(mode_val.iloc[0])
                        count = len(branch_data)
                        branch_aging.append({
                            'Branch': branch,
                            'Most Frequent Days': most_frequent,
                            'Count': count
                        })

            if branch_aging:
                branch_aging_df = pd.DataFrame(branch_aging)
                # Sort by most frequent days
                branch_aging_df = branch_aging_df.sort_values('Most Frequent Days', ascending=True)

                # Build HTML table
                html_parts = []
                html_parts.append("<div style='overflow-x: auto;'>")
                html_parts.append("<table style='width: 100%; border-collapse: collapse; border: 2px solid #3b82f6;'>")
                html_parts.append("<thead>")
                html_parts.append("<tr style='background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>")
                html_parts.append("<th style='padding: 12px; text-align: left; color: white; border: 1px solid #3b82f6;'>Branch</th>")
                html_parts.append("<th style='padding: 12px; text-align: center; color: white; border: 1px solid #3b82f6;'>Days to Receive POD (Most Frequent)</th>")
                html_parts.append("</tr>")
                html_parts.append("</thead>")
                html_parts.append("<tbody>")

                for idx, row in branch_aging_df.iterrows():
                    days = row['Most Frequent Days']
                    branch_name = row['Branch']

                    # Color coding based on days
                    if days <= 0:
                        day_text = "Same Day" if days == 0 else f"{abs(days)} days early"
                        day_color = "#10b981"  # Green
                    elif days <= 3:
                        day_text = f"{days} days"
                        day_color = "#f59e0b"  # Yellow/Orange
                    else:
                        day_text = f"{days} days"
                        day_color = "#ef4444"  # Red

                    bg_color = "#162544" if idx % 2 == 0 else "#1a2d4d"
                    html_parts.append(f"<tr style='background-color: {bg_color};'>")
                    html_parts.append(f"<td style='padding: 10px; border: 1px solid #3b82f6; color: white; font-weight: bold;'>{branch_name}</td>")
                    html_parts.append(f"<td style='padding: 10px; border: 1px solid #3b82f6; text-align: center; color: {day_color}; font-weight: bold;'>{day_text}</td>")
                    html_parts.append("</tr>")

                html_parts.append("</tbody></table></div>")
                st.markdown("".join(html_parts), unsafe_allow_html=True)
            else:
                st.info("No branch aging data available for current month.")
        else:
            st.info("No POD received data available for current month.")

with tab2:
    st.markdown("<div class='section-header'>Monthly Billing Trend</div>", unsafe_allow_html=True)

    # Split data into Pooja and Rohit categories
    pooja_data = df[~df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]
    rohit_data = df[df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]

    # For John Deere India Private Limited, use basic_freight + other_charges
    df['trend_amount'] = df.apply(
        lambda row: row['basic_freight'] + (row['other_charges'] if pd.notna(row['other_charges']) else 0)
        if row['billing_party'] == 'John Deere India Private Limited'
        else row['basic_freight'],
        axis=1
    )
    pooja_data_trend = df[~df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]
    rohit_data_trend = df[df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]

    # Total monthly trend
    monthly_trend = df.groupby('month').agg({
        'bill_no': lambda x: x.dropna().nunique(),
        'qty': 'sum',
        'trend_amount': 'sum'
    }).reset_index()
    monthly_trend['month'] = monthly_trend['month'].astype(str)
    monthly_trend.columns = ['Month', 'No of Bills', 'Units', 'Billed Amount']

    # Pooja Ma'am monthly trend
    pooja_trend = pooja_data_trend.groupby('month').agg({
        'bill_no': lambda x: x.dropna().nunique(),
        'qty': 'sum',
        'trend_amount': 'sum'
    }).reset_index()
    pooja_trend['month'] = pooja_trend['month'].astype(str)
    pooja_trend.columns = ['Month', 'No of Bills', 'Units', 'Billed Amount']

    # Rohit Sir monthly trend
    rohit_trend = rohit_data_trend.groupby('month').agg({
        'bill_no': lambda x: x.dropna().nunique(),
        'qty': 'sum',
        'trend_amount': 'sum'
    }).reset_index()
    rohit_trend['month'] = rohit_trend['month'].astype(str)
    rohit_trend.columns = ['Month', 'No of Bills', 'Units', 'Billed Amount']

    # Combine Pooja and Rohit into stacked bar chart with values
    import altair as alt

    pooja_chart = pooja_trend.set_index('Month')[['Billed Amount']].tail(6)
    pooja_chart.columns = ['Pooja Ma\'am']

    rohit_chart = rohit_trend.set_index('Month')[['Billed Amount']].tail(6)
    rohit_chart.columns = ['Rohit Sir']

    # Merge Pooja and Rohit data into one dataframe
    combined_chart = pooja_chart.join(rohit_chart, how='outer').fillna(0)
    combined_chart['Total'] = combined_chart['Pooja Ma\'am'] + combined_chart['Rohit Sir']
    combined_chart = combined_chart.reset_index()

    # Create labels in Lakhs
    combined_chart['Total_Label'] = combined_chart['Total'].apply(lambda x: f"₹{x/100000:.1f}L")

    # Melt data for stacked bar chart
    chart_data = combined_chart.melt(
        id_vars=['Month', 'Total', 'Total_Label'],
        value_vars=['Rohit Sir', 'Pooja Ma\'am'],
        var_name='Category',
        value_name='Amount'
    )
    chart_data['Label'] = chart_data['Amount'].apply(lambda x: f"₹{x/100000:.1f}L")

    # Create stacked bar chart with Altair
    bars = alt.Chart(chart_data).mark_bar().encode(
        x=alt.X('Month:N', sort=None, title='Month', axis=alt.Axis(labelAngle=0)),
        y=alt.Y('Amount:Q', title='Billed Amount', stack='zero'),
        color=alt.Color('Category:N', scale=alt.Scale(
            domain=['Pooja Ma\'am', 'Rohit Sir'],
            range=['#a855f7', '#06b6d4']
        ), legend=alt.Legend(title='Category', orient='top')),
        order=alt.Order('Category:N', sort='descending')
    ).properties(height=400)

    # Add value labels inside bars
    text = alt.Chart(chart_data).mark_text(
        align='center',
        baseline='middle',
        color='white',
        fontSize=12,
        fontWeight='bold'
    ).encode(
        x=alt.X('Month:N', sort=None),
        y=alt.Y('Amount:Q', stack='zero', bandPosition=0.5),
        text='Label:N',
        order=alt.Order('Category:N', sort='descending')
    )

    # Add total labels on top of stacked bars
    total_data = combined_chart[['Month', 'Total', 'Total_Label']].copy()
    total_text = alt.Chart(total_data).mark_text(
        align='center',
        baseline='bottom',
        dy=-5,
        color='#10b981',
        fontSize=14,
        fontWeight='bold'
    ).encode(
        x=alt.X('Month:N', sort=None),
        y=alt.Y('Total:Q'),
        text='Total_Label:N'
    )

    # Combine chart and labels
    final_chart = (bars + text + total_text).configure_axis(
        labelColor='#e2e8f0',
        titleColor='#e2e8f0',
        gridColor='#1e3a5f'
    ).configure_view(
        strokeWidth=0
    ).configure_legend(
        labelColor='#e2e8f0',
        titleColor='#e2e8f0'
    )

    st.altair_chart(final_chart, use_container_width=True)

    # Day-wise Billing Graph (based on sidebar month selection)
    st.markdown("<br>", unsafe_allow_html=True)
    if st.session_state.selected_month:
        selected_month_display = datetime.strptime(st.session_state.selected_month, "%Y-%m").strftime("%B %Y")
        st.markdown(f"<div class='section-header'>Day-wise Billing - {selected_month_display}</div>", unsafe_allow_html=True)

        # Filter data for selected month
        daywise_df = df[df['month'].astype(str) == st.session_state.selected_month].copy()
        daywise_df['bill_day'] = daywise_df['bill_date'].dt.day

        # For John Deere India Private Limited, use basic_freight + other_charges
        daywise_df['day_amount'] = daywise_df.apply(
            lambda row: row['basic_freight'] + (row['other_charges'] if pd.notna(row['other_charges']) else 0)
            if row['billing_party'] == 'John Deere India Private Limited'
            else row['basic_freight'],
            axis=1
        )

        # Split into Pooja and Rohit
        daywise_pooja = daywise_df[~daywise_df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]
        daywise_rohit = daywise_df[daywise_df['billing_party'].str.lower().str.contains('mahindra|john deere', na=False)]

        # Group by day for Pooja
        pooja_daily = daywise_pooja.groupby('bill_day').agg({'day_amount': 'sum'}).reset_index()
        pooja_daily.columns = ['Day', 'Pooja Ma\'am']

        # Group by day for Rohit (includes John Deere with other_charges)
        rohit_daily = daywise_rohit.groupby('bill_day').agg({'day_amount': 'sum'}).reset_index()
        rohit_daily.columns = ['Day', 'Rohit Sir']

        # Get the actual number of days in the month
        import calendar
        try:
            year, month_num = map(int, st.session_state.selected_month.split('-'))
            max_day = calendar.monthrange(year, month_num)[1]
        except:
            max_day = 31

        # Fill missing days with 0
        all_days = pd.DataFrame({'Day': range(1, max_day + 1)})
        daily_combined = all_days.merge(pooja_daily, on='Day', how='left').merge(rohit_daily, on='Day', how='left').fillna(0)
        daily_combined['Total'] = daily_combined['Pooja Ma\'am'] + daily_combined['Rohit Sir']

        # Filter out days with no billing
        daily_combined = daily_combined[daily_combined['Total'] > 0]

        daily_combined['Total_Label'] = daily_combined['Total'].apply(lambda x: f"₹{x/100000:.1f}L" if x > 0 else '')
        daily_combined['Day'] = daily_combined['Day'].astype(int).astype(str)

        # Melt data for stacked bar chart
        daily_chart_data = daily_combined.melt(
            id_vars=['Day', 'Total', 'Total_Label'],
            value_vars=['Rohit Sir', 'Pooja Ma\'am'],
            var_name='Category',
            value_name='Amount'
        )
        daily_chart_data['Label'] = daily_chart_data['Amount'].apply(lambda x: f"₹{x/100000:.1f}L" if x > 0 else '')

        # Create stacked bar chart with Altair
        day_bars = alt.Chart(daily_chart_data).mark_bar().encode(
            x=alt.X('Day:N', sort=None, title='Day', axis=alt.Axis(labelAngle=0)),
            y=alt.Y('Amount:Q', title='Billed Amount', stack='zero'),
            color=alt.Color('Category:N', scale=alt.Scale(
                domain=['Pooja Ma\'am', 'Rohit Sir'],
                range=['#a855f7', '#06b6d4']
            ), legend=alt.Legend(title='Category', orient='top')),
            order=alt.Order('Category:N', sort='descending')
        ).properties(height=350)

        # Add total labels on top of stacked bars
        total_data = daily_combined[['Day', 'Total', 'Total_Label']].copy()
        day_total_text = alt.Chart(total_data).mark_text(
            align='center',
            baseline='bottom',
            dy=-3,
            color='#10b981',
            fontSize=10,
            fontWeight='bold'
        ).encode(
            x=alt.X('Day:N', sort=None),
            y=alt.Y('Total:Q'),
            text='Total_Label:N'
        )

        # Combine chart and labels
        day_final_chart = (day_bars + day_total_text).configure_axis(
            labelColor='#e2e8f0',
            titleColor='#e2e8f0',
            gridColor='#1e3a5f'
        ).configure_view(
            strokeWidth=0
        ).configure_legend(
            labelColor='#e2e8f0',
            titleColor='#e2e8f0'
        )

        st.altair_chart(day_final_chart, use_container_width=True)

        # Show summary metrics for the selected month
        total_daywise = daily_combined['Total'].sum()
        total_pooja = daily_combined['Pooja Ma\'am'].sum()
        total_rohit = daily_combined['Rohit Sir'].sum()
        days_with_billing = len(daily_combined)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Billing", f"₹{total_daywise/100000:.2f}L")
        with col2:
            st.metric("Pooja Ma'am", f"₹{total_pooja/100000:.2f}L")
        with col3:
            st.metric("Rohit Sir", f"₹{total_rohit/100000:.2f}L")
        with col4:
            st.metric("Days with Billing", f"{days_with_billing}")

with tab4:
    st.markdown("<div class='section-header'>Pending POD - POD Not Received</div>", unsafe_allow_html=True)
    st.markdown("<p style='color: #64748b; font-size: 12px;'>CNs where Bill No is blank, POD Receipt No is blank, and ETA < D-4 - grouped by Party and Month</p>", unsafe_allow_html=True)

    # Get CNs with pending POD (POD not received) from FULL dataset
    # Filter: Bill No blank + POD Receipt No blank + ETA < D-4
    d_minus_4 = (datetime.now() - timedelta(days=4)).date()

    pending_pod_df = df[
        ((df['bill_no'].isna()) | (df['bill_no'] == '')) &
        ((df['pod_receipt_no'].isna()) | (df['pod_receipt_no'] == '')) &
        (df['eta'].notna()) & (pd.to_datetime(df['eta'], errors='coerce').dt.date < d_minus_4)
    ].copy()

    pending_pod_df['cn_month'] = pending_pod_df['cn_date'].dt.to_period('M')

    col1, col2 = st.columns([1, 4])

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total No. of CN</div>
            <div class="metric-value">{len(pending_pod_df):,}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">Total Qty</div>
            <div class="metric-value">{int(pending_pod_df['qty'].sum()):,}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # For John Deere India Private Limited, include other_charges
        jd_pending = pending_pod_df[pending_pod_df['billing_party'] == 'John Deere India Private Limited']
        other_pending = pending_pod_df[pending_pod_df['billing_party'] != 'John Deere India Private Limited']
        total_pending_amount = other_pending['basic_freight'].sum() + jd_pending['basic_freight'].sum() + jd_pending['other_charges'].fillna(0).sum()
        st.markdown(f"""
        <div class="metric-card metric-card-green">
            <div class="metric-title">Total Pending Amount</div>
            <div class="metric-value">{format_currency(total_pending_amount)}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        valid_months = pending_pod_df[pending_pod_df['cn_month'].notna()]['cn_month'].unique()
        months_sorted = sorted(valid_months, reverse=True)[:9]

        if len(pending_pod_df) > 0:
            # Define parent company groupings
            def get_parent_group_pending(party):
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

            # Build pivot data with group info
            pivot_data = []
            parties = pending_pod_df['billing_party'].dropna().unique()

            for party in parties:
                party_data = pending_pod_df[pending_pod_df['billing_party'] == party]
                row = {'Billing Party': party, 'Group': get_parent_group_pending(party)}

                for month in months_sorted:
                    month_data = party_data[party_data['cn_month'] == month]
                    month_str = str(month)
                    month_label = datetime.strptime(month_str, "%Y-%m").strftime("%b'%y")

                    row[f'{month_label}_CN'] = len(month_data)
                    row[f'{month_label}_Qty'] = int(month_data['qty'].sum())
                    # For John Deere India Private Limited, use basic_freight + other_charges
                    if party == 'John Deere India Private Limited':
                        row[f'{month_label}_Amt'] = month_data['basic_freight'].sum() + month_data['other_charges'].fillna(0).sum()
                    else:
                        row[f'{month_label}_Amt'] = month_data['basic_freight'].sum()

                # Calculate total amount for sorting
                total_amt = 0
                for m in months_sorted:
                    ml = datetime.strptime(str(m), "%Y-%m").strftime("%b'%y")
                    total_amt += row.get(f'{ml}_Amt', 0)
                row['Total_Amt'] = total_amt
                pivot_data.append(row)

            pivot_df = pd.DataFrame(pivot_data)

            # Get month labels for columns
            month_labels = [datetime.strptime(str(m), "%Y-%m").strftime("%b'%y") for m in months_sorted]

            # Build grouped rows with subtotals
            grouped_rows = []
            single_parties = []
            group_order = ['M & M', 'Toyota', 'Glovis', 'Tata', 'Honda', 'Skoda VW', 'John Deere', 'ValueDrive']

            for group in group_order:
                group_data = pivot_df[pivot_df['Group'] == group].sort_values('Total_Amt', ascending=False)
                if len(group_data) > 1:
                    for _, row in group_data.iterrows():
                        row_dict = {'Billing Party': row['Billing Party'], 'is_total': False}
                        for ml in month_labels:
                            row_dict[f'{ml}_CN'] = row.get(f'{ml}_CN', 0)
                            row_dict[f'{ml}_Qty'] = row.get(f'{ml}_Qty', 0)
                            row_dict[f'{ml}_Amt'] = row.get(f'{ml}_Amt', 0)
                        row_dict['_total_amt'] = row.get('Total_Amt', 0)
                        grouped_rows.append(row_dict)

                    subtotal_row = {'Billing Party': f'{group} - Total', 'is_total': True}
                    for ml in month_labels:
                        subtotal_row[f'{ml}_CN'] = int(group_data[f'{ml}_CN'].sum())
                        subtotal_row[f'{ml}_Qty'] = int(group_data[f'{ml}_Qty'].sum())
                        subtotal_row[f'{ml}_Amt'] = group_data[f'{ml}_Amt'].sum()
                    grouped_rows.append(subtotal_row)
                elif len(group_data) == 1:
                    row = group_data.iloc[0]
                    row_dict = {'Billing Party': row['Billing Party'], 'is_total': False}
                    for ml in month_labels:
                        row_dict[f'{ml}_CN'] = row.get(f'{ml}_CN', 0)
                        row_dict[f'{ml}_Qty'] = row.get(f'{ml}_Qty', 0)
                        row_dict[f'{ml}_Amt'] = row.get(f'{ml}_Amt', 0)
                    row_dict['_total_amt'] = row.get('Total_Amt', 0)
                    single_parties.append(row_dict)

            others_data = pivot_df[pivot_df['Group'] == 'Others'].sort_values('Total_Amt', ascending=False)
            for _, row in others_data.iterrows():
                row_dict = {'Billing Party': row['Billing Party'], 'is_total': False}
                for ml in month_labels:
                    row_dict[f'{ml}_CN'] = row.get(f'{ml}_CN', 0)
                    row_dict[f'{ml}_Qty'] = row.get(f'{ml}_Qty', 0)
                    row_dict[f'{ml}_Amt'] = row.get(f'{ml}_Amt', 0)
                row_dict['_total_amt'] = row.get('Total_Amt', 0)
                single_parties.append(row_dict)

            single_parties_sorted = sorted(single_parties, key=lambda x: x.get('_total_amt', 0), reverse=True)
            grouped_rows.extend(single_parties_sorted)

            # Calculate Grand Total
            grand_total = {'Billing Party': 'Grand Total', 'is_total': True, 'is_grand': True}
            for ml in month_labels:
                grand_total[f'{ml}_CN'] = int(pivot_df[f'{ml}_CN'].sum())
                grand_total[f'{ml}_Qty'] = int(pivot_df[f'{ml}_Qty'].sum())
                grand_total[f'{ml}_Amt'] = pivot_df[f'{ml}_Amt'].sum()

            # Build HTML table with bold borders and sticky headers
            html_parts = []
            html_parts.append("<div style='max-height: 500px; overflow-x: auto; overflow-y: auto; border-radius: 10px; border: 2px solid #3b82f6;'>")
            html_parts.append("<table style='width:100%; border-collapse: collapse; color: white; font-size: 12px; min-width: 1200px;'>")
            html_parts.append("<thead>")
            # First header row - month names
            html_parts.append("<tr style='background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%); position: sticky; top: 0; z-index: 3;'>")
            html_parts.append("<th style='padding: 10px; text-align: left; font-weight: 600; border: 2px solid #3b82f6; min-width: 250px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>Billing Party</th>")

            # Add month column headers
            for ml in month_labels:
                html_parts.append(f"<th style='padding: 8px 4px; text-align: center; font-weight: 600; border: 2px solid #3b82f6; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);' colspan='3'>{ml}</th>")

            html_parts.append("</tr>")
            # Second header row - sub-columns
            html_parts.append("<tr style='background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%); position: sticky; top: 35px; z-index: 3;'>")
            html_parts.append("<th style='padding: 6px; border: 2px solid #3b82f6; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'></th>")

            for ml in month_labels:
                html_parts.append("<th style='padding: 6px 4px; text-align: right; font-weight: 500; border-bottom: 2px solid #3b82f6; border-left: 2px solid #3b82f6; font-size: 10px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>No. of CN</th>")
                html_parts.append("<th style='padding: 6px 4px; text-align: right; font-weight: 500; border-bottom: 2px solid #3b82f6; font-size: 10px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>Qty</th>")
                html_parts.append("<th style='padding: 6px 4px; text-align: right; font-weight: 500; border-bottom: 2px solid #3b82f6; border-right: 2px solid #3b82f6; font-size: 10px; background: linear-gradient(135deg, #1e3a5f 0%, #2d4a6f 100%);'>Pending Amt</th>")

            html_parts.append("</tr>")
            # Grand Total row - sticky
            html_parts.append("<tr style='background: linear-gradient(135deg, #065f46 0%, #047857 100%); color: white; font-weight: bold; position: sticky; top: 62px; z-index: 2;'>")
            html_parts.append(f"<th style='padding: 10px; text-align: left; border: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{grand_total['Billing Party']}</th>")
            for ml in month_labels:
                cn_val = grand_total[f'{ml}_CN']
                qty_val = grand_total[f'{ml}_Qty']
                amt_val = grand_total[f'{ml}_Amt']
                cn_display = cn_val if cn_val > 0 else '-'
                qty_display = qty_val if qty_val > 0 else '-'
                amt_display = f"₹{amt_val:,.0f}" if amt_val > 0 else '-'
                html_parts.append(f"<th style='padding: 8px 4px; text-align: right; border-bottom: 2px solid #10b981; border-left: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{cn_display}</th>")
                html_parts.append(f"<th style='padding: 8px 4px; text-align: right; border-bottom: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{qty_display}</th>")
                html_parts.append(f"<th style='padding: 8px 4px; text-align: right; border-bottom: 2px solid #10b981; border-right: 2px solid #10b981; background: linear-gradient(135deg, #065f46 0%, #047857 100%);'>{amt_display}</th>")
            html_parts.append("</tr>")
            html_parts.append("</thead><tbody>")

            # Add data rows
            row_idx = 0
            for row in grouped_rows:
                if row.get('is_total'):
                    style = "background: linear-gradient(135deg, #b8860b 0%, #d4a017 100%); color: #000000; font-weight: bold;"
                    border_color = "#b8860b"
                else:
                    if row_idx % 2 == 0:
                        style = "background-color: #162544;"
                    else:
                        style = "background-color: #1a2d4d;"
                    row_idx += 1
                    border_color = "#3b82f6"

                html_parts.append(f"<tr style='{style}'>")
                html_parts.append(f"<td style='padding: 8px 10px; border: 1px solid {border_color}; border-left: 2px solid #3b82f6; border-right: 2px solid #3b82f6;'>{row['Billing Party']}</td>")

                for ml in month_labels:
                    cn_val = row.get(f'{ml}_CN', 0)
                    qty_val = row.get(f'{ml}_Qty', 0)
                    amt_val = row.get(f'{ml}_Amt', 0)
                    cn_display = int(cn_val) if cn_val > 0 else '-'
                    qty_display = int(qty_val) if qty_val > 0 else '-'
                    amt_display = f"₹{amt_val:,.0f}" if amt_val > 0 else '-'
                    html_parts.append(f"<td style='padding: 6px 4px; text-align: right; border-bottom: 1px solid {border_color}; border-left: 2px solid #3b82f6;'>{cn_display}</td>")
                    html_parts.append(f"<td style='padding: 6px 4px; text-align: right; border-bottom: 1px solid {border_color};'>{qty_display}</td>")
                    html_parts.append(f"<td style='padding: 6px 4px; text-align: right; border-bottom: 1px solid {border_color}; border-right: 2px solid #3b82f6;'>{amt_display}</td>")

                html_parts.append("</tr>")

            html_parts.append("</tbody></table></div>")
            html_table = "".join(html_parts)

            st.markdown(html_table, unsafe_allow_html=True)

            # Download button
            @st.cache_data
            def convert_pending_pod_to_csv(dataframe):
                return dataframe.to_csv(index=False).encode('utf-8')

            download_df = pending_pod_df[['cn_no', 'cn_date', 'branch', 'billing_party', 'origin', 'consignee', 'destination', 'vehicle_no', 'qty', 'basic_freight']].copy()
            download_df.columns = ['CN No', 'CN Date', 'Branch', 'Billing Party', 'Origin', 'Consignee', 'Destination', 'Vehicle No', 'Qty', 'Basic Freight']
            download_df['CN Date'] = download_df['CN Date'].dt.strftime('%d-%m-%Y')
            download_df = download_df.sort_values(['Billing Party', 'CN Date'], ascending=[True, False])

            csv = convert_pending_pod_to_csv(download_df)
            st.download_button(
                label="📥 Download Pending POD Data",
                data=csv,
                file_name=f"pending_pod_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No CNs with pending POD found.")

# Footer
st.markdown("---")
db_api_time = st.session_state.db_api_update.strftime('%d-%m-%Y %H:%M:%S') if st.session_state.db_api_update else "N/A"
st.markdown(
    f"<p style='text-align: center; color: #64748b;'>Swift Billing Dashboard | DB Last Updated from API: {db_api_time} | Auto-refresh: Checks every 5 min</p>",
    unsafe_allow_html=True
)
