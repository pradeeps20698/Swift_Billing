import streamlit as st
import pandas as pd
import psycopg2
import os
from datetime import datetime
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

# Auto-refresh every 1 hour (3600000 milliseconds)
refresh_count = st_autorefresh(interval=3600000, limit=None, key="billing_auto_refresh")

# Page config
st.set_page_config(
    page_title="Swift Billing Dashboard",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

# Auto-refresh every 1 hour: Clear session state cache and reload from database
if refresh_count > st.session_state.refresh_count:
    st.session_state.refresh_count = refresh_count
    st.session_state.df = None  # Clear cached data
    st.cache_resource.clear()  # Clear DB connection cache

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
            route, origin, destination, km, vehicle_no, vehicle_type,
            charge_weight, actual_weight, qty, basic_freight, detention,
            hamali, other_charges, penalty, total_freight, payment_received,
            deduction_lr, balance, consignee, material_detail, pod_status,
            pod_receipt_no
        FROM cn_data
        ORDER BY bill_date DESC NULLS LAST
    """
    df = pd.read_sql(query, conn)
    df['cn_date'] = pd.to_datetime(df['cn_date'])
    df['bill_date'] = pd.to_datetime(df['bill_date'])
    df['month'] = df['bill_date'].dt.to_period('M')
    df['is_own'] = df['vehicle_type'].str.lower().str.contains('own', na=False)
    return df

# Get last API update time from database
def get_db_last_update():
    conn = get_connection()
    query = "SELECT MAX(updated_at) FROM cn_data"
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
    st.session_state.df = load_data()
    st.session_state.last_db_refresh = datetime.now()
    st.session_state.db_api_update = get_db_last_update()
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

# Branch filter
st.sidebar.markdown("### 📍 Branch")
branch_index = branches.index(st.session_state.selected_branch) if st.session_state.selected_branch in branches else 0
st.sidebar.selectbox(
    "Select Branch",
    branches,
    index=branch_index,
    key="branch_select",
    on_change=on_branch_change,
    label_visibility="collapsed"
)

# Search box
st.sidebar.markdown("### 🔍 Search")
st.sidebar.text_input(
    "Search CN/Vehicle/Party",
    value=st.session_state.search_query,
    key="search_input",
    on_change=on_search_change,
    placeholder="Type to search..."
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
if st.session_state.selected_branch != "All":
    active_filters.append(f"Branch: {st.session_state.selected_branch}")
if st.session_state.search_query:
    active_filters.append(f"Search: {st.session_state.search_query}")

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

# Main content
st.markdown("<h1 style='text-align: center; color: white;'>🧾 Swift Billing Dashboard</h1>", unsafe_allow_html=True)

# Month display
if st.session_state.selected_month:
    month_display = datetime.strptime(st.session_state.selected_month, "%Y-%m").strftime("%B %Y")
    st.markdown(f"<h3 style='color: #94a3b8;'>Billing Summary ({month_display})</h3>", unsafe_allow_html=True)

# Calculate metrics
total_cns = len(filtered_df)
own_cns = len(filtered_df[filtered_df['is_own'] == True])
hire_cns = len(filtered_df[filtered_df['is_own'] == False])

total_freight = filtered_df['basic_freight'].sum()
own_freight = filtered_df[filtered_df['is_own'] == True]['basic_freight'].sum()
hire_freight = filtered_df[filtered_df['is_own'] == False]['basic_freight'].sum()

total_qty = filtered_df['qty'].sum()
own_qty = filtered_df[filtered_df['is_own'] == True]['qty'].sum()
hire_qty = filtered_df[filtered_df['is_own'] == False]['qty'].sum()

# Metric cards row
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Total CNs</div>
        <div class="metric-value">{total_cns:,}</div>
        <div class="metric-breakdown">
            <div class="breakdown-item">
                <div class="breakdown-label">Own</div>
                <div class="breakdown-value">{own_cns:,}</div>
            </div>
            <div class="breakdown-item">
                <div class="breakdown-label">Hire</div>
                <div class="breakdown-value breakdown-value-orange">{hire_cns:,}</div>
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
        <div class="metric-title">Basic Freight</div>
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
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Client-Wise Summary",
    "🏢 Branch-Wise Summary",
    "📋 CN Details",
    "💰 Unbilled CNs",
    "📈 Monthly Trend"
])

with tab1:
    st.markdown("<div class='section-header'>Client-Wise Billing Summary</div>", unsafe_allow_html=True)

    if len(filtered_df) > 0:
        client_summary = filtered_df.groupby('billing_party').agg({
            'cn_no': 'count',
            'qty': 'sum',
            'basic_freight': 'sum'
        }).reset_index()

        client_summary.columns = ['Billing Party', 'CN Count', 'Units', 'Basic Freight']
        client_summary = client_summary.sort_values('Basic Freight', ascending=False)

        client_display = client_summary.copy()
        client_display['Basic Freight'] = client_display['Basic Freight'].apply(lambda x: f"₹{x:,.0f}")
        client_display['Units'] = client_display['Units'].astype(int)

        st.dataframe(
            client_display,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("No data found for the selected filters.")

with tab2:
    st.markdown("<div class='section-header'>Branch-Wise Billing Summary</div>", unsafe_allow_html=True)

    if len(filtered_df) > 0:
        branch_summary = filtered_df.groupby('branch').agg({
            'cn_no': 'count',
            'qty': 'sum',
            'basic_freight': 'sum'
        }).reset_index()

        branch_summary.columns = ['Branch', 'CN Count', 'Units', 'Basic Freight']
        branch_summary = branch_summary.sort_values('Basic Freight', ascending=False)

        branch_display = branch_summary.copy()
        branch_display['Basic Freight'] = branch_display['Basic Freight'].apply(lambda x: f"₹{x:,.0f}")
        branch_display['Units'] = branch_display['Units'].astype(int)

        st.dataframe(
            branch_display,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("No data found for the selected filters.")

with tab3:
    st.markdown("<div class='section-header'>CN Details</div>", unsafe_allow_html=True)

    if len(filtered_df) > 0:
        cn_details = filtered_df[['cn_no', 'cn_date', 'branch', 'billing_party', 'origin',
                                   'destination', 'vehicle_no', 'vehicle_type', 'qty',
                                   'basic_freight', 'pod_status']].copy()

        cn_details['cn_date'] = cn_details['cn_date'].dt.strftime('%d-%m-%Y')
        cn_details.columns = ['CN No', 'Date', 'Branch', 'Billing Party', 'Origin',
                              'Destination', 'Vehicle', 'Type', 'Qty',
                              'Freight', 'POD Status']

        st.dataframe(
            cn_details,
            use_container_width=True,
            hide_index=True,
            height=500
        )

        st.markdown(f"**Showing {len(cn_details):,} records**")
    else:
        st.info("No data found for the selected filters.")

with tab4:
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

        st.markdown(f"""
        <div class="metric-card metric-card-green">
            <div class="metric-title">Total Unbilled Amount</div>
            <div class="metric-value">{format_currency(unbilled_df['basic_freight'].sum())}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        valid_months = unbilled_df[unbilled_df['cn_month'].notna()]['cn_month'].unique()
        months_sorted = sorted(valid_months, reverse=True)[:9]

        if len(unbilled_df) > 0:
            pivot_data = []
            parties = unbilled_df['billing_party'].dropna().unique()

            for party in sorted(parties):
                party_data = unbilled_df[unbilled_df['billing_party'] == party]
                row = {'Billing Party': party}

                for month in months_sorted:
                    month_data = party_data[party_data['cn_month'] == month]
                    month_str = str(month)
                    month_label = datetime.strptime(month_str, "%Y-%m").strftime("%b'%y")

                    row[f'{month_label}_CN'] = len(month_data)
                    row[f'{month_label}_Qty'] = int(month_data['qty'].sum())
                    row[f'{month_label}_Amt'] = month_data['basic_freight'].sum()

                pivot_data.append(row)

            pivot_df = pd.DataFrame(pivot_data)

            pivot_df['Total_Amt'] = pivot_df[[col for col in pivot_df.columns if col.endswith('_Amt')]].sum(axis=1)
            pivot_df = pivot_df.sort_values('Total_Amt', ascending=False)
            pivot_df = pivot_df.drop('Total_Amt', axis=1)

            for col in pivot_df.columns:
                if col.endswith('_Amt'):
                    pivot_df[col] = pivot_df[col].apply(lambda x: f"₹{x:,.0f}" if x > 0 else "-")
                elif col.endswith('_CN') or col.endswith('_Qty'):
                    pivot_df[col] = pivot_df[col].apply(lambda x: str(int(x)) if x > 0 else "-")

            new_columns = ['Billing Party']
            for month in months_sorted:
                month_str = str(month)
                month_label = datetime.strptime(month_str, "%Y-%m").strftime("%b'%y")
                new_columns.extend([f'{month_label} CN', f'{month_label} Qty', f'{month_label} Amt'])

            pivot_df.columns = new_columns

            st.dataframe(
                pivot_df,
                use_container_width=True,
                hide_index=True,
                height=450
            )

            @st.cache_data
            def convert_df_to_csv(dataframe):
                return dataframe.to_csv(index=False).encode('utf-8')

            csv = convert_df_to_csv(pivot_df)
            st.download_button(
                label="📥 Download Unbilled CN Data",
                data=csv,
                file_name=f"unbilled_cn_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No unbilled CNs with POD received found.")

with tab5:
    st.markdown("<div class='section-header'>Monthly Billing Trend</div>", unsafe_allow_html=True)

    monthly_trend = df.groupby('month').agg({
        'cn_no': 'count',
        'qty': 'sum',
        'basic_freight': 'sum'
    }).reset_index()

    monthly_trend['month'] = monthly_trend['month'].astype(str)
    monthly_trend.columns = ['Month', 'CNs', 'Units', 'Freight']

    chart_data = monthly_trend.set_index('Month')[['Freight']].tail(12)
    st.bar_chart(chart_data)

    monthly_display = monthly_trend.tail(12).copy()
    monthly_display['Freight'] = monthly_display['Freight'].apply(lambda x: f"₹{x:,.0f}")

    st.dataframe(
        monthly_display,
        use_container_width=True,
        hide_index=True
    )

# Footer
st.markdown("---")
db_api_time = st.session_state.db_api_update.strftime('%d-%m-%Y %H:%M:%S') if st.session_state.db_api_update else "N/A"
st.markdown(
    f"<p style='text-align: center; color: #64748b;'>Swift Billing Dashboard | DB Last Updated from API: {db_api_time} | Auto-refresh: Every 1 hour</p>",
    unsafe_allow_html=True
)
