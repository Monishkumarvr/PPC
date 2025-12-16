import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import optimization_engine
import grinding_optimization_engine
import io
import concurrent.futures
import time
import tempfile
import os

# Set page configuration
st.set_page_config(
    page_title="Foundry Production Planner",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for "Magazine Style"
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700&family=Lato:wght@300;400;700&display=swap');

    h1, h2, h3 {
        font-family: 'Playfair Display', serif;
        color: #2c3e50;
    }
    
    div[class*="stMarkdown"] p, div[class*="stText"] {
        font-family: 'Lato', sans-serif;
        color: #4a4a4a;
        font-size: 1.1rem;
    }

    .stButton>button {
        background-color: #2c3e50;
        color: white;
        border-radius: 0px;
        padding: 0.5rem 2rem;
        font-family: 'Lato', sans-serif;
        text-transform: uppercase;
        letter-spacing: 1px;
        border: none;
        transition: all 0.3s ease;
    }

    .stButton>button:hover {
        background-color: #34495e;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Clean Cards for Metrics */
    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 1rem;
        border-left: 5px solid #2c3e50;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        color: #2c3e50;
    }
    
    div[data-testid="stMetric"] label {
        color: #4a4a4a !important;
    }
    
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #2c3e50 !important;
    }

    /* Remove default padding */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    .sidebar .sidebar-content {
        background-color: #f0f2f6;
    }
    
    /* Table Styling */
    .dataframe {
        font-family: 'Lato', sans-serif;
        border-collapse: collapse;
        width: 100%;
    }
    .dataframe td, .dataframe th {
        border: 1px solid #ddd;
        padding: 8px;
    }
    .dataframe tr:nth-child(even){background-color: #f2f2f2;}
    .dataframe th {
        padding-top: 12px;
        padding-bottom: 12px;
        text-align: left;
        background-color: #2c3e50;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.title("🏭 Foundry Production Optimization")
st.markdown("### Intelligent scheduling for metal casting foundries")
st.markdown("---")

# --- Authentication Check ---
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
    st.session_state['username'] = None

if 'page' not in st.session_state:
    st.session_state['page'] = 'Planner'
    
# Initialize planner mode explicitly if needed, but the radio button manages it
if 'planner_mode' not in st.session_state:
    st.session_state['planner_mode'] = 'Casting'

# --- Login Logic ---
if not st.session_state['logged_in']:
    # Initialize DB (creates table if not exists)
    import database
    database.init_db()
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.subheader("🔐 Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        if st.button("Login", type="primary", use_container_width=True):
            if database.verify_user(username, password):
                st.session_state['logged_in'] = True
                st.session_state['username'] = username
                st.rerun()
            else:
                st.error("Invalid username or password")
                
    st.stop() # Stop execution here if not logged in

# --- Sidebar Navigation (Only visible if logged in) ---
with st.sidebar:
    st.subheader(f"👤 User: {st.session_state['username']}")
    page_selection = st.radio("Navigation", ["Planner", "History"], index=0 if st.session_state['page'] == 'Planner' else 1)
    st.session_state['page'] = page_selection
    
    if st.button("Logout"):
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.rerun()
    
    st.markdown("---")

# --- Page: History ---
if st.session_state['page'] == "History":
    import database
    st.header("📜 Run History")
    
    history_df = database.get_history(st.session_state['username'])
    if not history_df.empty:
        st.dataframe(history_df, use_container_width=True, hide_index=True)
    else:
        st.info("No history found for this user.")
        
    st.stop()

# --- Page: Planner ---

# Sidebar for Controls
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Planner Mode Selection
    # Ensure index is valid
    mode_options = ["Casting", "Grinding"]
    current_mode = st.session_state.get('planner_mode', 'Casting')
    try:
        idx = mode_options.index(current_mode)
    except ValueError:
        idx = 0
        
    planner_mode = st.radio("Optimization Mode", mode_options, index=idx)
    st.session_state['planner_mode'] = planner_mode
    
    st.subheader("1. Input Data")
    uploaded_file = st.file_uploader("Upload Master Data (Excel)", type=["xlsx"])
    
    file_valid = False
    if uploaded_file:
        # Load data into session state if new file uploaded
        if 'last_uploaded_file' not in st.session_state or st.session_state['last_uploaded_file'] != uploaded_file.name:
            try:
                dfs = pd.read_excel(uploaded_file, sheet_name=None)
                st.session_state['master_data'] = dfs
                st.session_state['last_uploaded_file'] = uploaded_file.name
            except Exception as e:
                st.error(f"Error reading file: {e}")

        # Validate from session state
        if 'master_data' in st.session_state:
            sheet_status = optimization_engine.validate_excel_sheets(st.session_state['master_data'])
            
            st.markdown("**Data Validation:**")
            all_required_present = True
            for sheet, present in sheet_status.items():
                if sheet == "Stage WIP": continue # Optional
                icon = "✅" if present else "❌"
                if not present: all_required_present = False
                st.markdown(f"{icon} {sheet}")
            
            # Check optional
            if sheet_status.get("Stage WIP"):
                st.markdown(f"✅ Stage WIP (Optional)")
            else:
                st.markdown(f"ℹ️ Stage WIP (Not Found)")
                
            file_valid = all_required_present
            if not file_valid:
                st.error("Missing required sheets! Please check your file.")

    st.subheader("2. Planning Parameters")
    planning_date = st.date_input("Planning Start Date", date.today())
    planning_end_date = st.date_input("Planning End Date (Optional)", value=None, help="If left empty, the system will auto-detect the horizon (max 90 days) to optimize performance.")
    
    if planning_end_date is None:
        st.info("ℹ️ Auto-Horizon: System will plan up to 90 days from start date.")
    
    if planner_mode == "Casting":
        daily_melt_tons = st.number_input("Daily Melt Capacity (Tons)", value=250.0, step=10.0)
        line_hours = st.number_input("Line Hours per Day", value=16.0, step=1.0)
        line_oee = st.slider("Line OEE (Efficiency)", 0.5, 1.0, 0.90)
    else:
        # Grinding Params
        grinding_resources = st.number_input("Grinding Resources", value=35, step=1)
        grinding_hours = st.number_input("Hours per Day", value=8.0, step=0.5)
        grinding_oee = st.slider("Grinding OEE", 0.5, 1.0, 0.90)
    
    st.subheader("3. Optimization Constraints")
    
    with st.expander("Advanced Penalties"):
        shortage_penalty = st.number_input("Shortage Penalty", value=100000000.0)
        leadtime_days = st.number_input("Required Lead Time (Days)", value=14)
        lateness_penalty = st.number_input("Lateness Penalty / Day", value=50.0)
        early_penalty = st.number_input("Early Production Penalty", value=0.0)
        leadtime_pen_val = st.number_input("Lead Time Penalty / Day", value=25.0)
        solver_timeout = st.number_input("Solver Timeout (seconds)", value=600)
    
    # Run Button
    run_btn = st.button(f"Run {planner_mode} Optimization", type="primary", disabled=not file_valid)

# --- Casting Planner View ---
if planner_mode == "Casting":
    if uploaded_file is None:
        st.info("👋 Welcome! Please upload your **Master Data Excel file** in the sidebar to begin optimization.")
        with st.expander("See expected data format"):
            st.markdown("""
            The Excel file should contain the following sheets:
            - **Part Master**: FG Code, Box Quantity, Weights, Cycle Times
            - **Sales Order**: Orders with committed delivery dates
            - **Machine Constraints**: Capacity of Small/Big vacuum lines
            - **Mould Box Capacity**: Available boxes per size
            - **Stage WIP**: Current Work-In-Progress inventory
            """)
    else:
        # Data Editor Section
        if file_valid and 'master_data' in st.session_state:
            st.subheader("📝 Master Data Editor")
            with st.expander("View & Edit Uploaded Data", expanded=True):
                sheet_status = optimization_engine.validate_excel_sheets(st.session_state['master_data'])
                valid_sheets = [s for s in sheet_status.keys() if sheet_status[s]]
                
                if valid_sheets:
                    data_tabs = st.tabs(valid_sheets)
                    for i, sheet_name in enumerate(valid_sheets):
                        with data_tabs[i]:
                            current_df = st.session_state['master_data'].get(sheet_name)
                            if current_df is not None and not current_df.empty:
                                edited_df = st.data_editor(
                                    current_df,
                                    num_rows="dynamic",
                                    key=f"editor_main_{sheet_name}",
                                    use_container_width=True
                                )
                                st.session_state['master_data'][sheet_name] = edited_df

        if run_btn and file_valid:
            try:
                # 1. Setup Config
                config = optimization_engine.OptimizationConfig(
                    daily_melt_tons=daily_melt_tons,
                    line_hours_per_day=line_hours,
                    line_oee=line_oee,
                    shortage_penalty=shortage_penalty,
                    leadtime_required_days=int(leadtime_days),
                    lateness_penalty_per_day=lateness_penalty,
                    early_production_penalty=early_penalty,
                    leadtime_penalty_per_day=leadtime_pen_val,
                    planning_date=pd.to_datetime(planning_date),
                    planning_end_date=pd.to_datetime(planning_end_date) if planning_end_date else None,
                    solver_timeout=int(solver_timeout)
                )
                
                # 2. Load Data
                with st.spinner("Processing Data..."):
                    input_data = st.session_state['master_data']
                    (
                        products, days, demand_boxes, bunch_weight_kg, box_qty,
                        line_time_min, cycle_days, line, box_size_of, box_max_boxes,
                        max_melt_kg_per_day, max_time_small_min, max_time_big_min,
                        order_list, wip_coverage_boxes, gross_demand_boxes
                    ) = optimization_engine.process_casting_data(input_data, config)

                # 3. Solve
                status_container = st.status("🚀 Running Optimization...", expanded=True)
                log_container = status_container.empty()
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp_log:
                    log_path = tmp_log.name
                
                def run_optimization():
                    try:
                        return optimization_engine.build_and_solve_enhanced_milp(
                            products, days, demand_boxes, bunch_weight_kg, box_qty,
                            line_time_min, cycle_days, line, box_size_of, box_max_boxes,
                            max_melt_kg_per_day, max_time_small_min, max_time_big_min,
                            order_list, wip_coverage_boxes, gross_demand_boxes, config,
                            log_path=log_path
                        )
                    except Exception as e:
                        return e

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_optimization)
                    while not future.done():
                        time.sleep(0.5)
                        try:
                            with open(log_path, "r") as f:
                                lines = f.readlines()
                                if lines:
                                    log_container.code("".join(lines[-15:]), language="text")
                        except Exception: pass
                    
                    result = future.result()
                    
                try: os.unlink(log_path)
                except: pass
                
                if isinstance(result, Exception):
                    status_container.update(label="❌ Optimization Failed", state="error", expanded=False)
                    st.error(f"Optimization Engine Error: {result}")
                else:
                    status_container.update(label="✅ Optimization Complete!", state="complete", expanded=False)

                    prob, schedule_rows, order_shortage_rows, daily_capacity_rows, box_utilization_rows = result

                    if not schedule_rows and not order_shortage_rows:
                            st.warning("No schedule returned.")
                    else:
                        st.session_state['results'] = {
                            'schedule_rows': schedule_rows,
                            'order_shortage_rows': order_shortage_rows,
                            'daily_capacity_rows': daily_capacity_rows,
                            'box_utilization_rows': box_utilization_rows
                        }
                        # Also save as a dataframe for Grinding to use
                        st.session_state['casting_schedule_df'] = pd.DataFrame(schedule_rows)
                        
                        # Log to History
                        try:
                            import database
                            total_melt = sum(r['Melt_Used_kg'] for r in daily_capacity_rows) / 1000.0
                            total_orders = len(order_shortage_rows)
                            on_time = len([r for r in order_shortage_rows if r['Status'] == 'ON TIME'])
                            fulfill_pct = (on_time / total_orders * 100) if total_orders > 0 else 0
                            database.log_run(st.session_state['username'], daily_melt_tons, total_orders, fulfill_pct, total_melt)
                        except Exception: pass

            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)

    # Display Casting Results
    if 'results' in st.session_state and planner_mode == "Casting":
        results = st.session_state['results']
        schedule_df = pd.DataFrame(results['schedule_rows'])
        shortage_df = pd.DataFrame(results['order_shortage_rows'])
        capacity_df = pd.DataFrame(results['daily_capacity_rows'])
        box_df = pd.DataFrame(results['box_utilization_rows'])
        
        # --- KPI Summary ---
        st.header("📊 Executive Summary")
        c1, c2, c3, c4 = st.columns(4)
        
        total_orders = len(shortage_df)
        on_time = len(shortage_df[shortage_df['Status'] == 'ON TIME'])
        on_time_pct = (on_time / total_orders * 100) if total_orders > 0 else 0
        total_melt = capacity_df['Melt_Used_kg'].sum() / 1000.0
        avg_melt_util = capacity_df['Melt_Utilization_%'].mean()
        
        c1.metric("Order Fulfillment", f"{on_time_pct:.1f}%", f"{on_time}/{total_orders} Orders")
        c2.metric("Total Melt Scheduled", f"{total_melt:,.1f} Tons")
        c3.metric("Avg Melt Utilization", f"{avg_melt_util:.1f}%")
        c4.metric("Total Boxes Moulded", f"{schedule_df['Boxes'].sum():,.0f}")
        
        st.markdown("---")
        
        # Tabs
        tab_schedule, tab_fulfill, tab_cap, tab_box, tab_ai, tab_input = st.tabs([
            "🗓️ Production Schedule", "🚚 Order Fulfillment", "📈 Capacity Analysis",
            "📦 Daily Box Status", "🤖 AI Assistant", "📂 Input Data"
        ])
        
        with tab_schedule:
            st.subheader("Daily Production Plan")
            st.dataframe(schedule_df, use_container_width=True, hide_index=True)
            with st.expander("📊 Custom Pivot Analysis"):
                if not schedule_df.empty:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: pivot_index = st.multiselect("Rows", schedule_df.columns, default=["Date"])
                    with c2: pivot_columns = st.multiselect("Columns", schedule_df.columns, default=["FG Code"])
                    with c3: pivot_values = st.selectbox("Values", schedule_df.select_dtypes(include=['number']).columns)
                    with c4: pivot_agg = st.selectbox("Aggregation", ["sum", "mean", "count"])
                    if pivot_index and pivot_values:
                        st.dataframe(schedule_df.pivot_table(index=pivot_index, columns=pivot_columns, values=pivot_values, aggfunc=pivot_agg, fill_value=0), use_container_width=True)

        with tab_fulfill:
            c_pie, c_table = st.columns([1, 2])
            with c_pie:
                fig = px.pie(shortage_df, names='Status', title="Status Distribution", color='Status', color_discrete_map={'ON TIME':'#2ecc71', 'LATE':'#e74c3c', 'SHORT':'#f1c40f'})
                st.plotly_chart(fig, use_container_width=True)
            with c_table:
                st.dataframe(shortage_df, hide_index=True, use_container_width=True)

        with tab_cap:
            fig_melt = px.bar(capacity_df, x='Date', y='Melt_Utilization_%', title="Melt Capacity Utilization", range_y=[0, 110])
            st.plotly_chart(fig_melt, use_container_width=True)

        with tab_box:
            st.dataframe(box_df, use_container_width=True, hide_index=True)

        with tab_ai:
            if st.button("Generate Recommendations", type="primary"):
                recs = optimization_engine.generate_recommendations(results['daily_capacity_rows'], results['box_utilization_rows'], results['order_shortage_rows'])
                for r in recs: st.warning(f"{r['type']}: {r['message']}")

        with tab_input:
            if 'master_data' in st.session_state:
                for k,v in st.session_state['master_data'].items():
                    with st.expander(k): st.dataframe(v)

        # Download
        st.markdown("---")
        st.subheader("💾 Export Results")
        buffer = optimization_engine.generate_excel_output(results['schedule_rows'], results['order_shortage_rows'], results['daily_capacity_rows'], results['box_utilization_rows'])
        st.download_button("Download Casting Schedule", buffer, f"Casting_Schedule_{datetime.now().strftime('%Y%m%d')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")


# --- Grinding Planner View ---
if planner_mode == "Grinding":
    
    st.info("💡 Grinding optimization requires both **Master Data** and a **Casting Schedule**.")
    
    # Check for casting schedule
    casting_schedule_df = None
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Source of Casting Schedule")
        cs_source = st.radio("Select Source", ["Use Current Session Result", "Upload Excel File"])
    
    if cs_source == "Use Current Session Result":
        if 'casting_schedule_df' in st.session_state:
            casting_schedule_df = st.session_state['casting_schedule_df']
            st.success(f"Loaded {len(casting_schedule_df)} rows from current session.")
        else:
            st.warning("No casting schedule found in current session. Please run Casting Optimization first or upload a file.")
    else:
        with c2:
            cs_file = st.file_uploader("Upload Casting Schedule (Excel)", type=["xlsx"])
            if cs_file:
                try:
                    casting_schedule_df = pd.read_excel(cs_file, sheet_name="Production_Schedule") # Assuming standard output format
                    st.success(f"Loaded {len(casting_schedule_df)} rows from file.")
                except Exception as e:
                    st.error(f"Error reading file: {e}")
    
    if run_btn:
        if not file_valid:
            st.error("Please upload Master Data first.")
        elif casting_schedule_df is None or casting_schedule_df.empty:
            st.error("Please provide a valid Casting Schedule.")
        else:
            try:
                # 1. Config
                grinding_config = grinding_optimization_engine.GrindingConfig(
                    master_data=st.session_state['master_data'],
                    grinding_resources=int(grinding_resources),
                    hours_per_day=float(grinding_hours),
                    line_oee=float(grinding_oee),
                    shortage_penalty=shortage_penalty,
                    leadtime_required_days=int(leadtime_days),
                    lateness_penalty=lateness_penalty,
                    production_lateness_penalty=1000.0, # Fixed or expose?
                    leadtime_penalty=leadtime_pen_val,
                    early_penalty=early_penalty,
                    solver_timeout=int(solver_timeout)
                )
                
                # 2. Determine Dates
                start_d = pd.to_datetime(planning_date)
                end_d = pd.to_datetime(planning_end_date) if planning_end_date else start_d + pd.Timedelta(days=90)
                
                # 3. Process & Optimize
                status_container = st.status("🚀 Running Grinding Optimization...", expanded=True)
                log_container = status_container.empty()
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp_log:
                    log_path = tmp_log.name
                
                def run_grinding():
                    try:
                        return grinding_optimization_engine.process_data_and_optimize(
                            st.session_state['master_data'],
                            casting_schedule_df,
                            start_d,
                            end_d,
                            grinding_config,
                            log_path=log_path
                        )
                    except Exception as e:
                        return e
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_grinding)
                    while not future.done():
                        time.sleep(0.5)
                        try:
                            with open(log_path, "r") as f:
                                lines = f.readlines()
                                if lines:
                                    log_container.code("".join(lines[-15:]), language="text")
                        except: pass
                    result = future.result()
                    
                try: os.unlink(log_path)
                except: pass
                
                if isinstance(result, Exception):
                    status_container.update(label="❌ Optimization Failed", state="error", expanded=False)
                    st.error(f"Error: {result}")
                elif result is None:
                    status_container.update(label="❌ No Solution Found", state="error", expanded=False)
                    st.warning("Solver could not find an optimal solution.")
                else:
                    status_container.update(label="✅ Grinding Optimization Complete!", state="complete", expanded=False)
                    st.session_state['grinding_results'] = result
                    
                    # Log to History
                    try:
                        import database
                        # Simple metrics for history
                        total_units = result['schedule']['Units'].sum()
                        total_orders = len(result['fulfillment'])
                        on_time = len(result['fulfillment'][result['fulfillment']['Status'] == 'ON TIME'])
                        fulfill_pct = (on_time / total_orders * 100) if total_orders > 0 else 0
                        
                        database.log_run(
                            st.session_state['username'],
                            0, # No melt capacity in grinding
                            total_orders,
                            fulfill_pct,
                            0, # No melt used
                            run_status="Grinding Success"
                        )
                    except Exception: pass

            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)

    # Display Grinding Results
    if 'grinding_results' in st.session_state and planner_mode == "Grinding":
        g_results = st.session_state['grinding_results']
        g_schedule = g_results['schedule']
        g_fulfill = g_results['fulfillment']
        g_daily = g_results['daily']
        
        st.header("📊 Grinding Summary")
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Units Planned", f"{g_schedule['Units'].sum():,.0f}")
        c2.metric("Orders Fulfilled", f"{len(g_fulfill[g_fulfill['Status']=='ON TIME'])}/{len(g_fulfill)}")
        c3.metric("Peak Daily Output", f"{g_daily['Total_Started'].max():,.0f}")
        
        st.markdown("---")
        
        t1, t2, t3 = st.tabs(["🗓️ Grinding Schedule", "🚚 Fulfillment", "📈 Daily Output"])
        
        with t1:
            st.dataframe(g_schedule, use_container_width=True)
            
        with t2:
             c_pie, c_table = st.columns([1, 2])
             with c_pie:
                 fig = px.pie(g_fulfill, names='Status', title="Status Distribution", color='Status', color_discrete_map={'ON TIME':'#2ecc71', 'LATE':'#e74c3c', 'SHORT':'#f1c40f'})
                 st.plotly_chart(fig, use_container_width=True)
             with c_table:
                 st.dataframe(g_fulfill, use_container_width=True)
        
        with t3:
            fig = px.bar(g_daily, x='Date', y='Total_Started', title="Daily Units Started")
            st.plotly_chart(fig, use_container_width=True)
            
        st.markdown("---")
        st.subheader("💾 Export Grinding Results")
        buffer = grinding_optimization_engine.generate_grinding_excel(g_results)
        st.download_button(
            "Download Grinding Schedule", 
            buffer, 
            f"Grinding_Schedule_{datetime.now().strftime('%Y%m%d')}.xlsx", 
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            type="primary"
        )
