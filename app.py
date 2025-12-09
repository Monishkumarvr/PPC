import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import optimization_engine
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

# Sidebar for Controls
with st.sidebar:
    st.header("⚙️ Configuration")
    
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
            
            # Data Editor Section
            with st.expander("📝 View & Edit Master Data", expanded=False):
                data_tabs = st.tabs([s for s in sheet_status.keys() if sheet_status[s]])
                for i, sheet_name in enumerate(data_tabs):
                    with data_tabs[i]:
                        if sheet_name in st.session_state['master_data']:
                            edited_df = st.data_editor(
                                st.session_state['master_data'][sheet_name],
                                num_rows="dynamic",
                                key=f"editor_{sheet_name}"
                            )
                            st.session_state['master_data'][sheet_name] = edited_df

    st.subheader("2. Planning Parameters")
    planning_date = st.date_input("Planning Start Date", date.today())
    planning_end_date = st.date_input("Planning End Date (Optional)", value=None)
    
    daily_melt_tons = st.number_input("Daily Melt Capacity (Tons)", value=250.0, step=10.0)
    line_hours = st.number_input("Line Hours per Day", value=16.0, step=1.0)
    line_oee = st.slider("Line OEE (Efficiency)", 0.5, 1.0, 0.90)
    
    st.subheader("3. Optimization Constraints")
    
    with st.expander("Advanced Penalties"):
        shortage_penalty = st.number_input("Shortage Penalty", value=100000000.0)
        leadtime_days = st.number_input("Required Lead Time (Days)", value=14)
        lateness_penalty = st.number_input("Lateness Penalty / Day", value=50.0)
        early_penalty = st.number_input("Early Production Penalty", value=0.0)
        leadtime_pen_val = st.number_input("Lead Time Penalty / Day", value=25.0)
        solver_timeout = st.number_input("Solver Timeout (seconds)", value=600)
    
    run_btn = st.button("Run Optimization", type="primary", disabled=not file_valid)

# Main Content Area

if uploaded_file is None:
    st.info("👋 Welcome! Please upload your **Master Data Excel file** in the sidebar to begin optimization.")
    
    # Optional: Display sample data structure or instructions
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
            
            # 2. Load Data (Fast) - From Session State (Edited Data)
            with st.spinner("Processing Data..."):
                input_data = st.session_state['master_data']
                (
                    products, days, demand_boxes, bunch_weight_kg, box_qty,
                    line_time_min, cycle_days, line, box_size_of, box_max_boxes,
                    max_melt_kg_per_day, max_time_small_min, max_time_big_min,
                    order_list, wip_coverage_boxes, gross_demand_boxes
                ) = optimization_engine.process_casting_data(input_data, config)

            # 3. Solve (Slow - Threaded with Log)
            status_container = st.status("🚀 Running Optimization...", expanded=True)
            log_container = status_container.empty()
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp_log:
                log_path = tmp_log.name
            
            def run_optimization():
                return optimization_engine.build_and_solve_enhanced_milp(
                    products, days, demand_boxes, bunch_weight_kg, box_qty,
                    line_time_min, cycle_days, line, box_size_of, box_max_boxes,
                    max_melt_kg_per_day, max_time_small_min, max_time_big_min,
                    order_list, wip_coverage_boxes, gross_demand_boxes, config,
                    log_path=log_path
                )

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_optimization)
                
                # Poll logs while running
                while not future.done():
                    time.sleep(0.5)
                    try:
                        with open(log_path, "r") as f:
                            lines = f.readlines()
                            if lines:
                                # Show last 15 lines
                                last_lines = "".join(lines[-15:])
                                log_container.code(last_lines, language="text")
                    except Exception:
                        pass
                
                result = future.result()
                
            # Final log update
            try:
                with open(log_path, "r") as f:
                    full_log = f.read()
                    # Keep full log in session state if user wants to see it?
                    # For now just show "Done"
                    log_container.code(full_log[-1000:], language="text")
                os.unlink(log_path)
            except:
                pass
            
            status_container.update(label="✅ Optimization Complete!", state="complete", expanded=False)

            # Check results
            prob = result[0]
            prob, schedule_rows, order_shortage_rows, daily_capacity_rows, box_utilization_rows = result

            if not schedule_rows and not order_shortage_rows:
                    st.warning("Optimization finished but returned no schedule. This might mean the problem was infeasible or no orders were selected.")
            else:
                st.session_state['results'] = {
                    'schedule_rows': schedule_rows,
                    'order_shortage_rows': order_shortage_rows,
                    'daily_capacity_rows': daily_capacity_rows,
                    'box_utilization_rows': box_utilization_rows
                }

        except Exception as e:
            st.error(f"An error occurred: {e}")
            st.exception(e)

    # Display Results if available
    if 'results' in st.session_state:
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
        
        total_melt = capacity_df['Melt_Used_kg'].sum() / 1000.0 # Tons
        avg_melt_util = capacity_df['Melt_Utilization_%'].mean()
        
        c1.metric("Order Fulfillment", f"{on_time_pct:.1f}%", f"{on_time}/{total_orders} Orders")
        c2.metric("Total Melt Scheduled", f"{total_melt:,.1f} Tons")
        c3.metric("Avg Melt Utilization", f"{avg_melt_util:.1f}%")
        
        total_boxes = schedule_df['Boxes'].sum() if not schedule_df.empty else 0
        c4.metric("Total Boxes Moulded", f"{total_boxes:,.0f}")
        
        st.markdown("---")
        
        # --- Visualizations ---
        # Tab Order: Production Schedule, Order Fulfillment, Capacity Analysis, Daily Box Status, AI Assistant, Input Data
        tab_schedule, tab_fulfill, tab_cap, tab_box, tab_ai, tab_input = st.tabs([
            "🗓️ Production Schedule", 
            "🚚 Order Fulfillment", 
            "📈 Capacity Analysis",
            "📦 Daily Box Status",
            "🤖 AI Assistant",
            "📂 Input Data"
        ])
        
        with tab_schedule:
            st.subheader("Daily Production Plan")
            st.dataframe(schedule_df, use_container_width=True, hide_index=True)
            
            with st.expander("📊 Custom Pivot Analysis"):
                if not schedule_df.empty:
                    c1, c2, c3, c4 = st.columns(4)
                    
                    available_cols = schedule_df.columns.tolist()
                    numeric_cols = schedule_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                    
                    with c1:
                        pivot_index = st.multiselect("Rows (Index)", available_cols, default=["Date"])
                    with c2:
                        pivot_columns = st.multiselect("Columns", available_cols, default=["FG Code"])
                    with c3:
                        pivot_values = st.selectbox("Values", numeric_cols, index=numeric_cols.index("Boxes") if "Boxes" in numeric_cols else 0)
                    with c4:
                        pivot_agg = st.selectbox("Aggregation", ["sum", "mean", "count", "min", "max"], index=0)
                    
                    if pivot_index and pivot_values:
                        try:
                            # Filter option (e.g. by Box Size)
                            if "Box_Size" in available_cols:
                                all_boxes = ["All"] + sorted(schedule_df['Box_Size'].astype(str).unique().tolist())
                                filter_box = st.selectbox("Filter Box Size (Optional)", all_boxes)
                                pivot_data = schedule_df if filter_box == "All" else schedule_df[schedule_df['Box_Size'] == filter_box]
                            else:
                                pivot_data = schedule_df

                            if not pivot_data.empty:
                                pivot_table = pivot_data.pivot_table(
                                    index=pivot_index,
                                    columns=pivot_columns if pivot_columns else None,
                                    values=pivot_values,
                                    aggfunc=pivot_agg,
                                    fill_value=0
                                )
                                st.dataframe(pivot_table, use_container_width=True)
                            else:
                                st.info("No data available for the selected filter.")
                        except Exception as e:
                            st.error(f"Could not create pivot table: {e}")
                    else:
                        st.warning("Please select at least one Row and Value.")

        with tab_fulfill:
            st.subheader("Order Fulfillment Status")
            
            c_pie, c_table = st.columns([1, 2])
            
            with c_pie:
                fig_pie = px.pie(
                    shortage_df, names='Status', title="Status Distribution",
                    color='Status',
                    color_discrete_map={'ON TIME': '#2ecc71', 'LATE': '#e74c3c', 'SHORT': '#f1c40f'},
                    hole=0.4
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with c_table:
                st.markdown("#### Detailed Order List")
                # Display the full dataframe with new columns
                st.dataframe(
                    shortage_df,
                    hide_index=True,
                    use_container_width=True
                )
                
        with tab_cap:
            st.subheader("Capacity Utilization")
            
            # Melt Utilization Chart
            fig_melt = px.bar(
                capacity_df, x='Date', y='Melt_Utilization_%',
                title="Melt Capacity Utilization (%)",
                color='Melt_Utilization_%',
                color_continuous_scale='Blues',
                range_y=[0, 110]
            )
            fig_melt.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="Max Capacity")
            st.plotly_chart(fig_melt, use_container_width=True)
            
            # Line Utilization
            line_melt_df = capacity_df[['Date', 'Small_Utilization_%', 'Big_Utilization_%']].melt(
                id_vars=['Date'], var_name='Line', value_name='Utilization'
            )
            fig_line = px.line(
                line_melt_df, x='Date', y='Utilization', color='Line',
                title="Moulding Line Utilization (%)",
                range_y=[0, 110]
            )
            st.plotly_chart(fig_line, use_container_width=True)
            
            st.markdown("---")
            st.subheader("Mould Box Bottlenecks")
            if not box_df.empty:
                heatmap_data = box_df.pivot(index='Box_Size', columns='Date', values='Utilization_%')
                fig_heat = px.imshow(
                    heatmap_data,
                    labels=dict(x="Date", y="Box Size", color="Utilization %"),
                    x=heatmap_data.columns,
                    y=heatmap_data.index,
                    color_continuous_scale='RdYlGn_r',
                    aspect="auto",
                    title="Box Utilization Heatmap"
                )
                st.plotly_chart(fig_heat, use_container_width=True)

        with tab_box:
            st.subheader("Daily Box Utilization Status")
            st.dataframe(box_df, use_container_width=True, hide_index=True)

        with tab_ai:
            st.subheader("🤖 AI Assistant Recommendations")
            st.info("The AI Assistant analyzes the optimization results to identify bottlenecks and suggest actionable improvements.")
            
            if st.button("Generate Recommendations", type="primary"):
                with st.spinner("Analyzing schedule data..."):
                    recommendations = optimization_engine.generate_recommendations(
                        results['daily_capacity_rows'],
                        results['box_utilization_rows'],
                        results['order_shortage_rows']
                    )
                    
                    if not recommendations:
                        st.success("No critical issues found. The plan is optimized!")
                    else:
                        for rec in recommendations:
                            severity_icon = "🔴" if rec['severity'] == "Critical" else ("🟠" if rec['severity'] == "High" else "🟡")
                            with st.expander(f"{severity_icon} {rec['type']} Recommendation ({rec['severity']})", expanded=True):
                                st.markdown(rec['message'])

        with tab_input:
            st.subheader("📂 Master Data Used")
            if 'master_data' in st.session_state:
                md_sheets = st.session_state['master_data']
                sheet_names = list(md_sheets.keys())
                
                if sheet_names:
                    i_tabs = st.tabs(sheet_names)
                    for idx, s_name in enumerate(sheet_names):
                        with i_tabs[idx]:
                            st.dataframe(md_sheets[s_name], use_container_width=True)
                else:
                    st.info("No Master Data found.")
            else:
                st.info("Master Data not loaded.")

        # --- Download Section ---
        st.markdown("---")
        st.subheader("💾 Export Results")
        
        excel_buffer = optimization_engine.generate_excel_output(
            results['schedule_rows'],
            results['order_shortage_rows'],
            results['daily_capacity_rows'],
            results['box_utilization_rows']
        )
        
        st.download_button(
            label="Download Complete Schedule (Excel)",
            data=excel_buffer,
            file_name=f"Casting_Schedule_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
