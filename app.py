import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import optimization_engine
import io

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
    
    st.subheader("2. Planning Parameters")
    planning_date = st.date_input("Planning Start Date", date.today())
    
    daily_melt_tons = st.number_input("Daily Melt Capacity (Tons)", value=250.0, step=10.0)
    line_hours = st.number_input("Line Hours per Day", value=16.0, step=1.0)
    line_oee = st.slider("Line OEE (Efficiency)", 0.5, 1.0, 0.90)
    
    st.subheader("3. Optimization Constraints")
    
    with st.expander("Advanced Penalties"):
        shortage_penalty = st.number_input("Shortage Penalty", value=100000000.0)
        leadtime_days = st.number_input("Required Lead Time (Days)", value=14)
        lateness_penalty = st.number_input("Lateness Penalty / Day", value=50.0)
    
    run_btn = st.button("Run Optimization", type="primary")

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
    if run_btn:
        with st.spinner("🔄 Running Optimization... This may take a few minutes."):
            try:
                # 1. Setup Config
                config = optimization_engine.OptimizationConfig(
                    daily_melt_tons=daily_melt_tons,
                    line_hours_per_day=line_hours,
                    line_oee=line_oee,
                    shortage_penalty=shortage_penalty,
                    leadtime_required_days=int(leadtime_days),
                    lateness_penalty_per_day=lateness_penalty,
                    planning_date=pd.to_datetime(planning_date)
                )
                
                # 2. Load Data
                (
                    products, days, demand_boxes, bunch_weight_kg, box_qty,
                    line_time_min, cycle_days, line, box_size_of, box_max_boxes,
                    max_melt_kg_per_day, max_time_small_min, max_time_big_min,
                    order_list, wip_coverage_boxes, gross_demand_boxes
                ) = optimization_engine.load_casting_data_from_excel(uploaded_file, config)
                
                # 3. Solve
                result = optimization_engine.build_and_solve_enhanced_milp(
                    products, days, demand_boxes, bunch_weight_kg, box_qty,
                    line_time_min, cycle_days, line, box_size_of, box_max_boxes,
                    max_melt_kg_per_day, max_time_small_min, max_time_big_min,
                    order_list, wip_coverage_boxes, gross_demand_boxes, config
                )
                
                # Check for solver failure (result is a tuple, index 0 is prob)
                # If prob.status is not optimal or feasible, we might have issues
                # optimization_engine logic returns valid lists (empty or not) even if failed
                # but let's check the problem status if available
                prob = result[0]
                
                # Pulp status: 1=Optimal, -1=Infeasible, -2=Unbounded, -3=Undefined, 0=Not Solved
                # However, optimization_engine.py catches this and returns a tuple.
                
                if prob is None or (prob.status != 1 and prob.status != 0): # 1 is Optimal in Pulp constants usually, but safer to rely on result presence
                    # Wait, build_and_solve_enhanced_milp returns (prob, ...)
                    # If prob is None (e.g. if we modified engine to return None on error), handle it.
                    pass
                
                # Using the pattern from optimization_engine: it returns the tuple regardless.
                # If solve failed, lists might be empty.
                
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
                    st.success("✅ Optimization Complete!")

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
        
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Capacity Utilization", "📦 Box Utilization", "🚚 Order Fulfillment", "🗓️ Production Schedule"])
        
        with tab1:
            st.subheader("Daily Capacity Utilization")
            
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

        with tab2:
            st.subheader("Mould Box Bottlenecks")
            
            # Heatmap of utilization
            # Prepare data for heatmap: Box Size vs Date with Utilization as value
            if not box_df.empty:
                heatmap_data = box_df.pivot(index='Box_Size', columns='Date', values='Utilization_%')
                
                fig_heat = px.imshow(
                    heatmap_data,
                    labels=dict(x="Date", y="Box Size", color="Utilization %"),
                    x=heatmap_data.columns,
                    y=heatmap_data.index,
                    color_continuous_scale='RdYlGn_r', # Red is high utilization (bad/busy) ? Or maybe Green to Red
                    # Usually 100% is bad if it restricts, so let's use a scale where high is 'hot'
                    aspect="auto",
                    title="Box Utilization Heatmap"
                )
                st.plotly_chart(fig_heat, use_container_width=True)
                
                # Critical Boxes Table
                st.markdown("#### Critical Box Sizes (>80% Avg Utilization)")
                box_summary = box_df.groupby('Box_Size')['Utilization_%'].mean().sort_values(ascending=False).reset_index()
                critical_boxes = box_summary[box_summary['Utilization_%'] > 80]
                st.dataframe(critical_boxes, hide_index=True)

        with tab3:
            st.subheader("Order Fulfillment Status")
            
            fig_pie = px.pie(
                shortage_df, names='Status', title="Order Status Distribution",
                color='Status',
                color_discrete_map={'ON TIME': '#2ecc71', 'LATE': '#e74c3c', 'SHORT': '#f1c40f'}
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
            st.markdown("#### Delayed / Short Orders")
            problem_orders = shortage_df[shortage_df['Status'] != 'ON TIME'].sort_values('Days Until Due')
            st.dataframe(
                problem_orders[['Sales Order No', 'FG Code', 'Due Date', 'Order Qty (pieces)', 'Shortage (pieces)', 'Status', 'Days Until Due']],
                hide_index=True,
                use_container_width=True
            )

        with tab4:
            st.subheader("Production Schedule Details")
            st.dataframe(schedule_df, use_container_width=True, hide_index=True)

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
