import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import time
from database_manager import DatabaseManager

def get_latest_data():
    """Get latest data from database"""
    print("Getting fresh latest data...")
    db_manager = DatabaseManager()
    data = db_manager.get_latest_data()
    print(f"Got fresh latest data: {len(data)} records")
    return data

def get_previous_data():
    """Get previous collection data for comparison"""
    print("Getting previous data for comparison...")
    db_manager = DatabaseManager()
    try:
        db_manager.ensure_connection()
        cursor = db_manager.connection.cursor()
        
        # Get the second most recent timestamp (previous collection)
        query = """
        SELECT * FROM ncu_data 
        WHERE timestamp = (
            SELECT DISTINCT timestamp FROM ncu_data 
            WHERE project != 'AAA' 
            ORDER BY timestamp DESC 
            LIMIT 1 OFFSET 1
        )
        AND project != 'AAA'
        ORDER BY project, ncu
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        if data:
            print(f"Previous data found: {len(data)} records")
            df = pd.DataFrame(data, columns=columns)
            print(f"Retrieved {len(df)} previous records from database")
            return df
        else:
            print("No previous data found")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error fetching previous data: {e}")
        return pd.DataFrame()
    
def calculate_delta_and_percentage(current, previous, metric_name):
    """Calculate delta and display percentage with correct visual meaning in Streamlit"""
    # print(f"Calculating delta for {metric_name}: current={current}, previous={previous}")
    
    if previous == 0:
        if current == 0:
            # print(f"Both current and previous are 0 for {metric_name}")
            return 0, "0%"
        else:
            # print(f"Previous was 0, current is {current} for {metric_name}")
            return current, f"+∞%"

    delta = current - previous
    percentage = (delta / previous) * 100
    
    # print(f"Delta: {delta}, Percentage: {percentage:.1f}% for {metric_name}")

    # Metrics where an increase is GOOD (green up arrow, red down arrow)
    positive_metrics = ['ok_status', 'master_mode', 'manual_mode']

    if metric_name in positive_metrics:
        # More is better - show actual delta and percentage
        if delta > 0:
            result = (delta, f"+{percentage:.1f}%")
        elif delta < 0:
            result = (delta, f"{percentage:.1f}%")  # Will be negative
        else:
            result = (0, "0%")
    else:
        # Less is better - show actual delta and percentage
        if delta > 0:
            result = (delta, f"+{percentage:.1f}%")  # Will show red
        elif delta < 0:
            result = (delta, f"{percentage:.1f}%")  # Will show green
        else:
            result = (0, "0%")
    
    # print(f"Result for {metric_name}: {result}")
    return result

def get_stats():
    """Get collection statistics"""
    print("Getting fresh stats...")
    db_manager = DatabaseManager()
    stats = db_manager.get_collection_stats()
    print("Got fresh stats")
    return stats

def create_realtime_dashboard():
    """Create real-time dashboard page"""
    st.title("🔴 Real-time NCU Dashboard")
    
    # Initialize session state for auto-refresh
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = time.time()
    if 'auto_refresh_enabled' not in st.session_state:
        st.session_state.auto_refresh_enabled = False
    if 'refresh_interval' not in st.session_state:
        st.session_state.refresh_interval = 30
    
    # Add refresh button and auto-refresh checkbox
    col1, = st.columns([ 1])
    
    with col1:
        if st.button("🔄 Refresh Data", key="refresh_btn"):
            st.session_state.last_refresh = time.time()
            st.rerun()

    # Get fresh data every time
    latest_data = get_latest_data()
    previous_data = get_previous_data()
    stats = get_stats()
    
    # Debug info
    # st.sidebar.write("Debug Info:")
    # st.sidebar.write(f"Latest data shape: {latest_data.shape if not latest_data.empty else 'Empty'}")
    # st.sidebar.write(f"Previous data shape: {previous_data.shape if not previous_data.empty else 'Empty'}")
    
    # Display collection info
    st.subheader("📊 Data Collection Status")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📈 Total Collections", stats['total_collections'])
    with col2:
        st.metric("🕐 Last 24hrs", stats['recent_collections'])
    with col3:
        if stats['last_collection']:
            last_time = stats['last_collection'][0]
            if isinstance(last_time, str):
                try:
                    last_time = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                except:
                    last_time = datetime.now()
            st.metric("⏰ Last Update", last_time.strftime("%H:%M:%S"))
        else:
            st.metric("⏰ Last Update", "Never")
    with col4:
        if stats['last_collection']:
            records = stats['last_collection'][1]
            st.metric("📋 Last Records", records)
        else:
            st.metric("📋 Last Records", "0")
    
    latest_timestamp = latest_data['date_time'].iloc[0]
    # Show current time
    st.info(f"🕐 Dashboard loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |-|-| 📅 Data as of: {latest_timestamp}")
    
    if not latest_data.empty:
        
        # Summary metrics - All 11 items in specified order
        st.subheader("🎯 System Overview")
        
        # Calculate current totals/values for all 11 metrics using numpy
        current_metrics = {
            'alarm': int(np.sum(latest_data['alarm'])),
            'battery_alarm': int(np.sum(latest_data['battery_alarm'])),
            'battery_warning': int(np.sum(latest_data['battery_warning'])),
            'warning_count': int(np.sum(latest_data['warning_count'])),
            'ok_status': int(np.sum(latest_data['ok_status'])),
            'communication_error': int(np.sum(latest_data['communication_error'])),
            'master_mode': int(np.sum(latest_data['master_mode'])),
            'manual_mode': int(np.sum(latest_data['manual_mode'])),
            'inactive_tcu': int(np.sum(latest_data['inactive_tcu'])),
            'max_wind_speed': float(np.max(latest_data['max_wind_speed'])),
            'avg_wind_speed': float(np.mean(latest_data['avg_wind_speed']))
        }
        
        print("Current metrics:", current_metrics)
        
        # Calculate previous totals if previous data exists
        previous_metrics = {}
        if not previous_data.empty:
            previous_metrics = {
                'alarm': int(np.sum(previous_data['alarm'])),
                'battery_alarm': int(np.sum(previous_data['battery_alarm'])),
                'battery_warning': int(np.sum(previous_data['battery_warning'])),
                'warning_count': int(np.sum(previous_data['warning_count'])),
                'ok_status': int(np.sum(previous_data['ok_status'])),
                'communication_error': int(np.sum(previous_data['communication_error'])),
                'master_mode': int(np.sum(previous_data['master_mode'])),
                'manual_mode': int(np.sum(previous_data['manual_mode'])),
                'inactive_tcu': int(np.sum(previous_data['inactive_tcu'])),
                'max_wind_speed': float(np.max(previous_data['max_wind_speed'])),
                'avg_wind_speed': float(np.mean(previous_data['avg_wind_speed']))
            }
            print("Previous metrics:", previous_metrics)
        else:
            print("No previous metrics available")
        
        # Create 11 columns for all 11 metrics
        col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11 = st.columns(11)
        
        # Display metrics with comparison
        metrics_config = [
            (col1, "🚨 Alarms", "alarm"),
            (col2, "🪫 Batt Alarms", "battery_alarm"),
            (col3, "⚡ Batt Warnings", "battery_warning"),
            (col4, "⚠️ Warnings", "warning_count"),
            (col5, "✅ OK Status", "ok_status"),
            (col6, "📡 Comm Errors", "communication_error"),
            (col7, "🤖 Master Mode", "master_mode"),
            (col8, "👤 Manual Mode", "manual_mode"),
            (col9, "⛔ Inactive", "inactive_tcu"),
            (col10, "💨 Max Wind", "max_wind_speed"),
            (col11, "🌬️ Avg Wind", "avg_wind_speed")
        ]
        
        for col, label, metric_key in metrics_config:
            current_value = current_metrics[metric_key]
            
            if previous_metrics and metric_key in previous_metrics:
                previous_value = previous_metrics[metric_key]
                delta, percentage = calculate_delta_and_percentage(current_value, previous_value, metric_key)
                
                # Format value based on metric type
                if metric_key in ['max_wind_speed', 'avg_wind_speed']:
                    display_value = f"{current_value:.1f} m/s"
                    if delta != 0:
                        delta_display = f"{delta:+.1f} ({percentage})"
                    else:
                        delta_display = percentage
                else:
                    display_value = str(int(current_value))
                    if delta != 0:
                        delta_display = f"{delta:+d} ({percentage})"
                    else:
                        delta_display = percentage
                
                # print(f"Displaying metric {metric_key}: value={display_value}, delta={delta_display}")
                col.metric(label, display_value, delta_display)
            else:
                # No previous data available
                if metric_key in ['max_wind_speed', 'avg_wind_speed']:
                    display_value = f"{current_value:.1f} m/s"
                else:
                    display_value = str(int(current_value))
                col.metric(label, display_value)

        # Show comparison info
        if not previous_data.empty:
            prev_timestamp = previous_data['date_time'].iloc[0]
            if isinstance(prev_timestamp, str):
                try:
                    prev_timestamp = pd.to_datetime(prev_timestamp)
                except:
                    prev_timestamp = None
            
            if prev_timestamp:
                st.caption(f"📊 Compared to previous collection: {prev_timestamp}")
        else:
            st.caption("📊 No previous data available for comparison")

        # Project-wise summary
        st.subheader("📊 Project-wise Status")

        # Aggregate data by project using pandas groupby
        project_summary = latest_data.groupby('project').agg({
            'alarm': 'sum',
            'battery_alarm': 'sum',
            'battery_warning': 'sum',
            'warning_count': 'sum',
            'ok_status': 'sum',
            'communication_error': 'sum',
            'inactive_tcu': 'sum',
            'max_wind_speed': 'max',
            'avg_wind_speed': 'mean'
        }).reset_index()

        # Build custom hover text for each project
        hover_text = [
            f"<span style='font-size:15px;'>"
            f"<b>📂 Project:</b> {row['project']}<br>"
            f"🚨 <b>Alarms:</b> {row['alarm']}<br>"
            f"⚠️ <b>Warnings:</b> {row['warning_count']}<br>"
            f"🔋 <b>Battery Alarm:</b> {row['battery_alarm']}<br>"
            f"✅ <b>OK Status:</b> {row['ok_status']}<br>"
            f"📡 <b>Comm Errors:</b> {row['communication_error']}<br>"
            f"🔕 <b>Inactive:</b> {row['inactive_tcu']}<br>"
            f"</span>"
            for _, row in project_summary.iterrows()
        ]

        # Create stacked bar chart
        fig = go.Figure()

        # Define trace configurations
        traces = [
            ('🚨 Alarms', 'alarm', '#ff4444', True),
            ('⚠️ Warnings', 'warning_count', '#ffa500', True),
            ('🔋 Battery Alarm', 'battery_alarm', '#ff8c00', True),
            ('✅ OK Status', 'ok_status', '#28a745', False),
            ('📡 Comm Errors', 'communication_error', '#6f42c1', True),
            ('🔕 Inactive TCU', 'inactive_tcu', '#adb5bd', False)
        ]

        # Add traces dynamically
        for name, col, color, visible in traces:
            fig.add_trace(go.Bar(
                name=name,
                x=project_summary['project'],
                y=project_summary[col],
                marker_color=color,
                hovertext=hover_text,
                hoverinfo='text',
                visible=True if visible else 'legendonly'
            ))

        # Configure layout
        fig.update_layout(
            title="📈 Status Distribution by Project",
            barmode='stack',
            height=500,
            width=1600,
            xaxis_title="Projects",
            yaxis_title="Count",
            showlegend=True,
            xaxis=dict(
                tickangle=45,
                type='category',
                tickfont=dict(size=12)
            ),
            yaxis=dict(
                tickfont=dict(size=12)
            )
        )

        # Render the chart
        st.plotly_chart(fig, use_container_width=False)

        # NCU-wise detailed view
        st.subheader("🏭 NCU Details")
        
        # Create tabs for different projects
        projects = latest_data['project'].unique()

        if len(projects) > 0:
            # Project selection dropdown
            selected_project = st.selectbox(
                "📂 Select Project",
                projects,
                index=0,
                key="project_selector"
            )

            # Filter data for selected project
            project_data = latest_data[latest_data['project'] == selected_project]
            
            # Project title - Big and Clear
            st.markdown(f"## Project Overview - {selected_project}")
            st.markdown("---")

            col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
            with col1:
                st.markdown(f"### 📂 {project_data['ncu'].nunique()}")
                st.markdown("**NCU**")
            with col2:
                st.markdown(f"### 🚨 {int(project_data['alarm'].sum())}")
                st.markdown("**Alarms**")
            with col3:
                st.markdown(f"### ⚠️ {int(project_data['warning_count'].sum())}")
                st.markdown("**Warnings**")
            with col4:
                st.markdown(f"### ✅ {int(project_data['ok_status'].sum())}")
                st.markdown("**OK Status**")
            with col5:
                st.markdown(f"### 📡 {int(project_data['communication_error'].sum())}")
                st.markdown("**Comm Errors**")
            with col6:
                st.markdown(f"### ⛔ {int(project_data['inactive_tcu'].sum())}")
                st.markdown("**Inactive**")
            with col7:
                st.markdown(f"### 🌬️ {project_data['max_wind_speed'].max():.1f}")
                st.markdown("**Max Wind**")
            st.markdown("---")   

            # NCU Details - Simple and Large
            for _, row in project_data.iterrows():
                # NCU Name - Very Large
                st.markdown(f"## 🔹 {row['ncu']}")
                
                # 7 Values in Large Text - Single Row
                col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
                
                with col1:
                    st.markdown(f"### 🚨 {int(row['alarm'])}")
                    st.markdown("**Alarm**")
                
                with col2:
                    st.markdown(f"### ⚠️ {int(row['warning_count'])}")
                    st.markdown("**Warnings**")
                
                with col3:
                    st.markdown(f"### 🔋 {int(row['ok_status'])}")
                    st.markdown("**Ok**")

                with col4:
                    st.markdown(f"### 📡 {int(row['communication_error'])}")
                    st.markdown("**Comm Error**")
                
                with col5:
                    st.markdown(f"### 💤 {int(row['inactive_tcu'])}")
                    st.markdown("**Inactive**")
                
                with col6:
                    st.markdown(f"### 🌬️ {row['max_wind_speed']:.1f}")
                    st.markdown("**Max Wind**")
                
                with col7:
                    st.markdown(f"### 🌪️ {row['avg_wind_speed']:.1f}")
                    st.markdown("**Avg Wind**")
                
                st.markdown("---")         
       
        # Raw data table (collapsible)
        with st.expander("🔍 View Raw Data"):
            # Select columns to display
            display_cols = ['project', 'ncu', 'alarm', 'warning_count', 'ok_status', 
                          'battery_alarm', 'communication_error', 'max_wind_speed', 'avg_wind_speed']
            available_cols = [col for col in display_cols if col in latest_data.columns]
            st.dataframe(latest_data[available_cols], use_container_width=True)
    
    else:
        st.warning("⚠️ No data available. Please check data collection.")
        
        # Show collection history even when no data
        st.subheader("📋 Collection History")
        if stats['recent_history']:
            history_df = pd.DataFrame(stats['recent_history'], 
                                    columns=['Time', 'Records', 'Excluded', 'Success'])
            history_df['Status'] = history_df['Success'].apply(lambda x: '✅' if x else '❌')
            st.dataframe(history_df[['Time', 'Records', 'Excluded', 'Status']], 
                       use_container_width=True)