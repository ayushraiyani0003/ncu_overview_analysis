import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import threading
from database_manager import DatabaseManager

def create_deep_analysis_page():
    """Create deep analysis page with pattern detection and comprehensive analytics"""
    
    st.title("🔍 Deep Analysis Dashboard")
    st.markdown("---")
    
    # Initialize database manager
    try:
        db_manager = DatabaseManager()
        db_manager.ensure_connection()
        # Ensure connection_lock exists
        if not hasattr(db_manager, 'connection_lock'):
            db_manager.connection_lock = threading.Lock()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return
    
    # Top Controls Section
    col1, col2, col3, col4 = st.columns([2, 2, 2, 3])
    
    with col1:
        # Date range selector
        date_range = st.selectbox(
            "📅 Select Time Range",
            options=[1, 2, 3, 5, 7, 15, 30, 60, 180],
            index=4,  # Default to 7 days
            format_func=lambda x: f"{x} days"
        )
    
    with col2:
        # Analysis type selector
        analysis_type = st.radio(
            "🎯 Analysis Type",
            options=["Project-wise", "NCU-wise"],
            horizontal=True
        )
    
    with col3:
        # Chart type selector
        chart_type = st.selectbox(
            "📊 Chart Type",
            options=["Time Series", "Heatmap", "Distribution", "Correlation"],
            index=0
        )
    
    with col4:
        # Chart time aggregation
        time_agg = st.selectbox(
            "⏰ Chart Aggregation",
            options=["1 Hour", "30 Minutes", "1 Day"],
            index=0
        )
    
    # Status tabs with additional parameters
    tab_options = [
        ("🚨 Alarm", "alarm"),
        ("✅ OK Status", "ok_status"), 
        ("🔋 Battery Alarm", "battery_alarm"),
        ("⚠️ Battery Warning", "battery_warning"),
        ("⚠️ Warning", "warning_count"),
        ("📡 Comm Error", "communication_error"),
        ("🎛️ Master Mode", "master_mode"),
        ("🔧 Manual Mode", "manual_mode"),
        ("💨 Max Wind Speed", "max_wind_speed"),
        ("🌪️ Avg Wind Speed", "avg_wind_speed"),
        ("📈 Trend Analysis", "trend_analysis"),
        ("🔄 Status Changes", "status_changes")
    ]
    
    # Create tabs
    tabs = st.tabs([tab[0] for tab in tab_options])
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=date_range)
    
    # Get data from database
    data = get_analysis_data(db_manager, start_date, end_date)
    
    if data.empty:
        st.warning("No data available for the selected time range.")
        return
    
    # Process each tab
    for i, (tab_name, column_name) in enumerate(tab_options):
        with tabs[i]:
            if column_name in ["trend_analysis", "status_changes"]:
                process_advanced_analysis_tab(data, column_name, analysis_type, chart_type, time_agg, tab_name)
            else:
                process_analysis_tab(data, column_name, analysis_type, chart_type, time_agg, tab_name)

def get_analysis_data(db_manager, start_date, end_date):
    """Get data from database for analysis"""
    try:
        with db_manager.connection_lock:
            cursor = db_manager.connection.cursor()
            
            if hasattr(db_manager.connection, 'is_connected'):
                # MySQL query
                query = """
                SELECT timestamp, date_time, project, ncu, 
                       alarm, ok_status, battery_alarm, battery_warning, 
                       warning_count, communication_error, master_mode, 
                       manual_mode, max_wind_speed, avg_wind_speed
                FROM ncu_data 
                WHERE date_time BETWEEN %s AND %s
                AND project != 'AAA'
                ORDER BY date_time
                """
                cursor.execute(query, (start_date, end_date))
            else:
                # SQLite query
                query = """
                SELECT timestamp, date_time, project, ncu, 
                       alarm, ok_status, battery_alarm, battery_warning, 
                       warning_count, communication_error, master_mode, 
                       manual_mode, max_wind_speed, avg_wind_speed
                FROM ncu_data 
                WHERE date_time BETWEEN ? AND ?
                AND project != 'AAA'
                ORDER BY date_time
                """
                cursor.execute(query, (start_date, end_date))
            
            results = cursor.fetchall()
            
            if results:
                columns = ['timestamp', 'date_time', 'project', 'ncu', 
                          'alarm', 'ok_status', 'battery_alarm', 'battery_warning',
                          'warning_count', 'communication_error', 'master_mode',
                          'manual_mode', 'max_wind_speed', 'avg_wind_speed']
                
                df = pd.DataFrame(results, columns=columns)
                df['date_time'] = pd.to_datetime(df['date_time'])
                return df
            else:
                return pd.DataFrame()
                
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def get_sorted_ncus_with_values(data, column_name, project=None):
    """Get NCUs sorted by max values with display format"""
    if project:
        filtered_data = data[data['project'] == project]
    else:
        filtered_data = data
    
    # Group by NCU and get max values
    ncu_values = filtered_data.groupby(['project', 'ncu']).agg({
        column_name: 'max'
    }).reset_index()
    
    # Sort by column value in descending order (highest first)
    ncu_values = ncu_values.sort_values(column_name, ascending=False)
    
    # Create display format and options list
    ncu_options = []
    ncu_mapping = {}
    
    for _, row in ncu_values.iterrows():
        ncu_name = row['ncu']
        max_value = row[column_name]
        project_name = row['project']
        
        # Format display based on column type
        if column_name in ["max_wind_speed", "avg_wind_speed"]:
            display_text = f"{ncu_name} (Max: {max_value:.1f} m/s)"
        else:
            display_text = f"{ncu_name} (Max: {int(max_value)})"
        
        ncu_options.append(display_text)
        ncu_mapping[display_text] = {
            'ncu': ncu_name,
            'project': project_name,
            'max_value': max_value
        }
    
    return ncu_options, ncu_mapping

def process_analysis_tab(data, column_name, analysis_type, chart_type, time_agg, tab_name):
    """Process each analysis tab with enhanced functionality"""
    
    # Top 5 section
    st.subheader(f"🏆 Top 5 {analysis_type.split('-')[0]}s by {tab_name}")
    
    # Calculate aggregated data
    if analysis_type == "Project-wise":
        top_data = get_top_projects(data, column_name)
        group_col = 'project'
    else:
        top_data = get_top_ncus(data, column_name)
        group_col = 'ncu'
    
    # Display top 5 cards
    if not top_data.empty:
        selected_item = display_top_cards(top_data, data, column_name, group_col, analysis_type)
        
        # NCU selector section
        st.subheader("🎛️ Select for Detailed Analysis")
        
        if analysis_type == "Project-wise":
            col1, col2 = st.columns([1, 1])
            with col1:
                # Get all available projects
                all_projects = sorted(data['project'].unique())
                
                # Set default selection based on card click or first project
                if selected_item:
                    default_index = all_projects.index(selected_item) if selected_item in all_projects else 0
                else:
                    default_index = all_projects.index(top_data['project'].iloc[0]) if not top_data.empty else 0
                
                selected_project = st.selectbox(
                    "🏢 Select Project",
                    options=all_projects,
                    index=default_index,
                    key=f"project_selector_{column_name}"
                )
            
            with col2:
                if selected_project:
                    # Get sorted NCUs for the selected project with max values
                    ncu_options, ncu_mapping = get_sorted_ncus_with_values(data, column_name, selected_project)
                    
                    if ncu_options:
                        # Set default based on top NCU for the project
                        default_index = 0  # Always select the top NCU (highest value)
                        
                        selected_ncu_display = st.selectbox(
                            f"🎛️ Select NCU (Sorted by {tab_name})",
                            options=ncu_options,
                            index=default_index,
                            key=f"ncu_selector_{column_name}",
                            help="NCUs are sorted by highest to lowest values"
                        )
                        
                        # Extract actual NCU name from mapping
                        selected_ncu = ncu_mapping[selected_ncu_display]['ncu']
                    else:
                        st.warning("No NCUs found for selected project")
                        return
        else:
            # For NCU-wise analysis - show ALL NCUs sorted by values
            ncu_options, ncu_mapping = get_sorted_ncus_with_values(data, column_name)
            
            if ncu_options:
                # Set default selection based on card click or highest value NCU
                default_index = 0
                if selected_item:
                    # Find the selected NCU in options
                    for i, option in enumerate(ncu_options):
                        if ncu_mapping[option]['ncu'] == selected_item:
                            default_index = i
                            break
                
                selected_ncu_display = st.selectbox(
                    f"🎛️ Select NCU (Sorted by {tab_name})",
                    options=ncu_options,
                    index=default_index,
                    key=f"ncu_selector_ncuwise_{column_name}",
                    help="NCUs are sorted by highest to lowest values"
                )
                
                # Extract actual NCU name and project from mapping
                selected_ncu = ncu_mapping[selected_ncu_display]['ncu']
                selected_project = ncu_mapping[selected_ncu_display]['project']
            else:
                st.warning("No NCUs found")
                return
        
        # Enhanced Charts Section
        st.subheader(f"📊 {chart_type} Analysis - {tab_name} ({selected_project} - {selected_ncu})")
        
        # Display current max value for selected NCU
        current_max = ncu_mapping[selected_ncu_display]['max_value'] if analysis_type == "NCU-wise" else \
                     data[(data['ncu'] == selected_ncu) & (data['project'] == selected_project)][column_name].max()
        
        if column_name in ["max_wind_speed", "avg_wind_speed"]:
            st.info(f"📊 Current Max {tab_name} for {selected_ncu}: **{current_max:.1f} m/s**")
        else:
            st.info(f"📊 Current Max {tab_name} for {selected_ncu}: **{int(current_max)}**")
        
        if chart_type == "Time Series":
            create_time_series_chart(data, selected_ncu, selected_project, column_name, time_agg)
        elif chart_type == "Heatmap":
            create_heatmap_chart(data, selected_ncu, selected_project, column_name)
        elif chart_type == "Distribution":
            create_distribution_chart(data, selected_ncu, selected_project, column_name)
        elif chart_type == "Correlation":
            create_correlation_chart(data, selected_ncu, selected_project, column_name)
            
        # Additional Analysis Section
        create_additional_analysis(data, selected_ncu, selected_project, column_name)
        
    else:
        st.warning(f"No data available for {tab_name}")

def process_advanced_analysis_tab(data, column_name, analysis_type, chart_type, time_agg, tab_name):
    """Process advanced analysis tabs"""
    
    if column_name == "trend_analysis":
        create_trend_analysis(data, analysis_type)
    elif column_name == "status_changes":
        create_status_change_analysis(data, analysis_type)

def get_top_projects(data, column_name):
    """Get top 5 projects by selected metric using max values"""
    if column_name in ["max_wind_speed", "avg_wind_speed"]:
        # For wind speed, use max
        top_data = data.groupby('project').agg({
            column_name: 'max',
            'alarm': 'max',
            'ok_status': 'max',
            'battery_alarm': 'max',
            'battery_warning': 'max',
            'warning_count': 'max',
            'communication_error': 'max',
            'master_mode': 'max',
            'manual_mode': 'max'
        }).round(2)
    else:
        # For other metrics, use max
        top_data = data.groupby('project').agg({
            column_name: 'max',
            'alarm': 'max',
            'ok_status': 'max', 
            'battery_alarm': 'max',
            'battery_warning': 'max',
            'warning_count': 'max',
            'communication_error': 'max',
            'master_mode': 'max',
            'manual_mode': 'max',
            'max_wind_speed': 'max',
            'avg_wind_speed': 'max'
        }).round(2)
    
    top_data = top_data.sort_values(column_name, ascending=False).head(5)
    top_data = top_data.reset_index()
    return top_data

def get_top_ncus(data, column_name):
    """Get top 5 NCUs by selected metric using max values"""
    if column_name in ["max_wind_speed", "avg_wind_speed"]:
        top_data = data.groupby(['project', 'ncu']).agg({
            column_name: 'max',
            'alarm': 'max',
            'ok_status': 'max',
            'battery_alarm': 'max', 
            'battery_warning': 'max',
            'warning_count': 'max',
            'communication_error': 'max',
            'master_mode': 'max',
            'manual_mode': 'max'
        }).round(2)
    else:
        top_data = data.groupby(['project', 'ncu']).agg({
            column_name: 'max',
            'alarm': 'max',
            'ok_status': 'max',
            'battery_alarm': 'max',
            'battery_warning': 'max', 
            'warning_count': 'max',
            'communication_error': 'max',
            'master_mode': 'max',
            'manual_mode': 'max',
            'max_wind_speed': 'max',
            'avg_wind_speed': 'max'
        }).round(2)
    
    top_data = top_data.sort_values(column_name, ascending=False).head(5)
    top_data = top_data.reset_index()
    return top_data

def get_top_ncus_for_project(project_data, column_name):
    """Get top NCUs for a specific project"""
    top_data = project_data.groupby(['project', 'ncu']).agg({
        column_name: 'max'
    }).round(2)
    
    top_data = top_data.sort_values(column_name, ascending=False).head(5)
    top_data = top_data.reset_index()
    return top_data

def display_top_cards(top_data, full_data, column_name, group_col, analysis_type):
    """Display top 5 cards with click functionality"""
    
    cols = st.columns(5)
    selected_item = None
    
    for i, (_, row) in enumerate(top_data.iterrows()):
        with cols[i]:
            if analysis_type == "Project-wise":
                title = row['project']
                subtitle = f"Project"
                item_key = row['project']
            else:
                title = row['ncu']
                subtitle = f"NCU ({row['project']})"
                item_key = row['ncu']
            
            # Get the main metric value (using max values)
            main_value = row[column_name]
            unit = " m/s" if column_name in ["max_wind_speed", "avg_wind_speed"] else ""
            
            # Create tooltip content
            tooltip_content = f"""
            **📊 Status Summary (Max Values):**
            - 🚨 Alarms: {int(row.get('alarm', 0))}
            - ✅ OK Status: {int(row.get('ok_status', 0))}
            - 🔋 Battery Alarms: {int(row.get('battery_alarm', 0))}
            - ⚠️ Battery Warnings: {int(row.get('battery_warning', 0))}
            - ⚠️ Warnings: {int(row.get('warning_count', 0))}
            - 📡 Comm Errors: {int(row.get('communication_error', 0))}
            - 🎛️ Master Mode: {int(row.get('master_mode', 0))}
            - 🔧 Manual Mode: {int(row.get('manual_mode', 0))}
            - 💨 Max Wind Speed: {row.get('max_wind_speed', 0):.1f} m/s
            - 🌪️ Avg Wind Speed: {row.get('avg_wind_speed', 0):.1f} m/s
            """
            
            # Create clickable card
            card_clicked = st.button(
                f"**{title}**\n\n{subtitle}\n\n**{main_value}{unit}**",
                key=f"card_{group_col}_{i}_{column_name}",
                help=tooltip_content,
                use_container_width=True
            )
            
            if card_clicked:
                selected_item = item_key
                st.session_state[f'selected_{group_col}_{column_name}'] = item_key
                st.rerun()
    
    # Check if there's a stored selection
    if f'selected_{group_col}_{column_name}' in st.session_state:
        selected_item = st.session_state[f'selected_{group_col}_{column_name}']
        # Clear the selection after using it
        del st.session_state[f'selected_{group_col}_{column_name}']
    
    return selected_item

def create_time_series_chart(data, selected_ncu, selected_project, column_name, time_agg):
    """Create interactive time series chart for selected NCU"""
    
    # Filter data for selected NCU
    chart_data = data[(data['ncu'] == selected_ncu) & (data['project'] == selected_project)]
    
    if chart_data.empty:
        st.warning("No data available for selected NCU.")
        return
    
    # Set up time aggregation
    freq_map = {
        "1 Hour": "1H",
        "30 Minutes": "30min", 
        "1 Day": "1D"
    }
    freq = freq_map[time_agg]
    
    # Create time series aggregation
    chart_data['date_only'] = chart_data['date_time'].dt.date
    chart_data['time_period'] = chart_data['date_time'].dt.floor(freq)
    
    # Aggregate data
    if column_name in ["max_wind_speed", "avg_wind_speed"]:
        agg_data = chart_data.groupby(['date_only', 'time_period']).agg({
            column_name: 'max'  # Take maximum wind speed in the period
        }).reset_index()
    else:
        agg_data = chart_data.groupby(['date_only', 'time_period']).agg({
            column_name: 'max'  # Take maximum count in the period
        }).reset_index()
    
    # Create the plot
    fig = go.Figure()
    
    # Get unique dates for color mapping
    unique_dates = sorted(agg_data['date_only'].unique())
    colors = px.colors.qualitative.Set3[:len(unique_dates)]
    
    # Add traces for each date
    for i, date in enumerate(unique_dates):
        date_data = agg_data[agg_data['date_only'] == date]
        
        if not date_data.empty:
            fig.add_trace(go.Scatter(
                x=date_data['time_period'].dt.time,
                y=date_data[column_name],
                mode='lines+markers',
                name=f"{date}",
                line=dict(color=colors[i % len(colors)]),
                hovertemplate=(
                    f"<b>{selected_ncu} ({selected_project})</b><br>"
                    f"Date: {date}<br>"
                    f"Time: %{{x}}<br>"
                    f"Value: %{{y}}<br>"
                    "<extra></extra>"
                ),
                showlegend=True
            ))
    
    # Update layout
    fig.update_layout(
        title=f"{column_name.replace('_', ' ').title()} - {selected_ncu} ({selected_project}) - {time_agg} intervals",
        xaxis_title="Time of Day",
        yaxis_title=column_name.replace('_', ' ').title(),
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left", 
            x=1.02
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_heatmap_chart(data, selected_ncu, selected_project, column_name):
    """Create heatmap chart showing patterns by hour and day"""
    
    chart_data = data[(data['ncu'] == selected_ncu) & (data['project'] == selected_project)]
    
    if chart_data.empty:
        st.warning("No data available for selected NCU.")
        return
    
    # Create hour and day columns
    chart_data['hour'] = chart_data['date_time'].dt.hour
    chart_data['day'] = chart_data['date_time'].dt.day_name()
    
    # Create pivot table for heatmap
    heatmap_data = chart_data.groupby(['day', 'hour'])[column_name].max().unstack(fill_value=0)
    
    # Reorder days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = heatmap_data.reindex([day for day in day_order if day in heatmap_data.index])
    
    # Create heatmap
    fig = px.imshow(heatmap_data, 
                    title=f"Heatmap: {column_name.replace('_', ' ').title()} by Day and Hour",
                    labels=dict(x="Hour of Day", y="Day of Week", color="Value"),
                    aspect="auto")
    
    st.plotly_chart(fig, use_container_width=True)

def create_distribution_chart(data, selected_ncu, selected_project, column_name):
    """Create distribution chart showing value frequency"""
    
    chart_data = data[(data['ncu'] == selected_ncu) & (data['project'] == selected_project)]
    
    if chart_data.empty:
        st.warning("No data available for selected NCU.")
        return
    
    # Create histogram
    fig = px.histogram(chart_data, x=column_name, 
                      title=f"Distribution: {column_name.replace('_', ' ').title()}",
                      labels={column_name: column_name.replace('_', ' ').title()})
    
    st.plotly_chart(fig, use_container_width=True)

def create_correlation_chart(data, selected_ncu, selected_project, column_name):
    """Create correlation chart with other parameters"""
    
    chart_data = data[(data['ncu'] == selected_ncu) & (data['project'] == selected_project)]
    
    if chart_data.empty:
        st.warning("No data available for selected NCU.")
        return
    
    # Select numeric columns for correlation
    numeric_cols = ['alarm', 'ok_status', 'battery_alarm', 'battery_warning', 
                   'warning_count', 'communication_error', 'master_mode', 
                   'manual_mode', 'max_wind_speed', 'avg_wind_speed']
    
    correlation_data = chart_data[numeric_cols].corr()
    
    # Create correlation heatmap
    fig = px.imshow(correlation_data, 
                    title="Parameter Correlation Matrix",
                    labels=dict(color="Correlation"),
                    aspect="auto")
    
    st.plotly_chart(fig, use_container_width=True)

def create_additional_analysis(data, selected_ncu, selected_project, column_name):
    """Create additional analysis section"""
    
    chart_data = data[(data['ncu'] == selected_ncu) & (data['project'] == selected_project)]
    
    if chart_data.empty:
        return
    
    st.subheader("🔍 Additional Analysis")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Statistics
        st.markdown("**📊 Statistics:**")
        stats = chart_data[column_name].describe()
        for stat, value in stats.items():
            st.write(f"• {stat.title()}: {value:.2f}")
    
    with col2:
        # Peak hours analysis - show top 6 hours
        chart_data['hour'] = chart_data['date_time'].dt.hour
        hourly_avg = chart_data.groupby('hour')[column_name].mean().sort_values(ascending=False)
        
        st.markdown("**🕐 Top 6 Peak Hours:**")
        for hour, value in hourly_avg.head(6).items():
            st.write(f"• {hour:02d}:00 - Avg: {value:.2f}")
    
    with col3:
        # Daily patterns - show all available days
        chart_data['weekday'] = chart_data['date_time'].dt.day_name()
        daily_avg = chart_data.groupby('weekday')[column_name].mean().sort_values(ascending=False)
        
        st.markdown("**📅 Daily Patterns:**")
        for day, value in daily_avg.items():
            st.write(f"• {day} - Avg: {value:.2f}")

def create_trend_analysis(data, analysis_type):
    """Create trend analysis for all parameters"""
    st.subheader("📈 Comprehensive Trend Analysis")
    
    # Parameters to analyze
    params = ['alarm', 'ok_status', 'battery_alarm', 'battery_warning',
              'warning_count', 'communication_error', 'master_mode',
              'manual_mode', 'max_wind_speed', 'avg_wind_speed']
    
    if analysis_type == "Project-wise":
        # Group by project and date
        data['date'] = data['date_time'].dt.date
        trend_data = data.groupby(['project', 'date'])[params].max().reset_index()
        
        # Select project
        selected_project = st.selectbox("Select Project for Trend Analysis",
                                      options=sorted(data['project'].unique()))
        project_data = trend_data[trend_data['project'] == selected_project]
        
        # Create trend charts
        for param in params:
            fig = px.line(project_data, x='date', y=param,
                         title=f"Trend: {param.replace('_', ' ').title()} - {selected_project}",
                         hover_data={'date': True, param: True})
            
            # Update layout for better hover experience
            fig.update_layout(
                hovermode='x unified',  # Show tooltip for all traces at same x position
                hoverlabel=dict(
                    bgcolor="gray",
                    bordercolor="black",
                    font_size=12,
                    font_family="Arial"
                )
            )
            
            # Update traces for enhanced hover
            fig.update_traces(
                mode='lines+markers',
                line=dict(width=2),
                marker=dict(size=6),
                hovertemplate='<b>Date:</b> %{x}<br>' +
                             f'<b>{param.replace("_", " ").title()}:</b> %{{y}}<br>' +
                             '<extra></extra>'  # Removes the trace box
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
    else:
        # NCU-wise analysis
        selected_ncu = st.selectbox("Select NCU for Trend Analysis",
                                  options=sorted(data['ncu'].unique()))
        ncu_data = data[data['ncu'] == selected_ncu]
        ncu_data['date'] = ncu_data['date_time'].dt.date
        trend_data = ncu_data.groupby('date')[params].max().reset_index()
        
        # Create trend charts
        for param in params:
            fig = px.line(trend_data, x='date', y=param,
                         title=f"Trend: {param.replace('_', ' ').title()} - {selected_ncu}",
                         hover_data={'date': True, param: True})
            
            # Update layout for better hover experience
            fig.update_layout(
                hovermode='x unified',  # Show tooltip for all traces at same x position
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="black",
                    font_size=12,
                    font_family="Arial"
                )
            )
            
            # Update traces for enhanced hover
            fig.update_traces(
                mode='lines+markers',
                line=dict(width=2),
                marker=dict(size=6),
                hovertemplate='<b>Date:</b> %{x}<br>' +
                             f'<b>{param.replace("_", " ").title()}:</b> %{{y}}<br>' +
                             '<extra></extra>'  # Removes the trace box
            )
            
            st.plotly_chart(fig, use_container_width=True)

def create_status_change_analysis(data, analysis_type):
    """Create status change analysis"""
    
    st.subheader("🔄 Status Change Analysis")
    
    # Calculate status changes
    status_cols = ['alarm', 'ok_status', 'battery_alarm', 'battery_warning', 
                   'communication_error', 'master_mode', 'manual_mode']
    
    if analysis_type == "Project-wise":
        selected_project = st.selectbox("Select Project for Status Change Analysis", 
                                       options=sorted(data['project'].unique()))
        
        project_data = data[data['project'] == selected_project].sort_values('date_time')
        
        # Calculate changes for each NCU
        change_summary = []
        for ncu in project_data['ncu'].unique():
            ncu_data = project_data[project_data['ncu'] == ncu].sort_values('date_time')
            
            for col in status_cols:
                changes = (ncu_data[col].diff() != 0).sum()
                change_summary.append({
                    'NCU': ncu,
                    'Parameter': col,
                    'Changes': changes
                })
        
        change_df = pd.DataFrame(change_summary)
        
        # Create pivot table for heatmap
        change_pivot = change_df.pivot(index='NCU', columns='Parameter', values='Changes').fillna(0)
        
        fig = px.imshow(change_pivot, 
                       title=f"Status Changes Heatmap - {selected_project}",
                       labels=dict(color="Number of Changes"))
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show summary table
        st.subheader("📋 Change Summary")
        st.dataframe(change_df.pivot_table(index='NCU', columns='Parameter', values='Changes', fill_value=0))
    
    else:
        selected_ncu = st.selectbox("Select NCU for Status Change Analysis", 
                                   options=sorted(data['ncu'].unique()))
        
        ncu_data = data[data['ncu'] == selected_ncu].sort_values('date_time')
        
        # Calculate changes over time
        change_data = []
        for col in status_cols:
            changes = ncu_data[col].diff().fillna(0)
            change_points = ncu_data[changes != 0]
            
            for _, row in change_points.iterrows():
                change_data.append({
                    'DateTime': row['date_time'],
                    'Parameter': col,
                    'Change': changes[row.name],
                    'New_Value': row[col]
                })
        
        if change_data:
            change_df = pd.DataFrame(change_data)
            
            # Create timeline chart
            fig = px.scatter(change_df, x='DateTime', y='Parameter', 
                           color='Change', size='New_Value',
                           title=f"Status Change Timeline - {selected_ncu}")
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show change table
            st.subheader("📋 Recent Changes")
            st.dataframe(change_df.sort_values('DateTime', ascending=False).head(20))
        else:
            st.info("No status changes detected for the selected NCU in the given time range.")

# Export function for main app
def create_deep_analysis_dashboard():
    """Main function to create the deep analysis dashboard"""
    create_deep_analysis_page()