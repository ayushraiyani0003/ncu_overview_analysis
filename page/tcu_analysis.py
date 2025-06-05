# tcu_analysis.py
import random
import string
import mysql.connector
import pandas as pd
import streamlit as st
from collections import defaultdict
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import hashlib

def generate_unique_key(*args):
    """Generate a unique key based on provided arguments"""
    # Convert all args to string and create a hash
    key_string = "_".join(str(arg) for arg in args)
    # Add timestamp for extra uniqueness
    key_string += str(datetime.now().timestamp())
    # Create hash and return first 12 characters
    return hashlib.md5(key_string.encode()).hexdigest()[:12]

def get_tcu_status_analysis(selected_ncu, selected_project, date_start=None, date_end=None):
    """
    Get TCU status analysis from database
    
    Args:
        selected_ncu (str): The NCU to filter by
        selected_project (str): The project name to filter by
        date_start (str): Start date in 'YYYY-MM-DD' format (optional)
        date_end (str): End date in 'YYYY-MM-DD' format (optional)
    
    Returns:
        tuple: (status_analysis_dict, raw_dataframe)
    """
    
    # Database connection configuration
    # Try to get from Streamlit secrets, fallback to environment or defaults
    try:
        db_config = {
            'host': st.secrets.get("DB_HOST", "localhost"),
            'user': st.secrets.get("DB_USER", "root"),
            'password': st.secrets.get("DB_PASSWORD", ""),
            'database': st.secrets.get("DB_NAME", "ncu_overview")
        }
    except:
        # Fallback configuration if secrets not available
        db_config = {
            'host': "localhost",
            'user': "root", 
            'password': "",
            'database': "ncu_overview"
        }
    
    try:
        connection = mysql.connector.connect(**db_config)
        
        # Build the base query
        base_query = """
        SELECT DISTINCT status_name, tcu_rows, alarm, manual_mode, 
               actual_angle, target_angle, wind_speed, created_at
        FROM tcu_overview 
        WHERE ncu = %s AND project_name = %s
        """
        
        params = [selected_ncu, selected_project]
        
        # Add date filtering if provided
        if date_start and date_end:
            base_query += " AND created_at BETWEEN %s AND %s"
            params.extend([date_start, date_end])
        
        base_query += " AND status_name IS NOT NULL AND tcu_rows IS NOT NULL"
        base_query += " ORDER BY status_name, tcu_rows"
        
        # Execute query and get results as DataFrame
        df = pd.read_sql(base_query, connection, params=params)
        connection.close()
        
        if df.empty:
            return {}, pd.DataFrame()
        
        # Process results into status analysis
        status_analysis = defaultdict(list)
        
        for _, row in df.iterrows():
            status = row['status_name']
            tcu_row = row['tcu_rows']
            if tcu_row not in status_analysis[status]:
                status_analysis[status].append(int(tcu_row))
        
        # Sort tcu_rows for each status
        for status in status_analysis:
            status_analysis[status].sort()
        
        return dict(status_analysis), df
        
    except mysql.connector.Error as e:
        st.error(f"Database connection error: {e}")
        return {}, pd.DataFrame()
    except Exception as e:
        st.error(f"Error: {e}")
        return {}, pd.DataFrame()

def get_tcu_detailed_stats(df):
    """Get detailed statistics from TCU dataframe"""
    if df.empty:
        return {}
    
    stats = {
        'total_records': len(df),
        'unique_tcu_rows': df['tcu_rows'].nunique(),
        'status_distribution': df['status_name'].value_counts().to_dict(),
        'avg_actual_angle': df['actual_angle'].mean() if df['actual_angle'].notna().any() else 0,  
        'avg_target_angle': df['target_angle'].mean() if df['target_angle'].notna().any() else 0,
        'avg_wind_speed': df['wind_speed'].mean() if df['wind_speed'].notna().any() else 0,
        'alarm_count': df['alarm'].notna().sum(),
        'manual_mode_count': df['manual_mode'].notna().sum(),
        'alarm_total_count': df['status_name'].str.contains('alarm').sum(),
        'battery_alarm_total_count': df['status_name'].str.contains('battery_warning').sum(),
        'iot_mode_total_count': df['status_name'].str.contains('iot_mode').sum(),
        'warning_total_count': df['status_name'].str.contains('warning').sum(),
    }
    
    return stats

def display_tcu_overview(selected_ncu, selected_project, date_start=None, date_end=None):
    """
    Display TCU overview analysis in Streamlit
    """
    # Generate unique base key for this function call
    base_key = generate_unique_key("tcu_overview", selected_ncu, selected_project, date_start, date_end)
    
    st.subheader("🔧 TCU Overview Analysis")
    
    # Get TCU analysis data
    with st.spinner("Loading TCU data..."):
        status_analysis, raw_df = get_tcu_status_analysis(
            selected_ncu, selected_project, date_start, date_end
        )
    
    if not status_analysis:
        st.warning(f"No TCU data found for NCU: {selected_ncu}, Project: {selected_project}")
        
        # Show available data for debugging
        with st.expander("🔍 Debug Info"):
            st.write(f"Selected NCU: {selected_ncu}")
            st.write(f"Selected Project: {selected_project}")
            if date_start and date_end:
                st.write(f"Date Range: {date_start} to {date_end}")
        return
    
    # Display date range info
    if date_start and date_end:
        st.info(f"📅 Analysis Period: {date_start} to {date_end}")
    
    # Create columns for layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Status Analysis Section
        st.markdown("### 📊 TCU Rows by Status")
        
        # Create a formatted display of status analysis
        status_df_data = []
        for status, tcu_rows in status_analysis.items():
            tcu_rows_str = ', '.join(map(str, tcu_rows))
            count = len(tcu_rows)
            status_df_data.append({
                'Status': status,
                'TCU Rows': tcu_rows_str,
                'Count': count
            })
        
        status_df = pd.DataFrame(status_df_data)
        
        # Display as a styled table
        st.dataframe(
            status_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Status distribution chart
        if len(status_analysis) > 1:
            st.markdown("### 📈 Status Distribution")
            
            # Create pie chart using plotly
            chart_data = pd.DataFrame([
                {'Status': status, 'Count': len(tcu_rows)} 
                for status, tcu_rows in status_analysis.items()
            ])
            
            fig = px.pie(chart_data, values='Count', names='Status', 
                        title='TCU Status Distribution')
            fig.update_traces(textposition='inside', textinfo='percent+label')
            
            # Add unique key to avoid duplicate element ID error
            chart_key = f"tcu_status_pie_{base_key}"
            st.plotly_chart(fig, use_container_width=True, key=chart_key)
    
    with col2:
        # Summary Statistics
        st.markdown("### 📋 Summary")
        
        stats = get_tcu_detailed_stats(raw_df)
        
        if stats:
            st.metric("Total Records", stats['total_records'])
            st.metric("Unique TCU Rows", stats['unique_tcu_rows'])
            st.metric("Different Statuses", len(status_analysis))
            
            if stats['avg_wind_speed'] > 0:
                st.metric("Avg Wind Speed", f"{stats['avg_wind_speed']:.1f} m/s")
            
            if stats['avg_actual_angle'] > 0:
                st.metric("Avg Actual Angle", f"{stats['avg_actual_angle']:.1f}°")
            
            if stats['alarm_total_count'] > 0:
                # amke a 4 column in this section
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Alarm Count", stats['alarm_total_count'])
                
                with col2:
                    st.metric("Iot Mode Count", stats['iot_mode_total_count'])
                
                with col3:
                    st.metric("Battery Alarm Count", stats['battery_alarm_total_count'])
                
                with col4:
                    st.metric("Warning Count", stats['warning_total_count'])
    
    # Detailed Data Section (Expandable)
    with st.expander("🔍 View Detailed TCU Data"):
        if not raw_df.empty:
            # Format the dataframe for better display
            display_df = raw_df.copy()
            
            # Format numeric columns
            numeric_cols = ['actual_angle', 'target_angle', 'wind_speed']
            for col in numeric_cols:
                if col in display_df.columns and display_df[col].notna().any():
                    display_df[col] = display_df[col].round(2)
            
            # Format datetime
            if 'created_at' in display_df.columns:
                display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Download button for the data
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download TCU Data as CSV",
                data=csv,
                file_name=f"tcu_analysis_{selected_ncu}_{selected_project}.csv",
                mime="text/csv",
                key=f"download_button_{base_key}"
            )
        else:
            st.info("No detailed data available")

# Simple test function for debugging
def test_tcu_connection():
    """Test database connection"""
    try:
        db_config = {
            'host': st.secrets.get("DB_HOST", "localhost"),
            'user': st.secrets.get("DB_USER", "root"),
            'password': st.secrets.get("DB_PASSWORD", ""),
            'database': st.secrets.get("DB_NAME", "ncu_overview")
        }
        
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM tcu_overview LIMIT 1")
        result = cursor.fetchone()
        connection.close()
        
        st.success(f"Database connection successful! Found {result[0]} records in tcu_overview table.")
        return True
        
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return False