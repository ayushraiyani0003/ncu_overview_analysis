import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector
from mysql.connector import Error
import json
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import threading
import schedule
from typing import Dict, List
import numpy as np

# v1.1
# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ncu_overview',
    'user': 'root',
    'password': ''
}

# Login Configuration
LOGIN_CONFIG = {
    'login_url': "http://app.sunchaser.cloud/login",
    'email': "admin@gmail.com",
    'password': "Admin@123",
    'target_url': "http://app.sunchaser.cloud/admin/tcu-overview"
}

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.create_connection()
        self.create_tables()
    
    def create_connection(self):
        """Create database connection"""
        try:
            self.connection = mysql.connector.connect(**DB_CONFIG)
            if self.connection.is_connected():
                print("Successfully connected to MySQL database")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            # Fallback to SQLite for demo purposes
            import sqlite3
            self.connection = sqlite3.connect('ncu_data.db', check_same_thread=False)
            print("Using SQLite database as fallback")
    
    def create_tables(self):
        """Create tables if not exists"""
        try:
            cursor = self.connection.cursor()
            
            # Main NCU data table
            create_ncu_table_query = """
            CREATE TABLE IF NOT EXISTS ncu_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp BIGINT,
                date_time DATETIME,
                project VARCHAR(255),
                ncu VARCHAR(255),
                user_id INT,
                ncu_id INT,
                alarm INT,
                battery_alarm INT,
                battery_warning INT,
                warning_count INT,
                master_mode INT,
                manual_mode INT,
                ok_status INT,
                communication_error INT,
                inactive_tcu INT,
                max_wind_speed DECIMAL(10,2),
                avg_wind_speed DECIMAL(10,2),
                raw_data TEXT,
                INDEX idx_timestamp (timestamp),
                INDEX idx_project_ncu (project, ncu),
                INDEX idx_datetime (date_time)
            )
            """
            
            # Data collection tracking table
            create_tracking_table_query = """
            CREATE TABLE IF NOT EXISTS data_collection_tracking (
                id INT AUTO_INCREMENT PRIMARY KEY,
                collection_time DATETIME,
                timestamp BIGINT,
                records_collected INT,
                records_inserted INT,
                excluded_records INT,
                success BOOLEAN,
                error_message TEXT,
                INDEX idx_collection_time (collection_time)
            )
            """
            
            cursor.execute(create_ncu_table_query)
            cursor.execute(create_tracking_table_query)
            self.connection.commit()
            print("Tables created successfully")
            
        except Error as e:
            print(f"Error creating tables: {e}")
    
    def insert_data(self, data_list: List[Dict], timestamp: int):
        """Insert data into database, excluding AAA project"""
        try:
            cursor = self.connection.cursor()
            date_time = datetime.fromtimestamp(timestamp / 1000)
            
            # Filter out AAA project data
            filtered_data = [item for item in data_list 
                           if item.get('project', {}).get('value', '') != 'AAA']
            
            excluded_count = len(data_list) - len(filtered_data)
            inserted_count = 0
            
            for item in filtered_data:
                insert_query = """
                INSERT INTO ncu_data 
                (timestamp, date_time, project, ncu, user_id, ncu_id, alarm, 
                 battery_alarm, battery_warning, warning_count, master_mode, 
                 manual_mode, ok_status, communication_error, inactive_tcu, 
                 max_wind_speed, avg_wind_speed, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    timestamp, date_time, 
                    item.get('project', {}).get('value', ''),
                    item.get('ncu', {}).get('value', ''),
                    item.get('user_id', {}).get('value', 0),
                    item.get('ncu_id', {}).get('value', 0),
                    item.get('alarm', {}).get('value', 0),
                    item.get('batteryAlarm', {}).get('value', 0),
                    item.get('batteryWarning', {}).get('value', 0),
                    item.get('warning', {}).get('value', 0),
                    item.get('masterMode', {}).get('value', 0),
                    item.get('manualMode', {}).get('value', 0),
                    item.get('okStatus', {}).get('value', 0),
                    item.get('communicationError', {}).get('value', 0),
                    item.get('inactvieTCU', {}).get('value', 0),
                    item.get('maxWindSpeed', {}).get('value', 0.0),
                    item.get('avgWindSpeed', {}).get('value', 0.0),
                    json.dumps(item)
                )
                
                cursor.execute(insert_query, values)
                inserted_count += 1
            
            # Insert tracking record
            tracking_query = """
            INSERT INTO data_collection_tracking 
            (collection_time, timestamp, records_collected, records_inserted, excluded_records, success)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(tracking_query, (
                date_time, timestamp, len(data_list), inserted_count, excluded_count, True
            ))
            
            self.connection.commit()
            print(f"Inserted {inserted_count} records successfully (excluded {excluded_count} AAA records)")
            return True
            
        except Error as e:
            print(f"Error inserting data: {e}")
            # Insert error tracking record
            try:
                error_tracking_query = """
                INSERT INTO data_collection_tracking 
                (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(error_tracking_query, (
                    datetime.now(), timestamp, len(data_list), 0, 0, False, str(e)
                ))
                self.connection.commit()
            except:
                pass
            return False
    
    def ensure_connection(self):
        """Ensure database connection is active with better error handling"""
        try:
            if hasattr(self.connection, 'is_connected'):
                # MySQL connection
                if not self.connection.is_connected():
                    print("MySQL connection lost, reconnecting...")
                    self.create_connection()
                else:
                    # Test the connection
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
            else:
                # SQLite connection
                try:
                    self.connection.execute("SELECT 1")
                except:
                    print("SQLite connection lost, reconnecting...")
                    self.create_connection()
        except Exception as e:
            print(f"Connection check failed: {e}")
            self.create_connection()
    
    def get_latest_data(self):
        """Get latest data for real-time view with better error handling"""
        try:
            # Always ensure fresh connection
            self.ensure_connection()
            cursor = self.connection.cursor()
            
            # First check if we have any data
            cursor.execute("SELECT COUNT(*) FROM ncu_data WHERE project != 'AAA'")
            count = cursor.fetchone()[0]
            
            if count == 0:
                print("No data found in database")
                return pd.DataFrame()
            
            query = """
            SELECT * FROM ncu_data 
            WHERE timestamp = (SELECT MAX(timestamp) FROM ncu_data WHERE project != 'AAA')
            AND project != 'AAA'
            ORDER BY project, ncu
            """
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            if data:
                df = pd.DataFrame(data, columns=columns)
                print(f"Retrieved {len(df)} records from database")
                return df
            else:
                print("No matching data found")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error fetching latest data: {e}")
            return pd.DataFrame()
    
    def get_collection_stats(self):
        """Get data collection statistics"""
        try:
            cursor = self.connection.cursor()
            
            # Get total collections count
            cursor.execute("SELECT COUNT(*) as total_collections FROM data_collection_tracking WHERE success = 1")
            result = cursor.fetchone()
            total_collections = result[0] if result else 0
            
            # Get collections in last 24 hours
            cursor.execute("""
                SELECT COUNT(*) as recent_collections 
                FROM data_collection_tracking 
                WHERE success = 1 AND collection_time >= %s
            """, (datetime.now() - timedelta(hours=24),))
            result = cursor.fetchone()
            recent_collections = result[0] if result else 0
            
            # Get last collection time
            cursor.execute("""
                SELECT collection_time, records_inserted, excluded_records 
                FROM data_collection_tracking 
                WHERE success = 1 
                ORDER BY collection_time DESC 
                LIMIT 1
            """)
            last_collection = cursor.fetchone()
            
            # Get recent collection history
            cursor.execute("""
                SELECT collection_time, records_inserted, excluded_records, success
                FROM data_collection_tracking 
                ORDER BY collection_time DESC 
                LIMIT 10
            """)
            recent_history = cursor.fetchall()
            
            return {
                'total_collections': total_collections,
                'recent_collections': recent_collections,
                'last_collection': last_collection,
                'recent_history': recent_history
            }
            
        except Error as e:
            print(f"Error fetching collection stats: {e}")
            return {
                'total_collections': 0,
                'recent_collections': 0,
                'last_collection': None,
                'recent_history': []
            }
        
    def get_historical_data(self, ncu_name: str, days: int = 7):
        """Get historical data for specific NCU (excluding AAA project)"""
        try:
            cursor = self.connection.cursor()
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            query = """
            SELECT * FROM ncu_data 
            WHERE ncu = %s AND date_time >= %s AND date_time <= %s
            AND project != 'AAA'
            ORDER BY date_time
            """
            cursor.execute(query, (ncu_name, start_date, end_date))
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Error as e:
            print(f"Error fetching historical data: {e}")
            return pd.DataFrame()

class DataCollector:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.session = requests.Session()
        self.is_logged_in = False
        self.last_login_time = None
    
    def login(self):
        """Login to the system"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            
            # Get login page for CSRF token
            login_page = self.session.get(LOGIN_CONFIG['login_url'], headers=headers)
            soup = BeautifulSoup(login_page.text, 'html.parser')
            
            csrf_token = None
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            if csrf_meta:
                csrf_token = csrf_meta.get('content')
            
            # Prepare login data
            login_data = {
                'email': LOGIN_CONFIG['email'],
                'password': LOGIN_CONFIG['password'],
            }
            
            if csrf_token:
                login_data['_token'] = csrf_token
            
            # Update headers for POST
            post_headers = headers.copy()
            post_headers.update({
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': LOGIN_CONFIG['login_url'],
                'Origin': 'http://app.sunchaser.cloud'
            })
            
            if csrf_token:
                post_headers['X-CSRF-TOKEN'] = csrf_token
            
            # Perform login
            login_response = self.session.post(
                LOGIN_CONFIG['login_url'], 
                data=login_data, 
                headers=post_headers, 
                allow_redirects=True
            )
            
            if login_response.status_code == 200:
                self.is_logged_in = True
                self.last_login_time = datetime.now()
                print("Login successful")
                return True
            else:
                print(f"Login failed: {login_response.status_code}")
                return False
                
        except Exception as e:
            print(f"Login error: {e}")
            return False
    
    def logout(self):
        """Logout from the system"""
        try:
            logout_url = "http://app.sunchaser.cloud/logout"
            self.session.get(logout_url)
            self.is_logged_in = False
            print("Logged out successfully")
        except Exception as e:
            print(f"Logout error: {e}")
    
    def collect_data(self):
        """Collect data from the API"""
        print("Collecting data...")
        try:
            # Check if need to re-login (every hour)
            if (not self.is_logged_in or 
                (self.last_login_time and 
                 datetime.now() - self.last_login_time > timedelta(hours=1))):
                self.logout()
                if not self.login():
                    return False
            
            # Generate timestamp
            timestamp = int(time.time() * 1000)
            ajax_url = f"{LOGIN_CONFIG['target_url']}?_={timestamp}"
            
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': LOGIN_CONFIG['target_url']
            }
            
            response = self.session.get(ajax_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and isinstance(data['data'], list):
                    success = self.db_manager.insert_data(data['data'], timestamp)
                    if success:
                        print(f"Data collected successfully at {datetime.now()}")
                        return True
            
            return False
            
        except Exception as e:
            print(f"Data collection error: {e}")
            return False

# Initialize database and data collector - REMOVED CACHING
def get_database_manager():
    """Get database manager without caching"""
    return DatabaseManager()

def get_data_collector():
    db_manager = get_database_manager()
    return DataCollector(db_manager)

# REMOVED ALL CACHING DECORATORS
def get_latest_data():
    """Get latest data without caching"""
    print("Getting fresh latest data...")
    db_manager = get_database_manager()
    db_manager.ensure_connection()
    data = db_manager.get_latest_data()
    print(f"Got fresh latest data: {len(data)} records")
    return data

def get_stats():
    """Get collection statistics without caching"""
    print("Getting fresh stats...")
    db_manager = get_database_manager()
    db_manager.ensure_connection()
    stats = db_manager.get_collection_stats()
    print("Got fresh stats")
    return stats

def create_realtime_dashboard():
    """Create real-time dashboard page with fresh data on every load"""
    # Add refresh button and auto-refresh checkbox
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button("🔄 Refresh Data", key="refresh_btn"):
            st.rerun()
    
    with col2:
        auto_refresh = st.checkbox("Auto Refresh", value=False)
    
    with col3:
        if auto_refresh:
            refresh_interval = st.selectbox("Interval (sec)", [10, 30, 60], index=1)
            # JavaScript auto-refresh
            st.markdown(f"""
            <script>
            setTimeout(function() {{
                window.location.reload();
            }}, {refresh_interval * 1000});
            </script>
            """, unsafe_allow_html=True)
    
    # Get fresh data every time
    latest_data = get_latest_data()
    stats = get_stats()
    
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
                # Parse string datetime
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
    
    # Show current time
    st.info(f"🕐 Dashboard loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not latest_data.empty:
        # Display timestamp
        latest_timestamp = latest_data['date_time'].iloc[0]
        if isinstance(latest_timestamp, str):
            try:
                latest_timestamp = pd.to_datetime(latest_timestamp)
            except:
                latest_timestamp = datetime.now()
        
        st.info(f"📅 Data as of: {latest_timestamp}")
        
        # Summary metrics - All 11 items in specified order
        st.subheader("🎯 System Overview")
        
        # Calculate totals/values for all 11 metrics
        total_alarms = latest_data['alarm'].sum()
        total_battery_alarms = latest_data['battery_alarm'].sum()
        total_battery_warnings = latest_data['battery_warning'].sum()
        total_warnings = latest_data['warning_count'].sum()
        total_ok = latest_data['ok_status'].sum()
        total_comm_errors = latest_data['communication_error'].sum()
        total_master_mode = latest_data['master_mode'].sum()
        total_manual_mode = latest_data['manual_mode'].sum()
        total_inactive = latest_data['inactive_tcu'].sum()
        max_wind = latest_data['max_wind_speed'].max()
        avg_wind = latest_data['avg_wind_speed'].mean()
        
        # Create 11 columns for all 11 metrics
        col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11 = st.columns(11)
        
        # Display metrics in the specified order
        col1.metric("🚨 Alarms", total_alarms, delta=None if total_alarms == 0 else f"+{total_alarms}")
        col2.metric("🪫 Batt Alarms", total_battery_alarms, delta=None if total_battery_alarms == 0 else f"+{total_battery_alarms}")
        col3.metric("⚡ Batt Warnings", total_battery_warnings, delta=None if total_battery_warnings == 0 else f"+{total_battery_warnings}")
        col4.metric("⚠️ Warnings", total_warnings, delta=None if total_warnings == 0 else f"+{total_warnings}")
        col5.metric("✅ OK Status", total_ok)
        col6.metric("📡 Comm Errors", total_comm_errors, delta=None if total_comm_errors == 0 else f"+{total_comm_errors}")
        col7.metric("🤖 Master Mode", total_master_mode)
        col8.metric("👤 Manual Mode", total_manual_mode)
        col9.metric("⛔ Inactive", total_inactive, delta=None if total_inactive == 0 else f"+{total_inactive}")
        col10.metric("💨 Max Wind", f"{max_wind:.1f} m/s")
        col11.metric("🌬️ Avg Wind", f"{avg_wind:.1f} m/s")

        # Project-wise summary
        st.subheader("📊 Project-wise Status")

        # Aggregate data by project
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
            f"<div style='font-size:16px;'>"
            f"<b>📂 Project:</b> {row['project']}<br>"
            f"🚨 <b>Alarms:</b> {row['alarm']}<br>"
            f"⚠️ <b>Warnings:</b> {row['warning_count']}<br>"
            f"🔋 <b>Battery Alarm:</b> {row['battery_alarm']}<br>"
            f"✅ <b>OK Status:</b> {row['ok_status']}<br>"
            f"📡 <b>Comm Errors:</b> {row['communication_error']}<br>"
            f"🔕 <b>Inactive:</b> {row['inactive_tcu']}<br>"
            f"</div>"
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
                
                # 6 Values in Large Text - Single Row
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
       # Recent collection history
        with st.expander("📋 Recent Data Collections"):
            if stats['recent_history']:
                history_df = pd.DataFrame(stats['recent_history'], 
                                        columns=['Time', 'Records', 'Excluded', 'Success'])
                history_df['Status'] = history_df['Success'].apply(lambda x: '✅' if x else '❌')
                st.dataframe(history_df[['Time', 'Records', 'Excluded', 'Status']], 
                           use_container_width=True)
            else:
                st.info("No collection history available")
        
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

def create_historical_analysis():
    """Create 7-day historical analysis page"""
    st.title("📈 Historical Analysis")
    
    db_manager = get_database_manager()
    
    # Get available NCUs (excluding AAA)
    latest_data = db_manager.get_latest_data()
    if latest_data.empty:
        st.warning("⚠️ No data available for analysis.")
        return
    
    available_ncus = latest_data['ncu'].unique()
    
    # NCU selection
    selected_ncu = st.selectbox("🏢 Select NCU for Analysis", available_ncus)
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        days_back = st.selectbox("📅 Analysis Period", [1, 3, 7, 14, 30], index=2)
    with col2:
        time_interval = st.selectbox("⏱️ Time Interval", ["30min", "1hour", "2hour", "4hour"], index=1)
    
    if selected_ncu:
        # Get historical data
        historical_data = db_manager.get_historical_data(selected_ncu, days_back)
        
        if not historical_data.empty:
            # Process data for time-based analysis
            historical_data['date_time'] = pd.to_datetime(historical_data['date_time'])
            historical_data['hour'] = historical_data['date_time'].dt.hour
            historical_data['minute'] = historical_data['date_time'].dt.minute
            historical_data['date'] = historical_data['date_time'].dt.date
            
            # Summary metrics
            st.subheader(f"📊 {selected_ncu} - {days_back} Days Analysis")
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("📈 Total Alarms", int(historical_data['alarm'].sum()))
            col2.metric("📈 Total Warnings", int(historical_data['warning_count'].sum()))
            col3.metric("📈 Avg OK Status", f"{historical_data['ok_status'].mean():.1f}")
            col4.metric("💨 Max Wind", f"{historical_data['max_wind_speed'].max():.1f}")
            
            # Time series analysis
            st.subheader("📈 Time Series Trends")
            
            # Create subplot
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Alarms Over Time', 'Wind Speed Trends', 
                              'Status Distribution', 'Daily Patterns'),
                specs=[[{"secondary_y": False}, {"secondary_y": True}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Alarms over time
            daily_alarms = historical_data.groupby('date')['alarm'].sum().reset_index()
            fig.add_trace(
                go.Scatter(x=daily_alarms['date'], y=daily_alarms['alarm'],
                          mode='lines+markers', name='Daily Alarms',
                          line=dict(color='red')),
                row=1, col=1
            )
            
            # Wind speed trends
            daily_wind = historical_data.groupby('date').agg({
                'max_wind_speed': 'max',
                'avg_wind_speed': 'mean'
            }).reset_index()
            
            fig.add_trace(
                go.Scatter(x=daily_wind['date'], y=daily_wind['max_wind_speed'],
                          mode='lines', name='Max Wind Speed',
                          line=dict(color='blue')),
                row=1, col=2
            )
            
            fig.add_trace(
                go.Scatter(x=daily_wind['date'], y=daily_wind['avg_wind_speed'],
                          mode='lines', name='Avg Wind Speed',
                          line=dict(color='lightblue')),
                row=1, col=2
            )
            
            # Status distribution
            status_avg = historical_data[['alarm', 'warning_count', 'ok_status', 'communication_error']].mean()
            fig.add_trace(
                go.Bar(x=status_avg.index, y=status_avg.values,
                      marker_color=['red', 'orange', 'green', 'purple']),
                row=2, col=1
            )
            
            # Hourly patterns
            hourly_pattern = historical_data.groupby('hour')['alarm'].mean().reset_index()
            fig.add_trace(
                go.Scatter(x=hourly_pattern['hour'], y=hourly_pattern['alarm'],
                          mode='lines+markers', name='Hourly Alarm Pattern',
                          line=dict(color='darkred')),
                row=2, col=2
            )
            
            fig.update_layout(height=800, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.warning(f"⚠️ No historical data found for {selected_ncu}")

def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="NCU Analytics Dashboard",
        page_icon="🏭",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Sidebar
    st.sidebar.title("🏭 NCU Analytics")
    st.sidebar.markdown("---")
    
    # Navigation
    page = st.sidebar.selectbox(
        "📍 Navigate to:",
        ["🔴 Real-time Dashboard", "📈 Historical Analysis", "⚙️ Data Collection"]
    )
    
    # Data collection status
    st.sidebar.markdown("### 📊 System Status")
    
    # Manual data collection button
    if st.sidebar.button("🔄 Collect Data Now"):
        data_collector = get_data_collector()
        if data_collector.collect_data():
            st.sidebar.success("✅ Data collected successfully!")
        else:
            st.sidebar.error("❌ Data collection failed!")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ About")
    st.sidebar.info(
        "This dashboard provides real-time monitoring and historical analysis "
        "of NCU (Network Control Unit) data including alarms, warnings, "
        "wind speeds, and operational status."
    )
    
    # Main content based on selected page
    if page == "🔴 Real-time Dashboard":
        create_realtime_dashboard()
    elif page == "📈 Historical Analysis":
        create_historical_analysis()
    elif page == "⚙️ Data Collection":
        st.title("⚙️ Data Collection Settings")
        
        st.subheader("🔧 Configuration")
        st.info("Data is automatically collected every 5 minutes with hourly re-authentication.")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📅 Collection Interval", "5 minutes")
            st.metric("🔐 Re-login Interval", "1 hour")
        
        with col2:
            db_manager = get_database_manager()
            latest_data = db_manager.get_latest_data()
            if not latest_data.empty:
                last_update = latest_data['date_time'].iloc[0]
                st.metric("🕐 Last Update", str(last_update))
                st.metric("📊 Records Count", len(latest_data))
            else:
                st.metric("🕐 Last Update", "No data")
                st.metric("📊 Records Count", "0")
        
        st.subheader("📋 Database Schema")
        st.code("""
        Table: ncu_data
        - id (Primary Key)
        - timestamp (BIGINT)
        - date_time (DATETIME)
        - project (VARCHAR)
        - ncu (VARCHAR)
        - alarm, battery_alarm, battery_warning (INT)
        - warning_count, master_mode, manual_mode (INT)
        - ok_status, communication_error, inactive_tcu (INT)
        - max_wind_speed, avg_wind_speed (DECIMAL)
        - raw_data (TEXT - JSON)
        """)

    def force_refresh_data():
        """Force refresh all cached data"""
        get_cached_latest_data.clear()
        get_cached_stats.clear()
        # Also ensure database connection is fresh
        db_manager = get_database_manager()
        db_manager.ensure_connection()

# Background data collection (run this separately or integrate with scheduler)
def setup_background_collection():
    """Setup background data collection"""
    print("Starting background data collection...")
    db_manager = DatabaseManager()
    data_collector = DataCollector(db_manager)
    
    # Schedule debug message every 2 minutes using lambda
    schedule.every(2).minutes.do(lambda: print("this is working you can focus in the other things"))
    
    # Schedule data collection every 5 minutes
    schedule.every(2).minutes.do(data_collector.collect_data)
    
    # Schedule re-login every hour
    schedule.every().hour.do(data_collector.login)
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute
        
if __name__ == "__main__":
    # Start background collection in a separate thread
    collection_thread = threading.Thread(target=setup_background_collection, daemon=True)
    collection_thread.start()
    
    # Run Streamlit app
    main()