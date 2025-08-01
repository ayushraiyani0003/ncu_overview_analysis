import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pytz
import numpy as np
from database_manager import DatabaseManager

# Import your existing functions
from page.collection_stats import create_collection_stats_page  # Assuming your first file is saved as paste.py
from page.realtime_dashboard import create_realtime_dashboard    # Assuming your second file is saved as paste2.py
from page.deep_analysis import create_deep_analysis_dashboard
def main():
    """Main application with navigation"""
    
    # Configure page
    st.set_page_config(
        page_title="NCU Monitoring System",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    
    # Sidebar navigation
    st.sidebar.title("🎛️ Navigation")
    
    # Page selection
    page = st.sidebar.selectbox(
        "Select Page",
        ["🔴 Real-time Dashboard", "📊 Collection Statistics",  "🔍 Deep Analysis"],
        index=0
    )
    
    # Add some info in sidebar
    st.sidebar.markdown("---")
    st.sidebar.info(f"🕐 Current Time: {datetime.now().strftime('%H:%M:%S')}")
    # Get current time in Indian Standard Time
    india_timezone = pytz.timezone('Asia/Kolkata')
    india_time = datetime.now(india_timezone)

    st.sidebar.info(f"🕐 Current Time (India): {india_time.strftime('%H:%M:%S')}")
    # Database connection status check
    try:
        db_manager = DatabaseManager()
        db_manager.ensure_connection()
        st.sidebar.success("✅ Database Connected")
    except Exception as e:
        st.sidebar.error(f"❌ Database Error: {str(e)[:50]}")
    
    # Route to appropriate page
    if page == "🔴 Real-time Dashboard":
        create_realtime_dashboard()
    elif page == "📊 Collection Statistics":
        create_collection_stats_page()
    elif page == "🔍 Deep Analysis":
        create_deep_analysis_dashboard()


if __name__ == "__main__":
    main()

#in this for navigation disp-ly three button, and this is current replace the realtime page data with other so i get lag, i need a pages so no contant replace i need page change.