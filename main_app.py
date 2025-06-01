import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from database_manager import DatabaseManager

# Import your existing functions
from collection_stats import create_collection_stats_page  # Assuming your first file is saved as paste.py
from realtime_dashboard import create_realtime_dashboard    # Assuming your second file is saved as paste2.py

def main():
    """Main application with navigation"""
    
    # Configure page
    st.set_page_config(
        page_title="NCU Monitoring System",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Sidebar navigation
    st.sidebar.title("🎛️ Navigation")
    
    # Page selection
    page = st.sidebar.selectbox(
        "Select Page",
        ["🔴 Real-time Dashboard", "📊 Collection Statistics"],
        index=0
    )
    
    # Add some info in sidebar
    st.sidebar.markdown("---")
    st.sidebar.info(f"🕐 Current Time: {datetime.now().strftime('%H:%M:%S')}")
    
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

if __name__ == "__main__":
    main()