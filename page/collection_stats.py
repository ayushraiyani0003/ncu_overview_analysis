import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from database_manager import DatabaseManager

def get_collection_stats():
    """Get detailed collection statistics"""
    db_manager = DatabaseManager()
    return db_manager.get_collection_stats()

def get_detailed_stats():
    """Get more detailed statistics from database"""
    try:
        db_manager = DatabaseManager()
        db_manager.ensure_connection()
        cursor = db_manager.connection.cursor()
        
        # Get hourly collection stats for last 7 days
        cursor.execute("""
            SELECT 
                DATE(collection_time) as date,
                HOUR(collection_time) as hour,
                COUNT(*) as collections,
                SUM(records_inserted) as total_records,
                AVG(records_inserted) as avg_records,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed
            FROM data_collection_tracking 
            WHERE collection_time >= %s
            GROUP BY DATE(collection_time), HOUR(collection_time)
            ORDER BY date DESC, hour DESC
            LIMIT 168
        """, (datetime.now() - timedelta(days=7),))
        
        hourly_stats = cursor.fetchall()
        
        # Get daily summary
        cursor.execute("""
            SELECT 
                DATE(collection_time) as date,
                COUNT(*) as total_collections,
                SUM(records_inserted) as total_records,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_collections,
                AVG(records_inserted) as avg_records_per_collection,
                MIN(collection_time) as first_collection,
                MAX(collection_time) as last_collection
            FROM data_collection_tracking 
            WHERE collection_time >= %s
            GROUP BY DATE(collection_time)
            ORDER BY date DESC
            LIMIT 30
        """, (datetime.now() - timedelta(days=30),))
        
        daily_stats = cursor.fetchall()
        
        # Get error analysis
        cursor.execute("""
            SELECT 
                DATE(collection_time) as date,
                error_message,
                COUNT(*) as error_count
            FROM data_collection_tracking 
            WHERE success = 0 AND collection_time >= %s
            GROUP BY DATE(collection_time), error_message
            ORDER BY date DESC, error_count DESC
        """, (datetime.now() - timedelta(days=7),))
        
        error_analysis = cursor.fetchall()
        
        return {
            'hourly_stats': hourly_stats,
            'daily_stats': daily_stats,
            'error_analysis': error_analysis
        }
        
    except Exception as e:
        print(f"Error fetching detailed stats: {e}")
        return {
            'hourly_stats': [],
            'daily_stats': [],
            'error_analysis': []
        }

def get_performance_metrics():
    """Get performance metrics for collections"""
    try:
        db_manager = DatabaseManager()
        db_manager.ensure_connection()
        cursor = db_manager.connection.cursor()
        
        # Get collection performance over time
        cursor.execute("""
            SELECT 
                collection_time,
                records_inserted,
                processing_time,
                success
            FROM data_collection_tracking 
            WHERE collection_time >= %s
            ORDER BY collection_time DESC
            LIMIT 1000
        """, (datetime.now() - timedelta(days=7),))
        
        performance_data = cursor.fetchall()
        return performance_data
        
    except Exception as e:
        print(f"Error fetching performance metrics: {e}")
        return []

def create_collection_stats_page():
    """Create collection statistics page"""
    st.title("📊 Data Collection Statistics")
    
    # Get basic stats
    basic_stats = get_collection_stats()
    detailed_stats = get_detailed_stats()
    
    # Current time
    st.info(f"🕐 Stats generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Basic metrics
    st.subheader("📈 Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Collections", basic_stats['total_collections'])
    with col2:
        st.metric("🕐 Last 24 Hours", basic_stats['recent_collections'])
    with col3:
        if basic_stats['last_collection']:
            last_time = basic_stats['last_collection'][0]
            if isinstance(last_time, str):
                try:
                    last_time = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                except:
                    last_time = datetime.now()
            st.metric("⏰ Last Collection", last_time.strftime("%H:%M:%S"))
        else:
            st.metric("⏰ Last Collection", "Never")
    with col4:
        if basic_stats['last_collection']:
            records = basic_stats['last_collection'][1]
            st.metric("📋 Last Records", records)
        else:
            st.metric("📋 Last Records", "0")
    
    # Daily statistics chart
    if detailed_stats['daily_stats']:
        st.subheader("📅 Daily Collection Trends (Last 30 Days)")
        
        daily_df = pd.DataFrame(detailed_stats['daily_stats'], 
                               columns=['Date', 'Total Collections', 'Total Records', 
                                       'Successful Collections', 'Avg Records', 
                                       'First Collection', 'Last Collection'])
        
        # Calculate success rate
        daily_df['Success Rate (%)'] = (daily_df['Successful Collections'] / daily_df['Total Collections'] * 100).round(1)
        
        # Create charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Collections per day
            fig1 = px.bar(daily_df.head(14), x='Date', y='Total Collections',
                         title='Daily Collections Count',
                         color='Success Rate (%)',
                         color_continuous_scale='RdYlGn')
            fig1.update_layout(height=400)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Records per day
            fig2 = px.line(daily_df.head(14), x='Date', y='Total Records',
                          title='Daily Records Collected',
                          markers=True)
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)
        
        # Success rate trend
        fig3 = px.line(daily_df.head(14), x='Date', y='Success Rate (%)',
                      title='Collection Success Rate Trend',
                      markers=True, line_shape='spline')
        fig3.update_layout(height=300)
        fig3.update_yaxis(range=[0, 100])
        st.plotly_chart(fig3, use_container_width=True)
    
    # Hourly pattern analysis
    if detailed_stats['hourly_stats']:
        st.subheader("🕐 Hourly Collection Pattern (Last 7 Days)")
        
        hourly_df = pd.DataFrame(detailed_stats['hourly_stats'],
                                columns=['Date', 'Hour', 'Collections', 'Total Records',
                                        'Avg Records', 'Successful', 'Failed'])
        
        # Create hourly heatmap
        if not hourly_df.empty:
            # Pivot for heatmap
            heatmap_data = hourly_df.pivot_table(values='Collections', 
                                                index='Date', 
                                                columns='Hour', 
                                                fill_value=0)
            
            fig_heatmap = px.imshow(heatmap_data, 
                                   title='Collections Heatmap (Hour vs Date)',
                                   labels=dict(x="Hour of Day", y="Date", color="Collections"),
                                   color_continuous_scale='Blues')
            fig_heatmap.update_layout(height=400)
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Hourly average
            hourly_avg = hourly_df.groupby('Hour')['Collections'].mean().reset_index()
            fig_hourly = px.bar(hourly_avg, x='Hour', y='Collections',
                               title='Average Collections by Hour of Day')
            fig_hourly.update_layout(height=300)
            st.plotly_chart(fig_hourly, use_container_width=True)
    
    # Performance metrics
    performance_data = get_performance_metrics()
    if performance_data:
        st.subheader("⚡ Performance Metrics")
        
        perf_df = pd.DataFrame(performance_data, 
                              columns=['Collection Time', 'Records', 'Processing Time', 'Success'])
        
        # Convert processing time to seconds if it's in different format
        if not perf_df.empty and 'Processing Time' in perf_df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Processing time trend
                fig_perf = px.scatter(perf_df.head(100), 
                                     x='Collection Time', 
                                     y='Processing Time',
                                     color='Success',
                                     title='Processing Time Trend',
                                     size='Records')
                fig_perf.update_layout(height=400)
                st.plotly_chart(fig_perf, use_container_width=True)
            
            with col2:
                # Records vs Processing Time
                successful_data = perf_df[perf_df['Success'] == 1]
                if not successful_data.empty:
                    fig_scatter = px.scatter(successful_data.head(100), 
                                           x='Records', 
                                           y='Processing Time',
                                           title='Records vs Processing Time',
                                           trendline='ols')
                    fig_scatter.update_layout(height=400)
                    st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Error analysis
    if detailed_stats['error_analysis']:
        st.subheader("🚨 Error Analysis (Last 7 Days)")
        
        error_df = pd.DataFrame(detailed_stats['error_analysis'],
                               columns=['Date', 'Error Message', 'Count'])
        
        if not error_df.empty:
            # Error distribution
            error_summary = error_df.groupby('Error Message')['Count'].sum().reset_index()
            error_summary = error_summary.sort_values('Count', ascending=False)
            
            fig_errors = px.bar(error_summary, x='Count', y='Error Message',
                               orientation='h', title='Error Distribution')
            fig_errors.update_layout(height=300)
            st.plotly_chart(fig_errors, use_container_width=True)
            
            # Recent errors table
            st.subheader("📋 Recent Errors")
            st.dataframe(error_df.head(20), use_container_width=True)
        else:
            st.success("✅ No errors recorded in the last 7 days!")
    
    # Recent collection history
    st.subheader("📋 Recent Collection History")
    if basic_stats['recent_history']:
        history_df = pd.DataFrame(basic_stats['recent_history'], 
                                columns=['Collection Time', 'Records Inserted', 'Excluded Records', 'Success'])
        
        # Add status emoji
        history_df['Status'] = history_df['Success'].apply(lambda x: '✅ Success' if x else '❌ Failed')
        history_df['Total Records'] = history_df['Records Inserted'] + history_df['Excluded Records']
        
        # Display with better formatting
        display_df = history_df[['Collection Time', 'Records Inserted', 'Excluded Records', 'Total Records', 'Status']]
        st.dataframe(display_df, use_container_width=True)
        
        # Summary stats for recent collections
        col1, col2, col3 = st.columns(3)
        with col1:
            success_rate = (history_df['Success'].sum() / len(history_df) * 100) if len(history_df) > 0 else 0
            st.metric("✅ Success Rate", f"{success_rate:.1f}%")
        
        with col2:
            avg_records = history_df['Records Inserted'].mean() if len(history_df) > 0 else 0
            st.metric("📊 Avg Records/Collection", f"{avg_records:.0f}")
            
        with col3:
            total_recent_records = history_df['Records Inserted'].sum() if len(history_df) > 0 else 0
            st.metric("📈 Total Recent Records", f"{total_recent_records:,}")
    
    # Collection frequency analysis
    st.subheader("📈 Collection Frequency Analysis")
    if detailed_stats['daily_stats']:
        # Calculate collection intervals
        intervals_col1, intervals_col2 = st.columns(2)
        
        with intervals_col1:
            # Average collections per day
            avg_collections_per_day = np.mean([row[1] for row in detailed_stats['daily_stats']])
            st.metric("📅 Avg Collections/Day", f"{avg_collections_per_day:.1f}")
            
            # Peak collection day
            max_collections_day = max(detailed_stats['daily_stats'], key=lambda x: x[1])
            st.metric("🏆 Peak Day Collections", f"{max_collections_day[1]} on {max_collections_day[0]}")
        
        with intervals_col2:
            # Average records per day
            avg_records_per_day = np.mean([row[2] for row in detailed_stats['daily_stats']])
            st.metric("📊 Avg Records/Day", f"{avg_records_per_day:,.0f}")
            
            # Peak records day
            max_records_day = max(detailed_stats['daily_stats'], key=lambda x: x[2])
            st.metric("📈 Peak Day Records", f"{max_records_day[2]:,} on {max_records_day[0]}")
    
    # System health indicators
    st.subheader("🏥 System Health")
    health_col1, health_col2, health_col3 = st.columns(3)
    
    with health_col1:
        # Recent success rate
        if basic_stats['recent_history']:
            recent_success_rate = (sum([row[3] for row in basic_stats['recent_history']]) / 
                                 len(basic_stats['recent_history']) * 100)
            if recent_success_rate >= 95:
                st.success(f"🟢 Excellent: {recent_success_rate:.1f}% success rate")
            elif recent_success_rate >= 85:
                st.warning(f"🟡 Good: {recent_success_rate:.1f}% success rate")
            else:
                st.error(f"🔴 Poor: {recent_success_rate:.1f}% success rate")
    
    with health_col2:
        # Time since last collection
        if basic_stats['last_collection']:
            last_time = basic_stats['last_collection'][0]
            if isinstance(last_time, str):
                try:
                    last_time = datetime.fromisoformat(last_time.replace('Z', '+00:00'))
                    time_diff = datetime.now() - last_time
                    hours_since = time_diff.total_seconds() / 3600
                    
                    if hours_since < 2:
                        st.success(f"🟢 Recent: {hours_since:.1f}h ago")
                    elif hours_since < 24:
                        st.warning(f"🟡 Moderate: {hours_since:.1f}h ago")
                    else:
                        st.error(f"🔴 Stale: {hours_since:.1f}h ago")
                except:
                    st.info("⚪ Unknown last collection time")
        else:
            st.error("🔴 No collections found")
    
    with health_col3:
        # Error rate
        if detailed_stats['error_analysis']:
            total_errors = sum([row[2] for row in detailed_stats['error_analysis']])
            total_recent = basic_stats['recent_collections']
            error_rate = (total_errors / total_recent * 100) if total_recent > 0 else 0
            
            if error_rate < 5:
                st.success(f"🟢 Low: {error_rate:.1f}% error rate")
            elif error_rate < 15:
                st.warning(f"🟡 Medium: {error_rate:.1f}% error rate")
            else:
                st.error(f"🔴 High: {error_rate:.1f}% error rate")
        else:
            st.success("🟢 No recent errors")
    
    # Auto-refresh option
    st.sidebar.subheader("⚙️ Settings")
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=False)
    
    if auto_refresh:
        st.sidebar.info("Page will refresh automatically every 30 seconds")
        # Add JavaScript for auto-refresh
        st.markdown("""
        <script>
        setTimeout(function(){
            window.location.reload(1);
        }, 30000);
        </script>
        """, unsafe_allow_html=True)
    
    # Export functionality
    if st.sidebar.button("📥 Export Recent Data"):
        if basic_stats['recent_history']:
            export_df = pd.DataFrame(basic_stats['recent_history'], 
                                   columns=['Collection Time', 'Records Inserted', 'Excluded Records', 'Success'])
            csv = export_df.to_csv(index=False)
            st.sidebar.download_button(
                label="💾 Download CSV",
                data=csv,
                file_name=f"collection_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime='text/csv'
            )
