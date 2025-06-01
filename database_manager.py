import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import sqlite3

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.db_config = {
            'host': 'localhost',
            'database': 'ncu_overview',
            'user': 'root',
            'password': ''
        }
        self.create_connection()
    
    def create_connection(self):
        """Create database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                print("Successfully connected to MySQL database")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            # Fallback to SQLite for demo purposes
            self.connection = sqlite3.connect('ncu_data.db', check_same_thread=False)
            print("Using SQLite database as fallback")
    
    def ensure_connection(self):
        """Ensure database connection is active"""
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
        """Get latest data for real-time view"""
        try:
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
            self.ensure_connection()
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
    
    def get_all_projects(self):
        """Get all unique projects from database"""
        try:
            self.ensure_connection()
            cursor = self.connection.cursor()
            cursor.execute("SELECT DISTINCT project FROM ncu_data WHERE project != 'AAA' ORDER BY project")
            projects = [row[0] for row in cursor.fetchall()]
            return projects
        except Exception as e:
            print(f"Error fetching projects: {e}")
            return []
    
    def get_all_ncus(self):
        """Get all unique NCUs from database"""
        try:
            self.ensure_connection()
            cursor = self.connection.cursor()
            cursor.execute("SELECT DISTINCT ncu FROM ncu_data WHERE project != 'AAA' ORDER BY ncu")
            ncus = [row[0] for row in cursor.fetchall()]
            return ncus
        except Exception as e:
            print(f"Error fetching NCUs: {e}")
            return []