#!/usr/bin/env python3
"""
Database Query Runner - Optimized Version
Fast database queries with per-request connections
Uses constants file for configuration
"""

import mysql.connector
from mysql.connector import Error

# Import configuration from constants file
from db_constants import DB_CONFIG, CONNECTION_CONFIG

class DatabaseQueryRunner:
    def __init__(self):
        # Build connection config from constants
        self.connection_config = {
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port'],
            'user': DB_CONFIG['username'],
            'password': DB_CONFIG['password'],
            'charset': DB_CONFIG['charset'],
            'autocommit': DB_CONFIG['autocommit'],
            'connection_timeout': CONNECTION_CONFIG['connection_timeout']
        }
    
    def _create_connection(self, database=None):
        """Create a new database connection"""
        config = self.connection_config.copy()
        if database:
            config['database'] = database
        
        try:
            connection = mysql.connector.connect(**config)
            return connection
        except Error as e:
            print(f"❌ Error connecting to MySQL: {e}")
            return None
    
    def get_master_iot_data(self):
        """Get project data from master_iot database"""
        connection = None
        cursor = None
        
        try:
            # Connect to master_iot database
            connection = self._create_connection('master_iot')
            if not connection:
                return []
            
            cursor = connection.cursor(dictionary=True)
            
            # Query users table for project info
            query = "SELECT name as project_name, db_name FROM users"
            cursor.execute(query)
            
            results = cursor.fetchall()
            return results
            
        except Error as e:
            print(f"❌ Error querying master_iot: {e}")
            return []
        
        finally:
            # Always close connections
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
    
    def get_tcu_data(self, project_db, start_date, end_date):
        """Get IoT data from project database within date range"""
        connection = None
        cursor = None
        print(f"Querying {project_db} for data between {start_date} and {end_date}")
        try:
            # Connect to specific project database
            connection = self._create_connection(project_db)
            if not connection:
                return []
            
            cursor = connection.cursor(dictionary=True)
            
            # Query local_devices_data table with date filter
            query = """
            SELECT * FROM local_devices_data 
            WHERE created_at BETWEEN %s AND %s 
            ORDER BY created_at ASC
            """
            
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            print(f"{connection}")
            return results
            
        except Error as e:
            print(f"❌ Error querying {project_db}: {e}")
            return []
        
        finally:
            # Always close connections
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()

# Create global instance for easy importing
db_runner = DatabaseQueryRunner()

# Convenience functions for direct import
def get_master_iot_data():
    """Get project data from master_iot database"""
    return db_runner.get_master_iot_data()

def get_tcu_data(project_db, start_date, end_date):
    """Get IoT data from project database within date range"""
    return db_runner.get_tcu_data(project_db, start_date, end_date)