import requests
import json
import sys
import time
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import sqlite3

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.db_config = {
            'host': 'localhost',
            'database': 'ncu_overview',
            'user': 'root',
            'password': '',
            'autocommit': True,
            'connection_timeout': 300,
            'sql_mode': 'TRADITIONAL',
            'charset': 'utf8mb4',
            'use_unicode': True,
            'get_warnings': True,
        }
        self.batch_size = 100  # Process records in batches of 100
        self.max_retries = 3
        self.create_connection()
    
    def create_connection(self):
        """Create database connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self.connection = mysql.connector.connect(**self.db_config)
                if self.connection.is_connected():
                    print(f"✅ Successfully connected to MySQL database (attempt {attempt + 1})")
                    self.create_tcu_overview_table()
                    return
            except Error as e:
                print(f"❌ MySQL connection attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    print(f"⏳ Retrying connection in 5 seconds...")
                    time.sleep(5)
                else:
                    print("🔄 Falling back to SQLite database")
                    self._fallback_to_sqlite()
    
    def _fallback_to_sqlite(self):
        """Fallback to SQLite database"""
        try:
            self.connection = sqlite3.connect('ncu_data.db', check_same_thread=False)
            print("✅ Using SQLite database as fallback")
            self.create_tcu_overview_table_sqlite()
        except Exception as e:
            print(f"❌ Failed to create SQLite connection: {e}")
            raise
    
    def reconnect_if_needed(self):
        """Check and reconnect to database if connection is lost"""
        try:
            if hasattr(self.connection, 'is_connected'):
                # MySQL connection
                if not self.connection.is_connected():
                    print("🔄 MySQL connection lost, attempting to reconnect...")
                    self.create_connection()
            else:
                # SQLite connection - test with a simple query
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
        except Exception as e:
            print(f"🔄 Database connection issue detected: {e}")
            print("🔄 Attempting to reconnect...")
            self.create_connection()
    
    def create_tcu_overview_table(self):
        """Create tcu_overview table if it doesn't exist (MySQL)"""
        try:
            cursor = self.connection.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tcu_overview (
                id VARCHAR(255) NOT NULL PRIMARY KEY,
                project_name VARCHAR(255) NOT NULL,
                ncu VARCHAR(255) NOT NULL,
                actual_angle DECIMAL(10,2),
                target_angle DECIMAL(10,2),
                status_name VARCHAR(50),
                alarm VARCHAR(255),
                manual_mode VARCHAR(255),
                tcu_rows INT,
                wind_speed DECIMAL(10,2),
                created_at DATETIME,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_project_ncu (project_name, ncu),
                INDEX idx_created_at (created_at),
                INDEX idx_tcu_rows_ncu (tcu_rows, ncu)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_query)
            self.connection.commit()
            cursor.close()
            print("✅ tcu_overview table created/verified successfully")
        except Error as e:
            print(f"❌ Error creating tcu_overview table: {e}")
            raise
    
    def create_tcu_overview_table_sqlite(self):
        """Create tcu_overview table if it doesn't exist (SQLite)"""
        try:
            cursor = self.connection.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tcu_overview (
                id TEXT NOT NULL PRIMARY KEY,
                project_name TEXT NOT NULL,
                ncu TEXT NOT NULL,
                actual_angle REAL,
                target_angle REAL,
                status_name TEXT,
                alarm TEXT,
                manual_mode TEXT,
                tcu_rows INTEGER,
                wind_speed REAL,
                created_at TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            self.connection.commit()
            cursor.close()
            print("✅ tcu_overview table created/verified successfully (SQLite)")
        except Exception as e:
            print(f"❌ Error creating tcu_overview table (SQLite): {e}")
            raise
    
    def check_ids_exist(self, ids):
        """Check which IDs already exist in the database"""
        if not ids:
            return set()
            
        try:
            self.reconnect_if_needed()
            cursor = self.connection.cursor()
            
            if hasattr(self.connection, 'is_connected'):
                # MySQL
                format_strings = ','.join(['%s'] * len(ids))
                query = f"SELECT id FROM tcu_overview WHERE id IN ({format_strings})"
            else:
                # SQLite
                format_strings = ','.join(['?'] * len(ids))
                query = f"SELECT id FROM tcu_overview WHERE id IN ({format_strings})"
            
            cursor.execute(query, ids)
            existing_ids = set(row[0] for row in cursor.fetchall())
            cursor.close()
            
            return existing_ids
            
        except Exception as e:
            print(f"❌ Error checking existing IDs: {e}")
            return set()
    
    def insert_ncu_data_batch(self, data_batch, project_name):
        """Insert a single batch of NCU data into database"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Check connection before inserting
                self.reconnect_if_needed()
                
                cursor = self.connection.cursor()
                
                if hasattr(self.connection, 'is_connected'):
                    # MySQL
                    insert_query = """
                    INSERT INTO tcu_overview 
                    (id, project_name, ncu, actual_angle, target_angle, status_name, alarm, manual_mode, tcu_rows, wind_speed, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                else:
                    # SQLite
                    insert_query = """
                    INSERT INTO tcu_overview 
                    (id, project_name, ncu, actual_angle, target_angle, status_name, alarm, manual_mode, tcu_rows, wind_speed, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                
                cursor.executemany(insert_query, data_batch)
                self.connection.commit()
                cursor.close()
                
                print(f"✅ Successfully inserted batch of {len(data_batch)} records for {project_name}")
                return True
                
            except mysql.connector.Error as e:
                if e.errno == 1153:  # Packet too large error
                    print(f"⚠️  Packet size error for {project_name}, attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        print("🔄 Reducing batch size and retrying...")
                        # Split the batch in half for retry
                        if len(data_batch) > 1:
                            mid = len(data_batch) // 2
                            success1 = self.insert_ncu_data_batch(data_batch[:mid], project_name)
                            time.sleep(2)  # Small delay between split batches
                            success2 = self.insert_ncu_data_batch(data_batch[mid:], project_name)
                            return success1 and success2
                        else:
                            print(f"❌ Single record too large for {project_name}")
                            return False
                    else:
                        print(f"❌ Max retries exceeded for packet size error: {project_name}")
                        return False
                elif e.errno == 2013:  # Lost connection error
                    print(f"⚠️  Lost connection during insert for {project_name}, attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        print("🔄 Reconnecting and retrying...")
                        time.sleep(5)
                        self.create_connection()
                    else:
                        print(f"❌ Max retries exceeded for connection error: {project_name}")
                        return False
                else:
                    print(f"❌ MySQL error for {project_name}: {e}")
                    if attempt < max_retries - 1:
                        print(f"🔄 Retrying in 3 seconds... (attempt {attempt + 2})")
                        time.sleep(3)
                    else:
                        return False
            except Exception as e:
                print(f"❌ Unexpected error inserting batch for {project_name}: {e}")
                if attempt < max_retries - 1:
                    print(f"🔄 Retrying in 3 seconds... (attempt {attempt + 2})")
                    time.sleep(3)
                else:
                    return False
        
        return False
    
    def insert_ncu_data(self, data_list, project_name):
        """Insert NCU data into database using batch processing with duplicate check"""
        if not data_list:
            print(f"⚠️  No data to insert for {project_name}")
            return True
        
        # Extract all IDs and check for duplicates
        all_ids = [record[0] for record in data_list]  # ID is first element
        existing_ids = self.check_ids_exist(all_ids)
        
        # Filter out existing records
        new_data = [record for record in data_list if record[0] not in existing_ids]
        
        print(f"📊 Total records: {len(data_list)}, Existing: {len(existing_ids)}, New: {len(new_data)} for {project_name}")
        
        if not new_data:
            print(f"ℹ️  All records already exist in database for {project_name}")
            return True
        
        print(f"📊 Processing {len(new_data)} new records for {project_name} in batches of {self.batch_size}")
        
        total_batches = (len(new_data) + self.batch_size - 1) // self.batch_size
        successful_batches = 0
        
        # Process data in batches
        for i in range(0, len(new_data), self.batch_size):
            batch_num = (i // self.batch_size) + 1
            batch = new_data[i:i + self.batch_size]
            
            print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} records) for {project_name}")
            
            if self.insert_ncu_data_batch(batch, project_name):
                successful_batches += 1
            else:
                print(f"❌ Failed to insert batch {batch_num} for {project_name}")
            
            # Add delay between batches to prevent overloading
            if batch_num < total_batches:
                time.sleep(1)  # Reduced delay for batches
        
        success_rate = successful_batches / total_batches if total_batches > 0 else 0
        print(f"📈 Batch processing summary for {project_name}: {successful_batches}/{total_batches} batches successful ({success_rate:.1%})")
        
        return successful_batches > 0  # Return True if at least one batch was successful
    
    def close_connection(self):
        """Close database connection"""
        if self.connection:
            try:
                self.connection.close()
                print("✅ Database connection closed")
            except Exception as e:
                print(f"⚠️  Error closing connection: {e}")

def fetch_projects_from_master():
    """Fetch projects from /master endpoint with retry logic"""
    url = "http://145.223.18.73:6265/master"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Fetching projects from: {url} (attempt {attempt + 1})")
            response = requests.get(url, timeout=15)
            
            print(f"📡 Master API Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    print(f"✅ Found {data['count']} projects")
                    return data['data']
                else:
                    print("⚠️  No project data found in response")
                    return []
            else:
                print(f"❌ Failed to fetch projects. Status: {response.status_code}")
                if attempt < max_retries - 1:
                    print("🔄 Retrying in 5 seconds...")
                    time.sleep(5)
                
        except requests.exceptions.ConnectionError as e:
            print(f"❌ Connection error while fetching projects (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 5 seconds...")
                time.sleep(5)
        except requests.exceptions.Timeout as e:
            print(f"❌ Timeout error while fetching projects (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 5 seconds...")
                time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error while fetching projects (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 5 seconds...")
                time.sleep(5)
    
    print("❌ Failed to fetch projects after all retry attempts")
    return []

def fetch_project_data(project_db, project_name, start_date, end_date):
    """Fetch data for a specific project from /data endpoint with retry logic"""
    url = "http://145.223.18.73:6265/data"
    params = {
        "project_db": project_db,
        "start": start_date,
        "end": end_date
    }
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Fetching data for project: {project_name} (attempt {attempt + 1})")
            response = requests.get(url, params=params, timeout=45)
            
            print(f"📡 Data API Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    print(f"✅ Found {data['count']} records for {project_name}")
                    return data['data']
                else:
                    print(f"⚠️  No data found for project: {project_name}")
                    return []
            else:
                print(f"❌ Failed to fetch data for {project_name}. Status: {response.status_code}")
                if attempt < max_retries - 1:
                    print("🔄 Retrying in 5 seconds...")
                    time.sleep(5)
                
        except requests.exceptions.ConnectionError as e:
            print(f"❌ Connection error while fetching data for {project_name} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 5 seconds...")
                time.sleep(5)
        except requests.exceptions.Timeout as e:
            print(f"❌ Timeout error while fetching data for {project_name} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 5 seconds...")
                time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error while fetching data for {project_name} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 5 seconds...")
                time.sleep(5)
    
    print(f"❌ Failed to fetch data for {project_name} after all retry attempts")
    return []

def process_and_store_data_for_timeframe(projects, start_datetime, end_datetime, db_manager):
    """Process all projects for a specific time frame and store data in database"""
    total_records = 0
    successful_projects = 0
    failed_projects = []
    
    start_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n🕒 Processing timeframe: {start_str} to {end_str}")
    
    for idx, project in enumerate(projects):
        project_db = project.get('db_name')
        project_name = project.get('project_name')
        
        if not project_db or not project_name:
            print(f"⚠️  Skipping project with missing db_name or project_name: {project}")
            continue
        
        print(f"\n{'='*60}")
        print(f"🔄 Processing: {project_name} ({project_db}) [{idx + 1}/{len(projects)}]")
        print(f"{'='*60}")
        
        try:
            # Fetch data for this project
            project_data = fetch_project_data(project_db, project_name, start_str, end_str)
            
            if not project_data:
                print(f"⚠️  No data to process for {project_name}")
                continue
            
            # Process and filter data
            processed_data = []
            for record in project_data:
                try:
                    # Only include records where status_name is not equal to "ok"
                    if record.get('status_name') != 'ok':
                        # Extract NCU from device_id (after "jsm-pub/")
                        device_id = record.get('device_id', '')
                        ncu = device_id.split('jsm-pub/')[-1] if 'jsm-pub/' in device_id else device_id
                        
                        # Parse created_at datetime
                        created_at_str = record.get('created_at', '')
                        try:
                            # Convert from "Sun, 30 Mar 2025 23:59:55 GMT" format
                            created_at = datetime.strptime(created_at_str, "%a, %d %b %Y %H:%M:%S %Z")
                        except:
                            try:
                                # Try alternative format
                                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
                            except:
                                created_at = datetime.now()  # Fallback to current time
                        
                        processed_record = (
                            record.get('id'),  # ID first for duplicate check
                            project_name,
                            ncu,
                            record.get('actual_angle'),
                            record.get('target_angle'),
                            record.get('status_name'),
                            record.get('alarm'),
                            record.get('manual_mode'),
                            record.get('rows'),
                            record.get('wind_speed'),
                            created_at
                        )
                        processed_data.append(processed_record)
                except Exception as e:
                    print(f"⚠️  Error processing record for {project_name}: {e}")
                    continue
            
            if processed_data:
                print(f"📊 Preparing to insert {len(processed_data)} records for {project_name}")
                if db_manager.insert_ncu_data(processed_data, project_name):
                    total_records += len(processed_data)
                    successful_projects += 1
                    print(f"✅ Successfully processed {project_name}")
                else:
                    print(f"❌ Failed to insert data for {project_name}")
                    failed_projects.append(project_name)
            else:
                print(f"ℹ️  No valid records (status != 'ok') found for {project_name}")
            
            # Short delay between projects
            time.sleep(2)
                
        except Exception as e:
            print(f"❌ Error processing project {project_name}: {e}")
            failed_projects.append(project_name)
            continue
    
    # Summary for this timeframe
    print(f"\n📊 Timeframe Summary ({start_str} to {end_str}):")
    print(f"✅ Projects processed: {successful_projects}/{len(projects)}")
    print(f"📈 Records processed: {total_records}")
    
    if failed_projects:
        print(f"❌ Failed projects: {', '.join(failed_projects)}")
    
    return successful_projects, total_records

def main():
    """Main function to execute continuous data collection"""
    print("=" * 70)
    print("🚀 CONTINUOUS CLOUD API DATA COLLECTION - 1 HOUR INTERVALS")
    print("=" * 70)
    
    # Configuration - Set your start date here
    START_DATE = "2025-06-03 12:30:00"  # Change this to your desired start date
    
    try:
        start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"❌ Invalid start date format. Please use: YYYY-MM-DD HH:MM:SS")
        return
    
    print(f"📅 Starting from: {START_DATE}")
    print(f"⚙️  Configuration: 1-hour intervals, checking for duplicates")
    
    # Initialize database connection
    db_manager = DatabaseManager()
    
    try:
        # Step 1: Fetch all projects once
        print(f"\n🔄 Step 1: Fetching projects from master API...")
        projects = fetch_projects_from_master()
        
        if not projects:
            print("❌ No projects found. Exiting.")
            return
        
        print(f"✅ Found {len(projects)} projects")
        
        # Step 2: Continuous processing
        print(f"\n🔄 Step 2: Starting continuous processing...")
        
        current_start = start_datetime
        cycle_count = 0
        total_records_processed = 0
        
        while True:
            cycle_count += 1
            current_now = datetime.now()
            
            # Calculate end time (1 hour after start)
            current_end = current_start + timedelta(hours=1)
            
            # Only process if the end time is in the past (not current or future)
            if current_end >= current_now:
                print(f"\n⏳ Waiting... End time {current_end.strftime('%Y-%m-%d %H:%M:%S')} is not yet in the past")
                print(f"⏳ Current time: {current_now.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"⏳ Will check again in 10 minutes...")
                time.sleep(600)  # Wait 10 minutes
                continue
            
            print(f"\n{'='*80}")
            print(f"🔄 CYCLE {cycle_count} - Processing 1-hour timeframe")
            print(f"{'='*80}")
            
            # Process this 1-hour timeframe
            successful_projects, records_processed = process_and_store_data_for_timeframe(
                projects, current_start, current_end, db_manager
            )
            
            total_records_processed += records_processed
            
            # Move to next hour
            current_start = current_end
            
            print(f"\n✅ Cycle {cycle_count} completed")
            print(f"📊 Total records processed so far: {total_records_processed}")
            print(f"🕒 Next timeframe: {current_start.strftime('%Y-%m-%d %H:%M:%S')} to {(current_start + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Short break before next cycle
            print(f"⏳ Waiting 5 minutes before next cycle...")
            time.sleep(300)  # 5 minutes break
            
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Script interrupted by user")
        print(f"📊 Total records processed: {total_records_processed}")
        print(f"🔄 Completed {cycle_count} cycles")
    except Exception as e:
        print(f"\n❌ Critical error in main: {e}")
    finally:
        db_manager.close_connection()
        print(f"\n🎉 Script ended at: {datetime.now()}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error in main: {e}")
        sys.exit(1)