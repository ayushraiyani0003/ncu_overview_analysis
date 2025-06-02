import mysql.connector
from mysql.connector import Error
import json
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import threading
import sqlite3
from typing import Dict, List
import logging
import pytz
import traceback
import signal
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ncu_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# Collection Configuration
COLLECTION_CONFIG = {
    'interval_minutes': 2,
    'max_retries': 3,
    'retry_delay': 30,  # seconds
    'login_timeout': 60,  # seconds
    'data_timeout': 30,   # seconds
    'connection_timeout': 10,  # seconds
}

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.connection_lock = threading.Lock()
        self.create_connection()
        self.create_tables()
    
    def create_connection(self):
        """Create database connection with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.connection = mysql.connector.connect(
                    **DB_CONFIG,
                    autocommit=False,
                    connection_timeout=COLLECTION_CONFIG['connection_timeout']
                )
                if self.connection.is_connected():
                    logger.info("Successfully connected to MySQL database")
                    return True
            except Error as e:
                logger.warning(f"MySQL connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.info("Falling back to SQLite database")
                    try:
                        self.connection = sqlite3.connect(
                            'ncu_data.db', 
                            check_same_thread=False,
                            timeout=20.0
                        )
                        logger.info("Using SQLite database as fallback")
                        return True
                    except Exception as sqlite_error:
                        logger.error(f"SQLite connection failed: {sqlite_error}")
                        return False
                time.sleep(2)
        return False
    
    def create_tables(self):
        """Create tables if not exists and migrate existing tables"""
        try:
            with self.connection_lock:
                cursor = self.connection.cursor()
                
                # Main NCU data table
                if hasattr(self.connection, 'is_connected'):
                    # MySQL
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
                        collection_duration DECIMAL(10,3),
                        INDEX idx_collection_time (collection_time)
                    )
                    """
                else:
                    # SQLite
                    create_ncu_table_query = """
                    CREATE TABLE IF NOT EXISTS ncu_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER,
                        date_time TEXT,
                        project TEXT,
                        ncu TEXT,
                        user_id INTEGER,
                        ncu_id INTEGER,
                        alarm INTEGER,
                        battery_alarm INTEGER,
                        battery_warning INTEGER,
                        warning_count INTEGER,
                        master_mode INTEGER,
                        manual_mode INTEGER,
                        ok_status INTEGER,
                        communication_error INTEGER,
                        inactive_tcu INTEGER,
                        max_wind_speed REAL,
                        avg_wind_speed REAL,
                        raw_data TEXT
                    )
                    """
                    
                    create_tracking_table_query = """
                    CREATE TABLE IF NOT EXISTS data_collection_tracking (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        collection_time TEXT,
                        timestamp INTEGER,
                        records_collected INTEGER,
                        records_inserted INTEGER,
                        excluded_records INTEGER,
                        success INTEGER,
                        error_message TEXT,
                        collection_duration REAL
                    )
                    """
                
                cursor.execute(create_ncu_table_query)
                cursor.execute(create_tracking_table_query)
                
                # Migrate existing tables if needed
                self.migrate_tables()
                
                self.connection.commit()
                logger.info("Tables created and migrated successfully")
                
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def migrate_tables(self):
        """Migrate existing tables to add missing columns"""
        try:
            cursor = self.connection.cursor()
            
            if hasattr(self.connection, 'is_connected'):
                # MySQL - Check if collection_duration column exists
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = 'data_collection_tracking' 
                    AND COLUMN_NAME = 'collection_duration'
                """, (DB_CONFIG['database'],))
                
                if cursor.fetchone()[0] == 0:
                    logger.info("Adding collection_duration column to data_collection_tracking table")
                    cursor.execute("""
                        ALTER TABLE data_collection_tracking 
                        ADD COLUMN collection_duration DECIMAL(10,3) DEFAULT 0.0
                    """)
                    logger.info("Successfully added collection_duration column")
            else:
                # SQLite - Check if collection_duration column exists
                cursor.execute("PRAGMA table_info(data_collection_tracking)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'collection_duration' not in columns:
                    logger.info("Adding collection_duration column to data_collection_tracking table")
                    cursor.execute("""
                        ALTER TABLE data_collection_tracking 
                        ADD COLUMN collection_duration REAL DEFAULT 0.0
                    """)
                    logger.info("Successfully added collection_duration column")
                    
        except Exception as e:
            logger.warning(f"Error during table migration (this might be normal): {e}")
            # Don't raise error as this might be expected for new installations
    
    def ensure_connection(self):
        """Ensure database connection is active"""
        try:
            with self.connection_lock:
                if hasattr(self.connection, 'is_connected'):
                    # MySQL connection
                    if not self.connection.is_connected():
                        logger.warning("MySQL connection lost, reconnecting...")
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
                    except Exception:
                        logger.warning("SQLite connection lost, reconnecting...")
                        self.create_connection()
                        
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            self.create_connection()
    
    def insert_data(self, data_list: List[Dict], timestamp: int, collection_duration: float):
        """Insert data into database with comprehensive error handling"""
        start_time = time.time()
        
        try:
            self.ensure_connection()
            
            with self.connection_lock:
                cursor = self.connection.cursor()
                date_time = datetime.fromtimestamp(timestamp / 1000)
                
                # Filter out AAA project data
                filtered_data = [item for item in data_list 
                               if item.get('project', {}).get('value', '') != 'AAA']
                
                excluded_count = len(data_list) - len(filtered_data)
                inserted_count = 0
                
                for item in filtered_data:
                    try:
                        if hasattr(self.connection, 'is_connected'):
                            # MySQL
                            insert_query = """
                            INSERT INTO ncu_data 
                            (timestamp, date_time, project, ncu, user_id, ncu_id, alarm, 
                             battery_alarm, battery_warning, warning_count, master_mode, 
                             manual_mode, ok_status, communication_error, inactive_tcu, 
                             max_wind_speed, avg_wind_speed, raw_data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                        else:
                            # SQLite
                            insert_query = """
                            INSERT INTO ncu_data 
                            (timestamp, date_time, project, ncu, user_id, ncu_id, alarm, 
                             battery_alarm, battery_warning, warning_count, master_mode, 
                             manual_mode, ok_status, communication_error, inactive_tcu, 
                             max_wind_speed, avg_wind_speed, raw_data)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            float(item.get('maxWindSpeed', {}).get('value', 0.0)),
                            float(item.get('avgWindSpeed', {}).get('value', 0.0)),
                            json.dumps(item)
                        )
                        
                        cursor.execute(insert_query, values)
                        inserted_count += 1
                        
                    except Exception as item_error:
                        logger.error(f"Error inserting individual record: {item_error}")
                        continue
                
                # Insert tracking record with fallback for missing collection_duration column
                try:
                    if hasattr(self.connection, 'is_connected'):
                        # MySQL
                        tracking_query = """
                        INSERT INTO data_collection_tracking 
                        (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, collection_duration)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                    else:
                        # SQLite
                        tracking_query = """
                        INSERT INTO data_collection_tracking 
                        (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, collection_duration)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                    
                    cursor.execute(tracking_query, (
                        date_time, timestamp, len(data_list), inserted_count, excluded_count, 1, collection_duration
                    ))
                    
                except Exception as tracking_error:
                    # Fallback: Insert without collection_duration column if it doesn't exist
                    logger.warning(f"Failed to insert with collection_duration, trying without: {tracking_error}")
                    
                    if hasattr(self.connection, 'is_connected'):
                        # MySQL
                        fallback_tracking_query = """
                        INSERT INTO data_collection_tracking 
                        (collection_time, timestamp, records_collected, records_inserted, excluded_records, success)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """
                    else:
                        # SQLite
                        fallback_tracking_query = """
                        INSERT INTO data_collection_tracking 
                        (collection_time, timestamp, records_collected, records_inserted, excluded_records, success)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """
                    
                    cursor.execute(fallback_tracking_query, (
                        date_time, timestamp, len(data_list), inserted_count, excluded_count, 1
                    ))
                
                self.connection.commit()
                logger.info(f"Inserted {inserted_count} records successfully (excluded {excluded_count} AAA records)")
                return True
                
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Insert error tracking record
            try:
                self.ensure_connection()
                with self.connection_lock:
                    cursor = self.connection.cursor()
                    
                    try:
                        if hasattr(self.connection, 'is_connected'):
                            error_tracking_query = """
                            INSERT INTO data_collection_tracking 
                            (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, error_message, collection_duration)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """
                        else:
                            error_tracking_query = """
                            INSERT INTO data_collection_tracking 
                            (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, error_message, collection_duration)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """
                        
                        cursor.execute(error_tracking_query, (
                            datetime.now(), timestamp, len(data_list), 0, 0, 0, str(e), collection_duration
                        ))
                        
                    except Exception:
                        # Fallback: Insert without collection_duration column
                        if hasattr(self.connection, 'is_connected'):
                            fallback_error_query = """
                            INSERT INTO data_collection_tracking 
                            (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, error_message)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                        else:
                            fallback_error_query = """
                            INSERT INTO data_collection_tracking 
                            (collection_time, timestamp, records_collected, records_inserted, excluded_records, success, error_message)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """
                        
                        cursor.execute(fallback_error_query, (
                            datetime.now(), timestamp, len(data_list), 0, 0, 0, str(e)
                        ))
                    
                    self.connection.commit()
                    
            except Exception as tracking_error:
                logger.error(f"Failed to insert error tracking record: {tracking_error}")
            
            return False

class DataCollector:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.session = None
        self.is_logged_in = False
        self.last_login_time = None
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        self.setup_session()
    
    def setup_session(self):
        """Setup requests session with retry strategy"""
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default timeout
        self.session.timeout = COLLECTION_CONFIG['data_timeout']
    
    def login(self):
        """Login to the system with retry logic"""
        max_retries = COLLECTION_CONFIG['max_retries']
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting login (attempt {attempt + 1}/{max_retries})")
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
                
                # Get login page for CSRF token
                login_page = self.session.get(
                    LOGIN_CONFIG['login_url'], 
                    headers=headers,
                    timeout=COLLECTION_CONFIG['login_timeout']
                )
                
                if login_page.status_code != 200:
                    raise Exception(f"Login page returned status {login_page.status_code}")
                
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
                    allow_redirects=True,
                    timeout=COLLECTION_CONFIG['login_timeout']
                )
                
                if login_response.status_code == 200:
                    self.is_logged_in = True
                    self.last_login_time = datetime.now()
                    self.consecutive_failures = 0
                    logger.info("Login successful")
                    return True
                else:
                    raise Exception(f"Login failed with status {login_response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Login attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = COLLECTION_CONFIG['retry_delay'] * (attempt + 1)
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error("All login attempts failed")
                    self.consecutive_failures += 1
        
        return False
    
    def logout(self):
        """Logout from the system"""
        try:
            if self.session and self.is_logged_in:
                logout_url = "http://app.sunchaser.cloud/logout"
                self.session.get(logout_url, timeout=10)
                logger.info("Logged out successfully")
        except Exception as e:
            logger.warning(f"Logout error: {e}")
        finally:
            self.is_logged_in = False
            self.last_login_time = None
    
    def collect_data(self):
        """Collect data from the API with comprehensive error handling"""
        collection_start_time = time.time()
        logger.info("Starting data collection...")
        
        try:
            # Check if need to re-login (every hour or if not logged in)
            if (not self.is_logged_in or 
                (self.last_login_time and 
                 datetime.now() - self.last_login_time > timedelta(hours=1))):
                
                self.logout()
                if not self.login():
                    logger.error("Failed to login, skipping data collection")
                    return False
            
            # Generate timestamp
            timestamp = int(time.time() * 1000)
            ajax_url = f"{LOGIN_CONFIG['target_url']}?_={timestamp}"
            
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': LOGIN_CONFIG['target_url'],
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info(f"Fetching data from: {ajax_url}")
            response = self.session.get(
                ajax_url, 
                headers=headers,
                timeout=COLLECTION_CONFIG['data_timeout']
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'data' in data and isinstance(data['data'], list):
                        collection_duration = time.time() - collection_start_time
                        success = self.db_manager.insert_data(data['data'], timestamp, collection_duration)
                        
                        if success:
                            self.consecutive_failures = 0
                            logger.info(f"Data collection completed successfully in {collection_duration:.2f} seconds")
                            return True
                        else:
                            logger.error("Failed to insert data into database")
                            self.consecutive_failures += 1
                    else:
                        logger.error("Invalid data format received from API")
                        self.consecutive_failures += 1
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    logger.error(f"Response content: {response.text[:500]}")
                    self.consecutive_failures += 1
                    
            elif response.status_code == 401:
                logger.warning("Authentication required, attempting re-login")
                self.is_logged_in = False
                return self.collect_data()  # Retry once after re-login
                
            else:
                logger.error(f"HTTP error {response.status_code}: {response.text[:200]}")
                self.consecutive_failures += 1
            
            return False
            
        except requests.exceptions.Timeout:
            logger.error("Request timeout during data collection")
            self.consecutive_failures += 1
            return False
            
        except requests.exceptions.ConnectionError:
            logger.error("Connection error during data collection")
            self.consecutive_failures += 1
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error during data collection: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.consecutive_failures += 1
            return False

class NCUDataCollectionService:
    def __init__(self):
        self.db_manager = None
        self.data_collector = None
        self.is_running = False
        self.collection_thread = None
        self.stop_event = threading.Event()
        
    def initialize(self):
        """Initialize the service components"""
        try:
            logger.info("Initializing NCU Data Collection Service...")
            self.db_manager = DatabaseManager()
            self.data_collector = DataCollector(self.db_manager)
            logger.info("Service initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize service: {e}")
            return False
        
    def is_sleep_time_london(self):
        """Check if current time is between 4:30 PM and 10:30 PM London time"""
        try:
            # Get current time in London timezone
            london_tz = pytz.timezone('Europe/London')
            london_time = datetime.now(london_tz)
            
            # Define sleep time range (3:30 PM to 10:30 PM)
            sleep_start = london_time.replace(hour=16, minute=30, second=0, microsecond=0)
            sleep_end = london_time.replace(hour=23, minute=00, second=0, microsecond=0)
            
            # Check if current time is within sleep period
            return sleep_start <= london_time <= sleep_end
        except Exception as e:
            logger.error(f"Error checking London time: {e}")
            return False
    def run_collection_loop(self):
        """Main collection loop that runs every 2 minutes"""
        logger.info("Starting data collection loop...")
        while not self.stop_event.is_set():
            try:
                # Check if it's sleep time in London (3:30 PM to 10:30 PM)
                if self.is_sleep_time_london():
                    logger.info("Sleep time for tracker (3:30 PM - 10:30 PM London time). Skipping data collection.")
                    # Wait for next collection interval but check stop event every 10 seconds
                    wait_seconds = COLLECTION_CONFIG['interval_minutes'] * 60
                    for _ in range(wait_seconds // 10):
                        if self.stop_event.wait(10):
                            return
                    continue
                
                # Check if too many consecutive failures
                if (self.data_collector.consecutive_failures >=
                    self.data_collector.max_consecutive_failures):
                    logger.error(f"Too many consecutive failures ({self.data_collector.consecutive_failures})")
                    logger.info("Waiting 10 minutes before resuming...")
                    # Wait 10 minutes but check stop event every 30 seconds
                    for _ in range(20):  # 20 * 30 = 600 seconds = 10 minutes
                        if self.stop_event.wait(30):
                            return
                    # Reset failure count and try again
                    self.data_collector.consecutive_failures = 0
                    logger.info("Resuming data collection after failure timeout")
                
                # Collect data
                success = self.data_collector.collect_data()
                if success:
                    logger.info("Data collection successful")
                else:
                    logger.warning("Data collection failed")
                
                # Log status every 10 collections (20 minutes)
                if hasattr(self, 'collection_count'):
                    self.collection_count += 1
                else:
                    self.collection_count = 1
                
                if self.collection_count % 10 == 0:
                    logger.info(f"Collection #{self.collection_count} completed. "
                            f"Consecutive failures: {self.data_collector.consecutive_failures}")
                
                # Wait for next collection (2 minutes) but check stop event every 10 seconds
                wait_seconds = COLLECTION_CONFIG['interval_minutes'] * 60
                for _ in range(wait_seconds // 10):
                    if self.stop_event.wait(10):
                        return
                        
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, stopping...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in collection loop: {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                # Wait before retrying
                if not self.stop_event.wait(60):
                    continue
                else:
                    break
        
        logger.info("Data collection loop stopped")
    
    def start(self):
        """Start the data collection service"""
        if self.is_running:
            logger.warning("Service is already running")
            return False
            
        if not self.initialize():
            logger.error("Failed to initialize service")
            return False
        
        try:
            self.is_running = True
            self.stop_event.clear()
            
            # Start collection in a separate thread
            self.collection_thread = threading.Thread(
                target=self.run_collection_loop,
                daemon=False,
                name="NCU-DataCollection"
            )
            self.collection_thread.start()
            
            logger.info("NCU Data Collection Service started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            self.is_running = False
            return False
    
    def stop(self):
        """Stop the data collection service"""
        if not self.is_running:
            logger.warning("Service is not running")
            return
            
        logger.info("Stopping NCU Data Collection Service...")
        self.stop_event.set()
        
        if self.collection_thread and self.collection_thread.is_alive():
            self.collection_thread.join(timeout=30)
            
        if self.data_collector:
            self.data_collector.logout()
            
        self.is_running = False
        logger.info("NCU Data Collection Service stopped")
    
    def get_status(self):
        """Get service status"""
        return {
            'is_running': self.is_running,
            'is_logged_in': self.data_collector.is_logged_in if self.data_collector else False,
            'consecutive_failures': self.data_collector.consecutive_failures if self.data_collector else 0,
            'last_login_time': self.data_collector.last_login_time if self.data_collector else None,
            'collection_count': getattr(self, 'collection_count', 0)
        }

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if 'service' in globals():
        service.stop()
    sys.exit(0)

def main():
    """Main function to run the service"""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    global service
    service = NCUDataCollectionService()
    
    logger.info("=" * 50)
    logger.info("NCU Data Collection Service Starting...")
    logger.info(f"Collection interval: {COLLECTION_CONFIG['interval_minutes']} minutes")
    logger.info(f"Max retries: {COLLECTION_CONFIG['max_retries']}")
    logger.info(f"Retry delay: {COLLECTION_CONFIG['retry_delay']} seconds")
    logger.info("=" * 50)
    
    if service.start():
        try:
            # Keep main thread alive
            while service.is_running:
                time.sleep(60)  # Check every minute
                
                # Log status every 30 minutes
                if hasattr(service, 'last_status_log'):
                    if time.time() - service.last_status_log > 1800:  # 30 minutes
                        status = service.get_status()
                        logger.info(f"Service Status: {status}")
                        service.last_status_log = time.time()
                else:
                    service.last_status_log = time.time()
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            service.stop()
    else:
        logger.error("Failed to start NCU Data Collection Service")
        sys.exit(1)

if __name__ == "__main__":
    main()