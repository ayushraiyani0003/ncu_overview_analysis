"""
Database Configuration Constants
Update these values according to your database setup
"""

# Database Connection Configuration
DB_CONFIG = {
    'connection': 'mysql',
    'host': 'localhost',
    'port': 3306,
    'database': '',  # Empty for root access to all databases
    'username': 'root',
    'password': '',  # Empty password
    'charset': 'utf8mb4',
    'autocommit': True
}

# System databases to exclude from inspection
SYSTEM_DATABASES = [
    'information_schema',
    'performance_schema', 
    'mysql',
    'sys'
]

# Output configuration
OUTPUT_CONFIG = {
    'json_file': 'database_structure.json',
    'include_data_preview': False,  # Set to True if you want sample data
    'max_preview_rows': 5,
    'save_to_file': True,
    'print_summary': True
}

# Connection timeout settings
CONNECTION_CONFIG = {
    'connection_timeout': 30,  # seconds
    'read_timeout': 60,       # seconds
    'write_timeout': 60       # seconds
}