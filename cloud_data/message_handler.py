import json
from datetime import datetime

class MessageHandler:
    """
    Handles message and data operations
    This class will be extended for SQL operations later
    """
    
    def __init__(self):
        self.messages = self.load_messages()
    
    def load_messages(self):
        """
        Load messages from JSON file
        You can replace this with database queries later
        """
        try:
            with open('messages.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Return default messages if file doesn't exist
            return {
                "hello": "Hello World",
                "welcome": "Welcome to the API",
                "status": "API is working correctly"
            }
    
    def get_hello_message(self):
        """
        Get hello world message
        Returns structured JSON response
        """
        return {
            "message": self.messages.get("hello", "Hello World"),
            "timestamp": datetime.now().isoformat(),
            "status": "success",
            "data_source": "file"  # Will change to "database" later
        }
    
    def get_dynamic_data(self):
        """
        Get dynamic data - placeholder for future SQL queries
        This method will be replaced with actual database queries
        """
        return {
            "message": "Dynamic data endpoint",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "current_records": 0,  # Will be replaced with SQL COUNT
                "last_updated": datetime.now().isoformat(),
                "source": "placeholder"  # Will be "database" later
            },
            "status": "success"
        }
    
    def get_sql_data(self, query=None):
        """
        Placeholder method for future SQL operations
        This will handle actual database queries
        """
        # TODO: Implement database connection and query execution
        # For now, return placeholder data
        return {
            "message": "SQL data will be implemented here",
            "query": query or "SELECT * FROM table",
            "results": [],
            "timestamp": datetime.now().isoformat()
        }