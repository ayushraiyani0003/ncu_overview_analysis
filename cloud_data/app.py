from flask import Flask, jsonify, request
from message_handler import MessageHandler
from db_query_runner import get_master_iot_data, get_tcu_data

app = Flask(__name__)

# Initialize message handler
message_handler = MessageHandler()

@app.route('/', methods=['GET'])
def hello_world():
    """
    GET endpoint that returns Hello World message
    Later this can be extended to return SQL data
    """
    try:
        # Get message from separate handler
        response_data = message_handler.get_hello_message()
        return jsonify(response_data), 200
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

@app.route('/master', methods=['GET'])
def get_master_data():
    """
    GET endpoint to retrieve all projects from master_iot database
    Returns: [{'project_name': 'name', 'db_name': 'database_name'}, ...]
    """
    try:
        response_data = get_master_iot_data()
        
        return jsonify({
            'success': True,
            'count': len(response_data),
            'data': response_data
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'Failed to retrieve master data',
            'message': str(e)
        }), 500
    
@app.route('/data', methods=['GET'])
def get_data():
    """
    GET endpoint to retrieve IoT data from specific project database
    Query parameters:
    - project_db: Database name (required)
    - start: Start date in YYYY-MM-DD format (required)
    - end: End date in YYYY-MM-DD format (required)
    """
    # Get query parameters
    project_db = request.args.get('project_db')
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    # Validate required parameters
    if not all([project_db, start_date, end_date]):
        return jsonify({
            'success': False,
            'error': 'Missing required parameters',
            'required': ['project_db', 'start', 'end'],
            'example': '/data?project_db=my_project&start=2024-01-01&end=2024-01-31'
        }), 400

    try:
        response_data = get_tcu_data(project_db, start_date, end_date)
        
        return jsonify({
            'success': True,
            'project_db': project_db,
            'date_range': {
                'start': start_date,
                'end': end_date
            },
            'count': len(response_data),
            'data': response_data
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': 'Failed to retrieve project data',
            'message': str(e),
            'project_db': project_db
        }), 500

if __name__ == '__main__':
   app.run(host='0.0.0.0', port=5000, debug=True)
