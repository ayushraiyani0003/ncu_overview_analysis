import requests
import json
import sys
from datetime import datetime

def make_request1():
    """Make request to /master endpoint"""
    url = "http://192.168.10.132:5000/master"
    
    try:
        print(f"Making request to: {url}")
        print("Attempting connection...")
        
        # Add timeout to prevent hanging
        response = requests.get(url, timeout=10)
        
        print(f"Request 1 Status Code: {response.status_code}")
        print("Request 1 Response Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        
        print("Request 1 Response Data:")
        print("-" * 50)
        
        # Try to parse as JSON, fallback to text if not JSON
        try:
            data = response.json()
            print(json.dumps(data, indent=2))
        except json.JSONDecodeError:
            print("Response is not JSON, displaying as text:")
            print(response.text)
        
        return response
        
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error in request 1: {e}")
        print("Check if the server is running and accessible")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout error in request 1: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error in request 1: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error in request 1: {e}")
        return None

def make_request2():
    """Make request to /data endpoint with parameters"""
    url = "http://192.168.10.132:5000/data"
    params = {
        "project_db": "sunchaser_mtk_2025",
        "start": "2025-03-30 18:30:00",
        "end": "2025-03-31 18:30:00"
    }
    
    try:
        print(f"\nMaking request to: {url}")
        print(f"Parameters: {params}")
        print("Attempting connection...")
        
        # Add timeout to prevent hanging
        response = requests.get(url, params=params, timeout=10)
        
        print(f"Request 2 Status Code: {response.status_code}")
        print("Request 2 Response Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        
        print("Request 2 Response Data:")
        print("-" * 50)
        
        # Try to parse as JSON, fallback to text if not JSON
        try:
            data = response.json()
            print(json.dumps(data["count"], indent=2))
        except json.JSONDecodeError:
            print("Response is not JSON, displaying as text:")
            print(response.text)
        
        return response
        
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error in request 2: {e}")
        print("Check if the server is running and accessible")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout error in request 2: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error in request 2: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error in request 2: {e}")
        return None

def main():
    """Main function to execute both requests"""
    print("=" * 60)
    print("CLOUD API REQUESTS")
    print("=" * 60)
    
    # Check if requests library is available
    try:
        print(f"Using requests library version: {requests.__version__}")
    except AttributeError:
        print("Requests library version not available")
    
    # Make first request
    print("\n🔄 Starting Request 1...")
    response1 = make_request1()
    
    # Make second request
    print("\n🔄 Starting Request 2...")
    response2 = make_request2()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if response1:
        print(f"✅ Request 1 (/master): Success - Status {response1.status_code}")
    else:
        print("❌ Request 1 (/master): Failed")
    
    if response2:
        print(f"✅ Request 2 (/data): Success - Status {response2.status_code}")
    else:
        print("❌ Request 2 (/data): Failed")
    
    print(f"\nScript completed at: {datetime.now()}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error in main: {e}")
        sys.exit(1)