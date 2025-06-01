#!/usr/bin/env python3
"""
Standalone script to run the NCU Data Collection Service
Use this when you want to run the data collector independently of Streamlit
"""

import time
import signal
import sys
from data_collector import NCUDataCollectionService, logger, COLLECTION_CONFIG

# Global service instance
service = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if service:
        service.stop()
    sys.exit(0)

def main():
    """Main function to run the service"""
    global service
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
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