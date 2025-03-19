import schedule
import time
import subprocess
import logging
import os
import threading

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(threadName)s - %(levelname)s: %(message)s'
)

def run_publisher():
    try:
        logging.info("Running Publisher...")
        result = subprocess.run(
            ['python', '-m', 'services.publisher'], 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            logging.info("Publisher completed successfully")
            if result.stdout:
                logging.info(f"Publisher output: {result.stdout}")
        else:
            logging.error(f"Publisher failed. Error: {result.stderr}")
    except Exception as e:
        logging.error(f"Publisher execution error: {e}")

def run_subscriber():
    try:
        logging.info("Running Subscriber...")
        result = subprocess.run(
            ['python', '-m', 'services.subscriber'], 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            logging.info("Subscriber completed successfully")
            if result.stdout:
                logging.info(f"Subscriber output: {result.stdout}")
        else:
            logging.error(f"Subscriber failed. Error: {result.stderr}")
    except Exception as e:
        logging.error(f"Subscriber execution error: {e}")

def schedule_runner(run_function):
    """
    Wrapper to run scheduled functions in a thread-safe manner
    Prevents overlapping executions of the same job
    """
    def job():
        # Use a thread to run the function
        thread = threading.Thread(target=run_function)
        thread.start()
        thread.join(timeout=300)  # 5-minute timeout
        if thread.is_alive():
            logging.error(f"Job {run_function.__name__} timed out")
    return job

def main():
    # Ensure we're in the correct directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Run immediately on start
    publisher_thread = threading.Thread(target=run_publisher)
    subscriber_thread = threading.Thread(target=run_subscriber)
    
    publisher_thread.start()
    subscriber_thread.start()
    
    publisher_thread.join()
    subscriber_thread.join()

    # Schedule runs every 30 seconds
    schedule.every(20).seconds.do(schedule_runner(run_publisher))
    schedule.every(45).seconds.do(schedule_runner(run_subscriber))

    logging.info("CDC Pipeline continuous runner started...")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()