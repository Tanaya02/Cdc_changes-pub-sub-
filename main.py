import multiprocessing
import subprocess

def run_publisher():
    subprocess.run(["python", "-m", "services.publisher"])

def run_subscriber():
    subprocess.run(["python", "-m", "services.subscriber"])

if __name__ == "__main__":
    p1 = multiprocessing.Process(target=run_publisher)
    p2 = multiprocessing.Process(target=run_subscriber)

    p1.start()
    p2.start()

    p1.join()
    p2.join()