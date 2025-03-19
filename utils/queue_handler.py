# import queue

# cdc_queue = queue.Queue()

# def publish_to_queue(data):
#     cdc_queue.put(data)

# def consume_from_queue():
#     if not cdc_queue.empty():
#         return cdc_queue.get()
#     return None
# import queue

# cdc_queue = queue.Queue()

# def publish_to_queue(data):
#     """Publish data to the queue."""
#     cdc_queue.put(data)

# def consume_from_queue():
#     """Consume and remove an item from the queue."""
#     if not cdc_queue.empty():
#         return cdc_queue.get()
#     return None

# def peek_queue():
#     """View the contents of the queue without removing items."""
#     # Create a copy of the queue to avoid modifying the original
#     temp_queue = queue.Queue()
#     queue_contents = []

#     while not cdc_queue.empty():
#         item = cdc_queue.get()
#         queue_contents.append(item)
#         temp_queue.put(item)

#     # Restore the original queue
#     while not temp_queue.empty():
#         cdc_queue.put(temp_queue.get())

#     return queue_contents

# def get_queue_size():
#     """Get the current size of the queue."""
#     return cdc_queue.qsize()

# def clear_queue():
#     """Clear all items from the queue."""
#     while not cdc_queue.empty():
#         cdc_queue.get()
import queue

from utils.logger import log_info, log_error

# Configure logging


# Global queue with logging
cdc_queue = queue.Queue()

def publish_to_queue(data):
    """Publish data to the queue with logging."""
    cdc_queue.put(data)
    log_info(f"Published to queue. Current size: {cdc_queue.qsize()}")

def consume_from_queue():
    """Consume and log queue items."""
    if not cdc_queue.empty():
        item = cdc_queue.get()
        log_info(f"Consumed from queue. Remaining size: {cdc_queue.qsize()}")
        return item
    return None

def print_queue_contents():
    """Print queue contents for debugging."""
    # Create a temporary queue to preserve original
    temp_queue = queue.Queue()
    contents = []

    while not cdc_queue.empty():
        item = cdc_queue.get()
        contents.append(item)
        temp_queue.put(item)

    # Restore original queue
    while not temp_queue.empty():
        cdc_queue.put(temp_queue.get())

    print("=== Queue Contents ===")
    for i, content in enumerate(contents, 1):
        print(f"Item {i}: {content}")
    print(f"Total Items: {len(contents)}")

    return contents