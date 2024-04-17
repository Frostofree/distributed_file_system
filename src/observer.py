import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        # Clear the screen
        os.system('cls' if os.name == 'nt' else 'clear')
        # Run the Python script
        subprocess.run(['python', './master.py'])

if __name__ == "__main__":
    file_path = 'master.py'

    # Create an event handler and observer
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=False)

    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
