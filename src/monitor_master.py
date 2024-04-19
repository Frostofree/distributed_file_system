import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MasterHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if os.path.basename(event.src_path) == 'master.py':
            os.system('cls' if os.name == 'nt' else 'clear')
            print("master.py has changed, rerunning it.")
            subprocess.run(['python', 'master.py'])

if __name__ == "__main__":
    observer = Observer()
    event_handler = MasterHandler()
    observer.schedule(event_handler, path='.', filter=lambda path: os.path.basename(path) == 'master.py', recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
