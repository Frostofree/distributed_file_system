import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ClientHandler(FileSystemEventHandler):
    def __init__(self):
        self.process = None

    def on_modified(self, event):
        if os.path.basename(event.src_path) == 'client.py':
            self.rerun_script()

    def rerun_script(self):
        if self.process:
            self.process.terminate()  # Terminate the current running process
        os.system('cls' if os.name == 'nt' else 'clear')
        print("client.py has changed, rerunning it.")
        self.process = subprocess.Popen(['python', 'client.py'])

if __name__ == "__main__":
    observer = Observer()
    event_handler = ClientHandler()
    observer.schedule(event_handler, path='.', recursive=False)
    observer.start()

    print("Watching for changes. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    if event_handler.process:
        event_handler.process.terminate()  # Ensure the subprocess is terminated on exit
    observer.join()
