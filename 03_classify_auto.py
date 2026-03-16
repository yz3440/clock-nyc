import sys
import os
import sqlite3
import json
import base64
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from functools import partial
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                              QLabel, QGridLayout)
from PyQt5.QtCore import Qt, QUrl, QTimer, QBuffer, QIODevice
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtGui import QKeyEvent, QPixmap
import argparse
from random import randrange, shuffle
from threading import Thread
import time as time_module
from queue import Queue
import queue
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# parser stuff
parser = argparse.ArgumentParser(description='Auto-classifier using GPT vision')
parser.add_argument('-hr', '--hour', help='set start hour')
parser.add_argument('-m', '--minute', help='set start minute')
parser.add_argument('-l', '--limit', help='set how many pics we want for each minute before going to the next one')

# db connection (local only)
script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "data", "process.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# get all already-checked panoramas from local db (where approved is not null)
cursor.execute("SELECT id FROM panoramas WHERE approved IS NOT NULL")
all_checked_panoramas = cursor.fetchall()
all_checked_ids = set(tup[0] for tup in all_checked_panoramas)

# print current status
cursor.execute("SELECT text, COUNT(text) as count_text FROM panoramas WHERE approved IN ('auto_approved', 'manual_approved') GROUP BY text ORDER BY text ASC")
time_status = cursor.fetchall()
for time in time_status:
    print(time[0], time[1])
print("time | num approved ^^^^")
print(f"Percent Done (24hr): {round(len(time_status) / 1440 * 100, 2)}%")

# OpenAI client for auto-classification
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Start local HTTP server to serve GUI files (needed for Google Maps API key referrer restrictions)
LOCAL_PORT = 8767
handler = partial(SimpleHTTPRequestHandler, directory=script_dir)
httpd = HTTPServer(("localhost", LOCAL_PORT), handler)
threading.Thread(target=httpd.serve_forever, daemon=True).start()

NUM_PANES = 9


class ClassifierPane:
    """One of 9 independent classification slots, each with its own web view and status."""

    def __init__(self, pane_index, viewer):
        self.pane_index = pane_index
        self.viewer = viewer  # reference to parent TimeBasedViewer

        # Per-pane state
        self.current_row_index = None
        self.current_row_data = None
        self.auto_classifying = False
        self.current_row_id = None  # DB id of the row currently loaded

        # Status label
        self.status_label = QLabel(f"Pane {pane_index + 1}: idle")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setMaximumHeight(30)
        self.status_label.setStyleSheet("QLabel { background-color: #f0f0f0; border-radius: 5px; padding: 5px; }")

        # Web view
        self.web_view = QWebEngineView()
        self.web_view.settings().setAttribute(
            self.web_view.settings().LocalContentCanAccessRemoteUrls, True)
        self.web_view.loadFinished.connect(self.on_load_finished)

        # Container widget
        self.container = QWidget()
        layout = QVBoxLayout(self.container)
        layout.setContentsMargins(2, 2, 2, 2)
        layout.addWidget(self.status_label)
        layout.addWidget(self.web_view)

        # Auto-classify timer
        self.auto_classify_timer = QTimer()
        self.auto_classify_timer.setSingleShot(True)
        self.auto_classify_timer.timeout.connect(self.auto_classify)

        # Spinner
        self.spinner_frames = ['|', '/', '-', '\\']
        self.spinner_index = 0
        self.spinner_timer = QTimer()
        self.spinner_timer.timeout.connect(self.update_spinner)

    def load_row(self, row_index):
        """Load a specific row from current_rows into this pane."""
        # Release previous in-flight id
        if self.current_row_id is not None:
            self.viewer.in_flight_ids.discard(self.current_row_id)

        self.current_row_index = row_index
        row = self.viewer.current_rows[row_index]
        self.current_row_id = row[0]
        self.viewer.in_flight_ids.add(self.current_row_id)

        row_dict = self.viewer.row_to_dict(row)
        self.current_row_data = json.dumps(row_dict)
        print(f"[Pane {self.pane_index}] Loading row id={row[0]}")

        url = QUrl(f"http://localhost:{LOCAL_PORT}/03_classify_gui.html")
        self.web_view.setUrl(url)

        self.status_label.setText(f"Pane {self.pane_index + 1}: loading...")
        self.status_label.setStyleSheet("QLabel { background-color: #f0f0f0; border-radius: 5px; padding: 5px; }")

    def on_load_finished(self, ok):
        if ok and self.current_row_data:
            js_code = f"window.rowData = {self.current_row_data};"
            self.web_view.page().runJavaScript(js_code)
            self.web_view.page().runJavaScript(
                "if (typeof onPythonVariableSet === 'function') { onPythonVariableSet(); }"
            )
            # Start auto-classify timer
            self.auto_classify_timer.stop()
            self.auto_classifying = False
            self.spinner_timer.stop()
            self.status_label.setText(f"Pane {self.pane_index + 1}: waiting for Street View...")
            self.status_label.setStyleSheet("QLabel { background-color: #f0f0f0; border-radius: 5px; padding: 5px; }")
            self.auto_classify_timer.start(2000)

    def update_spinner(self):
        self.spinner_index = (self.spinner_index + 1) % len(self.spinner_frames)
        frame = self.spinner_frames[self.spinner_index]
        self.status_label.setText(f"Pane {self.pane_index + 1}: classifying... {frame}")

    def auto_classify(self):
        self.auto_classifying = True
        self.status_label.setText(f"Pane {self.pane_index + 1}: classifying... |")
        self.status_label.setStyleSheet("QLabel { background-color: #fff7e6; border-radius: 5px; padding: 5px; }")
        self.spinner_timer.start(150)

        # Screenshot
        pixmap = self.web_view.grab()
        buffer = QBuffer()
        buffer.open(QIODevice.WriteOnly)
        pixmap.save(buffer, "PNG")
        image_bytes = buffer.data().data()
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        time_string = self.viewer.get_time_string()

        # Check approved count for stricter prompt
        approved_count = self.viewer._count_approved()

        prompt = f'This is an image of a Google Street View screen. Your job is to determine if the number {time_string} is clearly visible and front-and-center in the image. Reply "YES" if it is, and "NO" if it is not. Do not ever reply with anything else.'
        self.using_strict_prompt = approved_count > 15 and not self.viewer.strict_prompt_disabled
        if self.using_strict_prompt:
            prompt += ' Respond NO if the number is in the context of transit lines, markings on cars/trucks, or street numbers, or building identifiers.'

        pane_index = self.pane_index
        command_queue = self.viewer.command_queue

        def classify_thread():
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-5-nano",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{image_b64}"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }
                    ],
                    max_completion_tokens=1024
                )
                msg = response.choices[0].message
                content = msg.content or ""
                result = content.strip().upper()
                print(f"[Pane {pane_index}] GPT result: '{result}' (finish_reason={response.choices[0].finish_reason})")
                command_queue.put(("classify_result", pane_index, result))
            except Exception as e:
                print(f"[Pane {pane_index}] Auto-classify error: {e}")
                command_queue.put(("classify_result", pane_index, "ERROR"))

        Thread(target=classify_thread, daemon=True).start()

    def on_classify_result(self, result):
        self.spinner_timer.stop()

        if not self.auto_classifying:
            return
        self.auto_classifying = False

        if "YES" in result:
            self.status_label.setText(f"Pane {self.pane_index + 1}: YES")
            self.status_label.setStyleSheet("QLabel { background-color: #4CAF50; color: white; border-radius: 5px; padding: 5px; font-weight: bold; }")
            QTimer.singleShot(1000, lambda: self.commit_and_next(True))
        elif "NO" in result:
            self.status_label.setText(f"Pane {self.pane_index + 1}: NO")
            self.status_label.setStyleSheet("QLabel { background-color: #f44336; color: white; border-radius: 5px; padding: 5px; font-weight: bold; }")
            QTimer.singleShot(1000, lambda: self.commit_and_next(False))
        else:
            print(f"[Pane {self.pane_index}] Unrecognized result: '{result}', treating as NO")
            self.status_label.setText(f"Pane {self.pane_index + 1}: ??? — skip")
            self.status_label.setStyleSheet("QLabel { background-color: #ff9800; color: white; border-radius: 5px; padding: 5px; }")
            QTimer.singleShot(1000, lambda: self.commit_and_next(False))

    def commit_and_next(self, approved):
        """Commit the approval to DB and request next row from viewer."""
        if self.current_row_index is not None:
            row = self.viewer.current_rows[self.current_row_index]
            current_id = row[0]
            status = "auto_approved" if approved else "auto_rejected"
            cursor.execute("UPDATE panoramas SET approved = ? WHERE id = ?", (status, current_id))
            all_checked_ids.add(current_id)
            conn.commit()
            print(f"[Pane {self.pane_index}] {status} id={current_id}")

            # Track strict prompt rejection streak
            if getattr(self, 'using_strict_prompt', False):
                if not approved:
                    self.viewer.strict_reject_streak += 1
                    if self.viewer.strict_reject_streak >= 20:
                        self.viewer.strict_prompt_disabled = True
                        print(f"[Viewer] Strict prompt disabled after {self.viewer.strict_reject_streak} straight rejections")
                else:
                    self.viewer.strict_reject_streak = 0

        # Release in-flight
        if self.current_row_id is not None:
            self.viewer.in_flight_ids.discard(self.current_row_id)
            self.current_row_id = None

        # Request next row
        self.viewer.load_next_for_pane(self)

    def stop(self):
        """Stop all timers for this pane."""
        self.auto_classify_timer.stop()
        self.spinner_timer.stop()
        self.auto_classifying = False


class TimeBasedViewer(QMainWindow):
    def __init__(self, hour=1, minute=0, limit=6):
        super().__init__()
        self.setWindowTitle("Time-Based HTML Viewer (9-pane)")
        self.setGeometry(50, 50, 2400, 1400)

        self.current_hour = int(hour)
        self.current_minute = int(minute)
        self.limit = int(limit)
        self.current_rows = []
        self.in_flight_ids = set()  # panorama IDs currently loaded in panes
        self.strict_prompt_disabled = False  # set True if 20 straight rejections with strict prompt
        self.strict_reject_streak = 0  # consecutive rejections while strict prompt is active

        self.command_queue = Queue()
        self.startTimer(100)

        # Database / paths
        self.db_path = db_path
        self.html_path = os.path.join(script_dir, "03_classify_gui.html")

        # Layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        grid = QGridLayout(central_widget)
        grid.setSpacing(4)

        # Top status label
        self.global_status = QLabel()
        self.global_status.setAlignment(Qt.AlignCenter)
        self.global_status.setMaximumHeight(40)
        self.global_status.setStyleSheet("QLabel { font-size: 14px; font-weight: bold; }")
        grid.addWidget(self.global_status, 0, 0, 1, 3)

        # Create 9 panes in 3x3 grid
        self.panes = []
        for i in range(NUM_PANES):
            pane = ClassifierPane(i, self)
            self.panes.append(pane)
            row = 1 + i // 3
            col = i % 3
            grid.addWidget(pane.container, row, col)

        self.setFocusPolicy(Qt.StrongFocus)

        # Load initial data
        self.load_current_time_data()

    def timerEvent(self, event):
        try:
            command = self.command_queue.get_nowait()
            if isinstance(command, tuple) and command[0] == "classify_result":
                _, pane_index, result = command
                if 0 <= pane_index < len(self.panes):
                    self.panes[pane_index].on_classify_result(result)
        except queue.Empty:
            pass

    def keyPressEvent(self, event: QKeyEvent):
        if event.text() == "n":
            # Stop all panes and advance time
            for pane in self.panes:
                pane.stop()
            self.next_time()

    def row_to_dict(self, row):
        columns = ['id', 'panoramaId', 'text', 'ocrYaw', 'ocrPitch',
                   'ocrWidth', 'ocrHeight', 'lat', 'lng', 'heading',
                   'pitch', 'roll', 'approved']
        return {columns[i]: value for i, value in enumerate(row)}

    def get_time_string(self):
        return f"{self.current_hour}{str(self.current_minute).zfill(2)}"

    def get_time_variants(self):
        """Return both '432' and '0432' style variants for the current time."""
        base = self.get_time_string()
        padded = base.zfill(4)
        return list(set([base, padded]))

    def query_database(self, time_variants):
        try:
            placeholders = ",".join("?" * len(time_variants))
            cursor.execute(f"SELECT * FROM panoramas WHERE text IN ({placeholders})", time_variants)
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.global_status.setText(f"Database error: {str(e)}")
            return []

    def _count_approved(self):
        time_variants = self.get_time_variants()
        placeholders = ",".join("?" * len(time_variants))
        cursor.execute(f"SELECT COUNT(*) FROM panoramas WHERE text IN ({placeholders}) AND approved IN ('auto_approved', 'manual_approved')", time_variants)
        return cursor.fetchone()[0]

    def update_global_status(self):
        time_string = self.get_time_string()
        approved = self._count_approved()
        self.global_status.setText(
            f"Time: {time_string}  |  Approved: {approved}/{self.limit}  |  "
            f"Available rows: {len(self.current_rows)}  |  Press [N] to skip time"
        )

    def load_current_time_data(self):
        time_string = self.get_time_string()
        self.current_rows = self.query_database(self.get_time_variants())
        self.in_flight_ids.clear()
        self.update_global_status()

        if not self.current_rows:
            self.global_status.setText(f"No data for time {time_string}, advancing...")
            QTimer.singleShot(200, self.next_time)
            return

        # Load a row into each pane
        for pane in self.panes:
            self.load_next_for_pane(pane)

    def pick_next_row_index(self):
        """Pick a random row that isn't checked and isn't in-flight. Returns index or None."""
        available = []
        for i, row in enumerate(self.current_rows):
            row_id = row[0]
            if row_id not in all_checked_ids and row_id not in self.in_flight_ids:
                available.append(i)
        if not available:
            return None
        shuffle(available)
        return available[0]

    def load_next_for_pane(self, pane):
        """Load the next available row into a pane, or check if time should advance."""
        # Check if we've hit the limit
        approved_count = self._count_approved()
        if approved_count >= self.limit:
            # Stop this pane; if all panes idle, advance time
            pane.stop()
            pane.status_label.setText(f"Pane {pane.pane_index + 1}: limit reached")
            pane.status_label.setStyleSheet("QLabel { background-color: #e0e0e0; border-radius: 5px; padding: 5px; }")
            if all(not p.auto_classifying and p.current_row_id is None for p in self.panes):
                self.next_time()
            return

        row_index = self.pick_next_row_index()
        if row_index is None:
            pane.stop()
            pane.status_label.setText(f"Pane {pane.pane_index + 1}: no more rows")
            pane.status_label.setStyleSheet("QLabel { background-color: #e0e0e0; border-radius: 5px; padding: 5px; }")
            # If all panes are idle, advance time
            if all(not p.auto_classifying and p.current_row_id is None for p in self.panes):
                self.next_time()
            return

        self.update_global_status()
        pane.load_row(row_index)

    def next_time(self):
        self.current_minute += 1
        if self.current_minute > 59:
            self.current_minute = 0
            self.current_hour += 1
            if self.current_hour > 23:
                self.current_hour = 0

        # Reset strict prompt state for new time
        self.strict_prompt_disabled = False
        self.strict_reject_streak = 0

        # Stop all panes before reloading
        for pane in self.panes:
            pane.stop()
            if pane.current_row_id is not None:
                self.in_flight_ids.discard(pane.current_row_id)
                pane.current_row_id = None

        self.load_current_time_data()


def main():
    app = QApplication(sys.argv)
    args = parser.parse_args()
    print(args)
    viewer = TimeBasedViewer(
        hour=args.hour if args.hour else 0,
        minute=args.minute if args.minute else 0,
        limit=args.limit if args.limit else 20
    )
    viewer.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
