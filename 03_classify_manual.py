import sys
import os
import sqlite3
import json
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from functools import partial
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QLabel
from PyQt5.QtCore import Qt, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtGui import QKeyEvent
import argparse
from random import randrange
from dotenv import load_dotenv

load_dotenv()

# parser stuff
parser = argparse.ArgumentParser(description='Manual classifier with keyboard controls')
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
print(f"Percent Done (12hr): {round(len(time_status) / 720 * 100, 2)}%")
print(f"Percent Done (24hr): {round(len(time_status) / 1440 * 100, 2)}%")

# Start local HTTP server to serve GUI files (needed for Google Maps API key referrer restrictions)
LOCAL_PORT = 8766
handler = partial(SimpleHTTPRequestHandler, directory=script_dir)
httpd = HTTPServer(("localhost", LOCAL_PORT), handler)
threading.Thread(target=httpd.serve_forever, daemon=True).start()


class TimeBasedViewer(QMainWindow):
    def __init__(self, hour=1, minute=0, limit=6):
        super().__init__()
        self.setWindowTitle("Time-Based HTML Viewer (Manual)")
        self.setGeometry(100, 100, 1200, 1000)

        self.current_hour = int(hour)
        self.current_minute = int(minute)
        self.current_row_index = 0
        self.current_rows = []
        self.visited_rows = []
        self.visited_rows_index = None
        self.limit = int(limit)

        # Database / paths
        self.db_path = db_path
        self.html_path = os.path.join(script_dir, "03_classify_gui.html")

        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        layout.setStretch(1, 1)

        # Create status label
        self.status_label = QLabel()
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setMaximumHeight(150)
        layout.addWidget(self.status_label)

        # Create web view
        self.web_view = QWebEngineView()
        self.web_view.loadFinished.connect(self.on_load_finished)
        self.web_view.settings().setAttribute(self.web_view.settings().LocalContentCanAccessRemoteUrls, True)
        layout.addWidget(self.web_view)

        # Load initial data
        self.load_current_time_data()

        # Set focus policy to receive keyboard events
        self.setFocusPolicy(Qt.StrongFocus)

    def in_history(self):
        if self.visited_rows_index is None:
            return False
        else:
            return self.visited_rows_index < len(self.visited_rows) - 1

    def keyPressEvent(self, event: QKeyEvent):
        if event.text() == "c":
            self.next_row()
        elif event.text() == "n":
            self.next_time()
        elif event.text() == "l":
            self.update_approval(True)
        elif event.text() == "a":
            self.update_approval(False)
        elif event.text() == "b":
            self.last_row()

    def row_to_dict(self, row):
        """Convert a database row to a dictionary."""
        columns = ['id', 'panoramaId', 'text', 'ocrYaw', 'ocrPitch',
                  'ocrWidth', 'ocrHeight', 'lat', 'lng', 'heading',
                  'pitch', 'roll', 'approved']
        return {columns[i]: value for i, value in enumerate(row)}

    def get_time_string(self):
        """Format current hour and minute as a string."""
        return f"{self.current_hour}{str(self.current_minute).zfill(2)}"

    def get_time_variants(self):
        """Return both '432' and '0432' style variants for the current time."""
        base = self.get_time_string()
        padded = base.zfill(4)
        return list(set([base, padded]))

    def query_database(self, time_variants):
        """Query the database for the current time variants."""
        try:
            placeholders = ",".join("?" * len(time_variants))
            cursor.execute(f"SELECT * FROM panoramas WHERE text IN ({placeholders})", time_variants)
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.status_label.setText(f"Database error: {str(e)}")
            return []

    def _count_approved(self):
        time_variants = self.get_time_variants()
        placeholders = ",".join("?" * len(time_variants))
        cursor.execute(f"SELECT COUNT(*) FROM panoramas WHERE text IN ({placeholders}) AND approved IN ('auto_approved', 'manual_approved')", time_variants)
        return cursor.fetchone()[0]

    def load_current_time_data(self):
        """Load data for current time from database."""
        time_string = self.get_time_string()
        self.current_rows = self.query_database(self.get_time_variants())
        self.current_row_index = 0
        self.visited_rows = []
        self.visited_rows_index = None

        if self.current_rows:
            self.load_current_row()
        else:
            self.status_label.setText(f"No data found for time {time_string}")

    def load_current_row(self):
        if len(self.visited_rows) == 0:
            self.visited_rows.append(self.current_row_index)
            self.visited_rows_index = 0
        """Load the current row data and update the view."""
        if 0 <= self.current_row_index < len(self.current_rows):
            row = self.current_rows[self.current_row_index]

            # skip if someone has checked this
            if row[0] in all_checked_ids:
                self.next_row()
                return

            if os.path.exists(self.html_path):
                url = QUrl(f"http://localhost:{LOCAL_PORT}/03_classify_gui.html")
                self.web_view.setUrl(url)
                row_dict = self.row_to_dict(row)
                self.current_row_data = json.dumps(row_dict)
                time_string = self.get_time_string()
                self.status_label.setText(
                    f"Time: {time_string}\n\nPress [A] to reject\nPress [L] to approve\nPress [B] to go back\nPress [C] for next row\nPress [N] for next time"
                )

    def on_load_finished(self, ok):
        """Called when the page finishes loading."""
        if ok and hasattr(self, 'current_row_data'):
            js_code = f"window.rowData = {self.current_row_data};"
            self.web_view.page().runJavaScript(js_code)
            self.web_view.page().runJavaScript(
                "if (typeof onPythonVariableSet === 'function') { onPythonVariableSet(); }"
            )

    def update_approval(self, approved=False):
        row = self.current_rows[self.current_row_index]
        current_id = row[0]
        status = "manual_approved" if approved else "manual_rejected"
        cursor.execute("UPDATE panoramas SET approved = ? WHERE id = ?", (status, current_id))
        all_checked_ids.add(current_id)
        conn.commit()
        print(f"{status} id={current_id}")
        self.next_row()

    def last_row(self):
        if self.visited_rows_index is not None:
            if len(self.visited_rows) > 0:
                self.visited_rows_index = self.visited_rows_index - 1
                self.current_row_index = self.visited_rows[self.visited_rows_index]
                self.load_current_row()

    def next_row(self):
        """Move to the next row in the current time results."""
        # check if we've hit the limit
        if self._count_approved() >= self.limit:
            self.next_time()
            return

        # check if we are in history
        if self.in_history():
            self.visited_rows_index = self.visited_rows_index + 1
            self.current_row_index = self.visited_rows[self.visited_rows_index]
            self.load_current_row()
        elif self.current_rows:
            self.current_row_index = randrange(len(self.current_rows)) - 1
            self.visited_rows.append(self.current_row_index)
            self.visited_rows_index = self.visited_rows_index + 1
            self.load_current_row()

    def next_time(self):
        """Move to the next time increment."""
        self.current_minute += 1
        if self.current_minute > 60:
            self.current_minute = 1
            self.current_hour += 1
            if self.current_hour > 12:
                self.current_hour = 1

        self.load_current_time_data()


def main():
    app = QApplication(sys.argv)
    args = parser.parse_args()
    print(args)
    viewer = TimeBasedViewer(
        hour=args.hour if args.hour else 1,
        minute=args.minute if args.minute else 0,
        limit=args.limit if args.limit else 6
    )
    viewer.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
