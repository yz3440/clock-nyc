import sys
import os
import sqlite3
import json
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QLabel
from PyQt5.QtCore import Qt, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtGui import QKeyEvent
from urllib.parse import urlparse
import argparse
import psycopg2
from random import randrange, uniform
from dotenv import load_dotenv
load_dotenv()  

#parser stuff
parser = argparse.ArgumentParser(description='My script description')
parser.add_argument('-hr', '--hour', help='set start hour')
parser.add_argument('-m', '--minute', help='set start minute')
parser.add_argument('-l', '--limit', help='set how many pics we want for each minute before going to the next one')

#db connections
## local one
conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.db"))
cursor = conn.cursor()
## postgres one
p = urlparse(os.getenv('POSTGRES_URL'))

pg_connection_dict = {
    'dbname': p.path[1:],
    'user': p.username,
    'password': p.password,
    'port': p.port,
    'host': p.hostname
}

print(pg_connection_dict)
pg_conn = psycopg2.connect(**pg_connection_dict)
print(pg_conn)
pg_cursor = pg_conn.cursor()
# pg_cursor.execute("DROP TABLE panoramas")
# pg_cursor.execute("""CREATE TABLE IF NOT EXISTS panoramas (
#   "id" INTEGER,
#   "panorama_id" TEXT,
#   "text" INTEGER,
#   "ocr_yaw" REAL,
#   "ocr_pitch" REAL,
#   "ocr_width" REAL,
#   "ocr_height" REAL,
#   "lat" REAL,
#   "lng" REAL,
#   "heading" REAL,
#   "pitch" REAL,
#   "roll" REAL, 
#   "approved" BOOL)""")

# pg_cursor.execute("select COLUMN_NAME from information_schema.columns where table_name = 'panoramas' ORDER BY ordinal_position")
pg_cursor.execute("select * from panoramas")

all_checked_panoramas = pg_cursor.fetchall()
print(all_checked_panoramas)
all_checked_ids = [tup[0] for tup in all_checked_panoramas]

# print current status
pg_cursor.execute("SELECT text, COUNT(text) as count_text FROM panoramas WHERE approved = True GROUP BY text ORDER BY text ASC")
time_status = pg_cursor.fetchall()
for time in time_status:
    print(time[0], time[1])
print("time | num approved ^^^^")
print(f"Percent Done (12hr): {round(len(time_status) / 720 * 100, 2)}%")
print(f"Percent Done (24hr): {round(len(time_status) / 1440 * 100, 2)}%")

class TimeBasedViewer(QMainWindow):
    def __init__(self, hour = 1, minute = 0, limit = 6):
        super().__init__()
        self.setWindowTitle("Time-Based HTML Viewer")
        self.setGeometry(100, 100, 1200, 1000)

        # Initialize time-related variables
        self.current_hour = int(hour)
        self.current_minute = int(minute)
        self.current_row_index = 0


        self.current_rows = []
        self.visited_rows = []
        self.visited_rows_index = None 
        self.limit = int(limit)
        

        # Set how many of each one we want to check

        # Database setup
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.db")
        
        # Get the absolute path to index.html
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.html_path = os.path.join(current_dir, "gui.html")

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
        print(row)
        """Convert a database row to a dictionary"""
        # Adjust these column names to match your actual database schema
        columns = ['id', 'panoramaId', 'text', 'ocrYaw', 'ocrPitch', 
                  'ocrWidth', 'ocrHeight', 'lat', 'lng', 'heading', 
                  'pitch', 'roll', 'approved']
        return {columns[i]: value for i, value in enumerate(row)}

    def get_time_string(self):
        """Format current hour and minute as a string"""
        return f"{self.current_hour}{str(self.current_minute).zfill(2)}"

    def query_database(self, time_string):
        """Query the database for the current time string"""
        try:
            # conn = sqlite3.connect(self.db_path)
            # cursor = conn.cursor()
            cursor.execute("SELECT * FROM panoramas WHERE text = ?", (time_string,))
            rows = cursor.fetchall()
            # conn.close()
            return rows
        except sqlite3.Error as e:
            self.status_label.setText(f"Database error: {str(e)}")
            return []

    def load_current_time_data(self):
        """Load data for current time from database"""
        time_string = self.get_time_string()
        self.current_rows = self.query_database(time_string)
        self.current_row_index = 0
        
        if self.current_rows:
            self.load_current_row()
        else:
            self.status_label.setText(f"No data found for time {time_string}")

    def load_current_row(self):
        
        if len(self.visited_rows) == 0:
            self.visited_rows.append(self.current_row_index)
            self.visited_rows_index = 0 
        """Load the current row data and update the view"""
        if 0 <= self.current_row_index < len(self.current_rows):
            row = self.current_rows[self.current_row_index]

            # skip if someone has checked this
            if row[0] in all_checked_ids:
                self.next_row()
                return

            if os.path.exists(self.html_path):
                url = QUrl.fromLocalFile(self.html_path)
                self.web_view.setUrl(url)
                # Convert row to dictionary and then to JSON
                row_dict = self.row_to_dict(row)
                self.current_row_data = json.dumps(row_dict)
                time_string = self.get_time_string()
                self.status_label.setText(
                    f"Time: {time_string}\n\nPress [A] to reject\nPress [L] to approve\nPress [B] to go back (does not work across minutes)\n\nPrefer clarity + weirdness.\nTry not to do more than 1 per minute of the following: transit lines, markings on cars/trucks, street numbers, etc.\nyou'll get the gist"
                )




    def on_load_finished(self, ok):
        """Called when the page finishes loading"""
        if ok and hasattr(self, 'current_row_data'):
            print("sending this data: ", self.current_row_data)
            # Pass the JSON string to JavaScript
            js_code = f"window.rowData = {self.current_row_data};"
            self.web_view.page().runJavaScript(js_code)
            self.web_view.page().runJavaScript(
                "if (typeof onPythonVariableSet === 'function') { onPythonVariableSet(); }"
            )

    def update_approval(self, approved = False):
        print(self.current_rows[self.current_row_index])
        cursor.execute("UPDATE panoramas SET approved = ? WHERE id = ?", (approved, self.current_rows[self.current_row_index][0]))
        # check if we're in history, if we are, we're updating
        if self.in_history():
            pg_cursor.execute("UPDATE panoramas SET approved = %s WHERE id = %s", ([(approved,), (self.current_rows[self.current_row_index][0], )]))
            # pg_cursor.execute("UPDATE panoramas (id, panorama_id, text, ocr_yaw, ocr_pitch, ocr_width, ocr_height, lat, lng, heading, pitch, roll, approved) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", self.current_rows[self.current_row_index][:-1] + (approved,))
        
        # if we're not, we're inserting
        else:
            pg_cursor.execute("INSERT INTO panoramas (id, panorama_id, text, ocr_yaw, ocr_pitch, ocr_width, ocr_height, lat, lng, heading, pitch, roll, approved) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", self.current_rows[self.current_row_index][:-1] + (approved,))
        self.next_row()
        pg_conn.commit()
        conn.commit()

    def last_row(self):
        # check if we're at the beginning of our history
        if self.visited_rows_index is not None:
            if len(self.visited_rows) > 0:
                self.visited_rows_index = self.visited_rows_index - 1
                self.current_row_index = self.visited_rows[self.visited_rows_index]
                self.load_current_row()
    

    def next_row(self):
        """Move to the next row in the current time results"""

        # check if we've hit the limit
        pg_cursor.execute(f"SELECT COUNT(*) FROM panoramas WHERE text = {self.get_time_string()} AND approved = True")
        if pg_cursor.fetchone()[0] > self.limit:
            self.next_time()
            return
        # check if we are in history
        elif self.in_history():
            self.visited_rows_index  = self.visited_rows_index + 1
            self.current_row_index = self.visited_rows[self.visited_rows_index]
            self.load_current_row()

        elif self.current_rows:
            self.current_row_index = randrange(len(self.current_rows)) -1
            self.visited_rows.append(self.current_row_index)
            self.visited_rows_index = self.visited_rows_index + 1
            # self.current_row_index = (self.current_row_index + 1) % len(self.current_rows)
            self.load_current_row()
        

    def next_time(self):
        """Move to the next time increment"""
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
    # print(args.hour)
    viewer = TimeBasedViewer(hour = args.hour if args.hour else 1, minute = args.minute if args.minute else 0, limit = args.limit if args.limit else 6)
    viewer.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()