import sys
import socket
import threading
from collections import deque
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QComboBox, QLabel, QTabWidget, QFrame)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QObject
from PyQt6.QtCharts import QChart, QChartView, QLineSeries, QValueAxis
from PyQt6.QtGui import QPainter

MAX_DATA_POINTS = 60

class BrokerSignals(QObject):
    message_received = pyqtSignal(str, str)  # topic, message
    connection_status = pyqtSignal(str, str)  # topic, status

class ProducerChart(QWidget):
    def __init__(self, topic, tab_widget, main_window, parent=None):
        super().__init__(parent)
        self.topic = topic
        self.tab_widget = tab_widget
        self.main_window = main_window
        self.data = deque(maxlen=MAX_DATA_POINTS)
        
        self.chart = QChart()
        self.series = QLineSeries()
        self.chart.addSeries(self.series)
        
        self.axisX = QValueAxis()
        self.axisX.setRange(0, MAX_DATA_POINTS)
        self.axisX.setLabelFormat("%d")
        self.axisX.setTitleText("Time (5 sec intervals)")
        
        self.axisY = QValueAxis()
        self.setup_axis_y()
        
        self.chart.addAxis(self.axisX, Qt.AlignmentFlag.AlignBottom)
        self.chart.addAxis(self.axisY, Qt.AlignmentFlag.AlignLeft)
        self.series.attachAxis(self.axisX)
        self.series.attachAxis(self.axisY)
        
        self.chart.setTitle(f"Topic: {topic}")
        self.chart.legend().hide()
        
        self.chart_view = QChartView(self.chart)
        self.chart_view.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        self.info_label = QLabel()
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        layout = QVBoxLayout()
        layout.addWidget(self.chart_view)
        layout.addWidget(self.info_label)
        
        self.unsubscribe_btn = QPushButton("Unsubscribe")
        self.unsubscribe_btn.clicked.connect(self.unsubscribe)
        layout.addWidget(self.unsubscribe_btn)
        
        self.setLayout(layout)
    
    def setup_axis_y(self):
        if "cpu" in self.topic.lower():
            self.axisY.setRange(0, 100)
            self.axisY.setTitleText("CPU Usage (%)")
        elif "memory" in self.topic.lower():
            self.axisY.setRange(0, 100)
            self.axisY.setTitleText("Memory Usage (%)")
        elif "disk" in self.topic.lower():
            self.axisY.setRange(0, 100)
            self.axisY.setTitleText("Disk Usage (%)")
        elif "network" in self.topic.lower():
            self.axisY.setTitleText("Network Speed (KiB/s)")
        elif "processes" in self.topic.lower():
            self.axisY.setTitleText("Process Count")
        elif "load" in self.topic.lower():
            self.axisY.setTitleText("Load Average")
        else:
            self.axisY.setTitleText("Value")
    
    def unsubscribe(self):
        self.main_window.unsubscribe_from_topic(self.topic)
    
    def update_chart(self, value, text_info=None):
        self.data.append(value)
        self.series.clear()
        
        for i, val in enumerate(self.data):
            self.series.append(i, val)
        
        if "network" in self.topic.lower() or "processes" in self.topic.lower() or "load" in self.topic.lower():
            min_val = min(self.data) if self.data else 0
            max_val = max(self.data) if self.data else 1
            padding = (max_val - min_val) * 0.1
            self.axisY.setRange(min_val - padding, max_val + padding)
        
        if text_info:
            self.info_label.setText(text_info)
        else:
            self.info_label.setText(f"Current value: {value:.2f}")

class SubscriptionClient:
    def __init__(self, topic, main_window):
        self.topic = topic
        self.main_window = main_window
        self.sock = None
        self.running = False
        self.thread = None
        
    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(1)
            self.sock.connect(("127.0.0.1", 8080))
            self.running = True
            self.thread = threading.Thread(target=self.read_from_broker)
            self.thread.daemon = True
            self.thread.start()
            self.send_command(f"SUBSCRIBE {self.topic}")
            self.main_window.signals.connection_status.emit(self.topic, f"Connected to broker for {self.topic}")
            return True
        except Exception as e:
            self.main_window.signals.connection_status.emit(self.topic, f"Connection failed for {self.topic}: {str(e)}")
            self.running = False
            return False
    
    def send_command(self, command):
        if self.sock and self.running:
            try:
                self.sock.sendall(f"{command}\n".encode())
            except Exception as e:
                self.main_window.signals.connection_status.emit(self.topic, f"Send failed for {self.topic}: {str(e)}")
                self.running = False
    
    def read_from_broker(self):
        buffer = ""
        while self.running:
            try:
                data = self.sock.recv(4096)
                if not data:
                    self.main_window.signals.connection_status.emit(self.topic, f"Connection closed by broker for {self.topic}")
                    break
                
                buffer += data.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    self.main_window.signals.message_received.emit(self.topic, line.strip())
            except socket.timeout:
                continue
            except Exception as e:
                self.main_window.signals.connection_status.emit(self.topic, f"Error reading from broker for {self.topic}: {str(e)}")
                break
    
    def disconnect(self):
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1)
        if self.sock:
            try:
                self.send_command(f"UNSUBSCRIBE {self.topic}")
            finally:
                self.sock.close()

class BrokerClient(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Broker Client")
        self.setGeometry(100, 100, 800, 600)
        
        self.subscriptions = {}  # topic: {'chart': chart, 'client': client}
        self.running = False
        
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        
        self.setup_ui()
        self.setup_signals()
        
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.check_connections)
        self.update_timer.start(1000)
    
    def setup_ui(self):
        control_panel = QWidget()
        control_layout = QHBoxLayout(control_panel)
        
        self.topic_combo = QComboBox()
        self.topic_combo.addItems([
            "system.cpu",
            "system.memory",
            "system.disk",
            "system.network",
            "system.processes",
            "system.load"
        ])
        
        self.subscribe_btn = QPushButton("Subscribe")
        self.subscribe_btn.clicked.connect(self.subscribe_to_topic)
        
        self.status_label = QLabel("Ready to subscribe")
        
        control_layout.addWidget(QLabel("Topic:"))
        control_layout.addWidget(self.topic_combo)
        control_layout.addWidget(self.subscribe_btn)
        control_layout.addWidget(self.status_label)
        
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        
        self.main_layout.addWidget(control_panel)
        self.main_layout.addWidget(separator)
        self.main_layout.addWidget(self.tab_widget)
    
    def setup_signals(self):
        self.signals = BrokerSignals()
        self.signals.message_received.connect(self.process_broker_message)
        self.signals.connection_status.connect(self.update_status)
    
    def update_status(self, topic, message):
        if topic == "":
            self.status_label.setText(message)
        else:
            if topic in self.subscriptions:
                self.subscriptions[topic]['chart'].info_label.setText(message)
    
    def subscribe_to_topic(self):
        topic = self.topic_combo.currentText()
        if topic not in self.subscriptions:
            client = SubscriptionClient(topic, self)
            if client.connect():
                chart = ProducerChart(topic, self.tab_widget, self)
                self.subscriptions[topic] = {'chart': chart, 'client': client}
                self.tab_widget.addTab(chart, topic)
                self.tab_widget.setCurrentWidget(chart)
                self.signals.connection_status.emit("", f"Subscribed to {topic}")
        else:
            self.tab_widget.setCurrentWidget(self.subscriptions[topic]['chart'])
            self.signals.connection_status.emit("", f"Already subscribed to {topic}")
    
    def unsubscribe_from_topic(self, topic):
        if topic in self.subscriptions:
            subscription = self.subscriptions.pop(topic)
            subscription['client'].disconnect()
            chart = subscription['chart']
            index = self.tab_widget.indexOf(chart)
            if index >= 0:
                self.tab_widget.removeTab(index)
            chart.deleteLater()
            self.signals.connection_status.emit("", f"Unsubscribed from {topic}")
    
    def close_tab(self, index):
        widget = self.tab_widget.widget(index)
        for topic, sub in self.subscriptions.items():
            if sub['chart'] == widget:
                self.unsubscribe_from_topic(topic)
                break
    
    def check_connections(self):
        for topic, sub in list(self.subscriptions.items()):
            if not sub['client'].running:
                self.signals.connection_status.emit(topic, f"Connection lost for {topic}")
                self.reconnect_subscription(topic)
    
    def reconnect_subscription(self, topic):
        if topic in self.subscriptions:
            client = self.subscriptions[topic]['client']
            if not client.running:
                if client.connect():
                    self.signals.connection_status.emit(topic, f"Reconnected to {topic}")
                else:
                    self.signals.connection_status.emit(topic, f"Reconnection failed for {topic}")
    
    def process_broker_message(self, topic, message):
        if topic not in self.subscriptions:
            return
            
        if message.startswith("PUBLISH "):
            try:
                parts = message.split(" ", 2)
                if len(parts) < 3:
                    return
                    
                content = parts[2]
                self.update_chart_data(topic, content)
            except Exception as e:
                print(f"Error processing PUBLISH message for {topic}: {e}")
    
    def update_chart_data(self, topic, content):
        chart = self.subscriptions[topic]['chart']
        value = None
        text_info = None
        
        if topic == "system.cpu":
            if "CPU Usage:" in content:
                value = float(content.split(":")[1].strip().replace("%", ""))
        elif topic == "system.memory":
            if "Memory Usage:" in content:
                value = float(content.split(":")[1].strip().replace("%", ""))
        elif topic == "system.disk":
            if "Disk Usage:" in content:
                value = float(content.split(":")[1].strip().replace("%", ""))
        elif topic == "system.network":
            if "Network:" in content:
                parts = content.split("Network:")[1].split(",")
                rx = float(parts[0].split()[1])
                tx = float(parts[1].split()[1])
                value = max(rx, tx)
                text_info = f"RX: {rx:.2f} KiB/s, TX: {tx:.2f} KiB/s"
        elif topic == "system.processes":
            if "Process Count:" in content:
                value = int(content.split(":")[1].strip())
        elif topic == "system.load":
            if "Load Average:" in content:
                try:
                    # Обработка формата: "Load Average: 1m 0.15, 5m 0.10, 15m 0.05"
                    parts = content.split("Load Average:")[1].strip().split(",")
                    load1 = float(parts[0].strip().split()[1])
                    load5 = float(parts[1].strip().split()[1])
                    load15 = float(parts[2].strip().split()[1])
                    value = load1  # Используем 1-минутное среднее для графика
                    text_info = f"1m: {load1:.2f}, 5m: {load5:.2f}, 15m: {load15:.2f}"
                except (IndexError, ValueError) as e:
                    print(f"Error parsing load average for {topic}: {e}")
                    return
        
        if value is not None:
            chart.update_chart(value, text_info)
    
    def closeEvent(self, event):
        for topic in list(self.subscriptions.keys()):
            self.unsubscribe_from_topic(topic)
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    client = BrokerClient()
    client.show()
    sys.exit(app.exec())
