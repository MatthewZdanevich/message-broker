import unittest
import socket
from unittest.mock import MagicMock, patch, call
from PyQt6.QtWidgets import QApplication
import sys
from datetime import datetime
from collections import deque

# Инициализация QApplication перед импортом модулей, использующих Qt
app = QApplication(sys.argv)

from client import BrokerClient, SubscriptionClient, ProducerChart, MAX_DATA_POINTS

class TestProducerChart(unittest.TestCase):
    def setUp(self):
        self.topic = "system.cpu"
        self.tab_widget = MagicMock()
        self.main_window = MagicMock()
        self.chart = ProducerChart(self.topic, self.tab_widget, self.main_window)
    
    def test_initialization(self):
        self.assertEqual(self.chart.topic, self.topic)
        self.assertEqual(self.chart.data.maxlen, MAX_DATA_POINTS)
        self.assertEqual(len(self.chart.data), 0)
        self.assertEqual(self.chart.axisY.titleText(), "CPU Usage (%)")
        self.assertEqual(self.chart.axisY.min(), 0)
        self.assertEqual(self.chart.axisY.max(), 100)
    
    def test_update_chart(self):
        test_value = 50.5
        self.chart.update_chart(test_value)
        
        self.assertEqual(len(self.chart.data), 1)
        self.assertEqual(self.chart.data[0], test_value)
        self.assertEqual(self.chart.info_label.text(), f"Current value: {test_value:.2f}")
        
        # Проверка обновления с текстовой информацией
        text_info = "Additional info"
        self.chart.update_chart(test_value + 10, text_info)
        self.assertEqual(self.chart.info_label.text(), text_info)
    
    def test_unsubscribe(self):
        self.chart.unsubscribe()
        self.main_window.unsubscribe_from_topic.assert_called_once_with(self.topic)
    
    def test_save_logs(self):
        # Настройка моков для логов
        mock_logs = [
            {'timestamp': '2023-01-01 00:00:00', 'level': 'INFO', 'topic': 'system.cpu', 'message': 'Test message 1'},
            {'timestamp': '2023-01-01 00:00:01', 'level': 'ERROR', 'topic': 'system.memory', 'message': 'Test message 2'},
            {'timestamp': '2023-01-01 00:00:02', 'level': 'DEBUG', 'topic': 'system.cpu', 'message': 'Test message 3'},
        ]
        self.main_window.logger.logs = mock_logs
        
        with patch('client.QFileDialog.getSaveFileName', return_value=('test_logs.txt', '')):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                self.chart.save_logs()
                
                # Проверяем, что записаны только логи для system.cpu
                expected_calls = [
                    call('[2023-01-01 00:00:00] [INFO] Test message 1\n'),
                    call('[2023-01-01 00:00:02] [DEBUG] Test message 3\n')
                ]
                handle = mock_file()
                handle.write.assert_has_calls(expected_calls)
                
        self.main_window.logger.log.assert_called_with('INFO', 'Saved logs for topic system.cpu to test_logs.txt')

class TestSubscriptionClient(unittest.TestCase):
    def setUp(self):
        self.topic = "system.memory"
        self.main_window = MagicMock()
        self.main_window.signals = MagicMock()
        self.main_window.logger = MagicMock()
        self.client = SubscriptionClient(self.topic, self.main_window)
    
    @patch('socket.socket')
    def test_connect_failure(self, mock_socket):
        mock_sock = MagicMock()
        mock_socket.return_value = mock_sock
        mock_sock.connect.side_effect = ConnectionError("Connection failed")
        
        result = self.client.connect()
        
        self.assertFalse(result)
        self.assertFalse(self.client.running)
        self.main_window.signals.connection_status.emit.assert_called_once_with(
            self.topic, f"Connection failed for {self.topic}: Connection failed"
        )
        self.main_window.logger.log.assert_called_once_with('ERROR', "Connection failed: Connection failed", self.topic)
    
    @patch('socket.socket')
    def test_read_from_broker(self, mock_socket):
        mock_sock = MagicMock()
        mock_socket.return_value = mock_sock
        self.client.connect()
        
        # Эмулируем получение данных
        test_messages = [
            "PUBLISH system.memory 50.0\n",
            "PUBLISH system.memory 60.0\nEXTRA"
        ]
        mock_sock.recv.side_effect = [
            test_messages[0].encode(),
            test_messages[1].encode(),
            b""  # Эмулируем закрытие соединения
        ]
        
        # Запускаем поток вручную (обычно он запускается в connect)
        self.client.read_from_broker()
        
        # Проверяем, что сообщения были обработаны
        expected_calls = [
            call(self.topic, "PUBLISH system.memory 50.0"),
            call(self.topic, "PUBLISH system.memory 60.0")
        ]
        self.main_window.signals.message_received.emit.assert_has_calls(expected_calls)
        
        # Проверяем логи
        self.main_window.logger.log.assert_any_call('DEBUG', "Received: PUBLISH system.memory 50.0", self.topic)
        self.main_window.logger.log.assert_any_call('DEBUG', "Received: PUBLISH system.memory 60.0", self.topic)
        
        # Проверяем обработку закрытия соединения
        self.main_window.signals.connection_status.emit.assert_called_with(
            self.topic, f"Connection closed by broker for {self.topic}"
        )
        self.main_window.logger.log.assert_called_with('WARNING', "Connection closed by broker", self.topic)

class TestBrokerClient(unittest.TestCase):
    def setUp(self):
        self.client = BrokerClient()
        self.client.signals = MagicMock()
        self.client.logger = MagicMock()
    
    def test_initialization(self):
        self.assertEqual(self.client.windowTitle(), "Broker Client")
        self.assertEqual(len(self.client.subscriptions), 0)
        self.assertIsNotNone(self.client.logger)
        self.assertIsNotNone(self.client.tab_widget)
        self.assertIsNotNone(self.client.topic_combo)
        self.assertEqual(self.client.topic_combo.count(), 6)
    
    def test_subscribe_to_topic(self):
        test_topic = "system.cpu"
        self.client.topic_combo.setCurrentText(test_topic)
        
        with patch('client.SubscriptionClient') as mock_client:
            mock_instance = mock_client.return_value
            mock_instance.connect.return_value = True
            
            self.client.subscribe_to_topic()
            
            mock_client.assert_called_once_with(test_topic, self.client)
            self.assertEqual(self.client.tab_widget.count(), 1)
            self.client.signals.connection_status.emit.assert_called_once_with("", f"Subscribed to {test_topic}")
            self.client.logger.log.assert_called_once_with('INFO', f"Subscribed to topic {test_topic}")
    
    def test_unsubscribe_from_topic(self):
        test_topic = "system.memory"
        
        # Создаем моки для подписки
        mock_client = MagicMock()
        mock_chart = MagicMock()
        self.client.subscriptions[test_topic] = {'client': mock_client, 'chart': mock_chart}
        self.client.tab_widget.addTab = MagicMock()
        self.client.tab_widget.indexOf = MagicMock(return_value=0)
        self.client.tab_widget.removeTab = MagicMock()
        
        self.client.unsubscribe_from_topic(test_topic)
        
        self.assertNotIn(test_topic, self.client.subscriptions)
        mock_client.disconnect.assert_called_once()
        mock_chart.deleteLater.assert_called_once()
        self.client.tab_widget.removeTab.assert_called_once_with(0)
        self.client.signals.connection_status.emit.assert_called_once_with("", f"Unsubscribed from {test_topic}")
        self.client.logger.log.assert_called_once_with('INFO', f"Unsubscribed from topic {test_topic}")
    
    def test_process_broker_message(self):
        test_topic = "system.network"
        test_message = "PUBLISH system.network Network: RX: 100.0 KiB/s, TX: 150.0 KiB/s"
        
        # Создаем реальный chart вместо mock для проверки обновления данных
        chart = ProducerChart(test_topic, MagicMock(), self.client)
        self.client.subscriptions[test_topic] = {'chart': chart, 'client': MagicMock()}
        
        self.client.process_broker_message(test_topic, test_message)
        
        # Проверяем, что данные обновились
        self.assertEqual(len(chart.data), 1)
        self.assertEqual(chart.data[0], 150.0)
        self.assertEqual(chart.info_label.text(), "RX: 100.00 KiB/s, TX: 150.00 KiB/s")
        self.client.logger.log.assert_called_once_with('DEBUG', "Received data: Network: RX: 100.0 KiB/s, TX: 150.0 KiB/s", test_topic)

if __name__ == "__main__":
    unittest.main()
