import logging
from datetime import datetime

class ClientLogger:
    def __init__(self):
        self.logs = []
        self.setup_logging()
    
    def setup_logging(self):
        """Настройка базового логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('BrokerClient')
    
    def log(self, level, message, topic=None):
        """Запись лога с указанием уровня важности"""
        log_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'level': level,
            'topic': topic if topic else 'GLOBAL',
            'message': message
        }
        self.logs.append(log_entry)
        
        # Также пишем в стандартный логгер
        if level == 'INFO':
            self.logger.info(f"{topic if topic else ''} - {message}")
        elif level == 'WARNING':
            self.logger.warning(f"{topic if topic else ''} - {message}")
        elif level == 'ERROR':
            self.logger.error(f"{topic if topic else ''} - {message}")
        elif level == 'DEBUG':
            self.logger.debug(f"{topic if topic else ''} - {message}")
    
    def save_logs_to_file(self, filename):
        """Сохранение логов в файл"""
        try:
            with open(filename, 'w') as f:
                for log in self.logs:
                    f.write(f"[{log['timestamp']}] [{log['level']}] [{log['topic']}] {log['message']}\n")
            return True
        except Exception as e:
            self.log('ERROR', f"Failed to save logs: {str(e)}")
            return False
