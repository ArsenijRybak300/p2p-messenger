import socket
import threading
import json
import time
import sys
import os
import logging
from datetime import datetime

class NetworkMessenger:
    def __init__(self, config_file="config.json"):
        self.setup_logging()  # Настройка логирования
        self.load_config(config_file)
        self.running = True
        self.peers = {}
        self.messages = []
        self.peers_lock = threading.Lock()
        
    def setup_logging(self):
        """Настройка системы логирования - только в файл"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('network_messenger.log', encoding='utf-8'),
                # Убрали StreamHandler чтобы не выводить в консоль
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Инициализация сетевого мессенджера")
    
    def load_config(self, config_file):
        """Загрузка конфигурационных параметров"""
        default_config = {
            "host": "0.0.0.0",
            "port": 8888,
            "discovery_port": 8889
        }
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    self.config = {**default_config, **loaded_config}
                self.logger.info(f"Конфигурация загружена из {config_file}")
            else:
                self.config = default_config
                self.logger.info("Используются значения по умолчанию")
                
                # Сохраняем дефолтную конфигурацию
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=4, ensure_ascii=False)
                    
        except Exception as e:
            self.logger.error(f"Ошибка загрузки конфигурации: {e}")
            self.config = default_config
            
        print(f"Параметры: {self.config}")
    
    def start_tcp_server(self):
        """Инициализация TCP сервера для приема сообщений"""
        try:
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Автоподбор порта
            port = self.config['port']
            for p in range(port, port + 20):
                try:
                    self.tcp_socket.bind((self.config['host'], p))
                    self.config['port'] = p
                    self.logger.info(f"TCP сервер на порту {p}")
                    print(f"TCP сервер на порту {p}")
                    break
                except OSError as e:
                    if p == port + 19:
                        self.logger.error(f"Не удалось найти свободный порт: {e}")
                        print(f"Ошибка: не удалось найти свободный порт")
                        return False
                    continue
            
            self.tcp_socket.listen(5)
            self.tcp_socket.settimeout(1.0)
            
            server_thread = threading.Thread(target=self.accept_connections, daemon=True)
            server_thread.start()
            
            self.logger.info("TCP сервер запущен")
            print("TCP сервер запущен")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации TCP: {e}", exc_info=True)
            print(f"Ошибка инициализации TCP: {e}")
            return False
    
    def accept_connections(self):
        """Обработка входящих подключений"""
        self.logger.info("Начало приема подключений")
        while self.running:
            try:
                client_socket, address = self.tcp_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
                
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.error(f"Ошибка accept: {e}")
                break
    
    def handle_client(self, client_socket, address):
        """Обработка клиентского соединения"""
        try:
            client_socket.settimeout(5.0)
            data = client_socket.recv(4096)
            
            if data:
                try:
                    message_data = json.loads(data.decode('utf-8'))
                    text = message_data.get('text', '')
                    sender = message_data.get('sender', f'{address[0]}:{address[1]}')
                    timestamp = message_data.get('timestamp', datetime.now().isoformat())
                    
                    # Форматируем время
                    try:
                        msg_time = datetime.fromisoformat(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        msg_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    display_msg = f"[{msg_time}] {sender}: {text}"
                    print(f"\n{display_msg}")
                    self.messages.append(display_msg)
                    
                    # Сохраняем как активного пира
                    with self.peers_lock:
                        if ':' in sender:
                            host, port = sender.split(':')
                            try:
                                self.peers[(host, int(port))] = time.time()
                            except:
                                self.peers[(address[0], address[1])] = time.time()
                    
                    self.logger.info(f"Получено сообщение от {sender}")
                    
                except json.JSONDecodeError:
                    # Если не JSON, обрабатываем как plain text
                    text = data.decode('utf-8', errors='ignore')
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    display_msg = f"[{timestamp}] {address[0]}:{address[1]}: {text}"
                    print(f"\n{display_msg}")
                    self.messages.append(display_msg)
                    
                    with self.peers_lock:
                        self.peers[(address[0], address[1])] = time.time()
                    
                    self.logger.info(f"Получено plain text от {address}")
                    
        except Exception as e:
            self.logger.error(f"Ошибка обработки клиента {address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def start_discovery_service(self):
        """Служба обнаружения сетевых узлов"""
        try:
            self.udp_send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_send_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            
            self.udp_recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            self.udp_recv_socket.bind(('', self.config['discovery_port']))
            self.udp_recv_socket.settimeout(1.0)
            
            self.logger.info(f"UDP discovery на порту {self.config['discovery_port']}")
            print(f"UDP discovery на порту {self.config['discovery_port']}")
            
            broadcast_thread = threading.Thread(target=self.broadcast_presence, daemon=True)
            broadcast_thread.start()
            
            listen_thread = threading.Thread(target=self.listen_for_peers, daemon=True)
            listen_thread.start()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации UDP: {e}", exc_info=True)
            print(f"Ошибка инициализации UDP: {e}")
            return False
    
    def broadcast_presence(self):
        """Рассылка информации о текущем узле"""
        discovery_msg = {
            "type": "discovery",
            "host": self.config['host'],
            "port": self.config['port'],
            "timestamp": time.time()
        }
        message = json.dumps(discovery_msg).encode('utf-8')
        
        interval = 10  # секунд
        while self.running:
            try:
                # Широковещательная рассылка
                self.udp_send_socket.sendto(message, ('255.255.255.255', self.config['discovery_port']))
                self.udp_send_socket.sendto(message, ('127.0.0.1', self.config['discovery_port']))
                
            except Exception as e:
                self.logger.error(f"Ошибка broadcast: {e}")
            
            # Ожидание с проверкой running
            for _ in range(interval * 10):
                if not self.running:
                    break
                time.sleep(0.1)
    
    def listen_for_peers(self):
        """Прослушивание информации от других узлов"""
        self.logger.info("Начало прослушивания discovery сообщений")
        
        while self.running:
            try:
                data, addr = self.udp_recv_socket.recvfrom(4096)
                
                if not data:
                    continue
                    
                try:
                    message = json.loads(data.decode('utf-8'))
                    
                    if message.get('type') == 'discovery':
                        peer_host = message.get('host', addr[0])
                        peer_port = message.get('port')
                        
                        if not peer_port:
                            continue
                            
                        try:
                            peer_port = int(peer_port)
                        except (ValueError, TypeError):
                            continue
                        
                        # Пропускаем свой собственный узел
                        if (peer_host == self.config['host'] and 
                            peer_port == self.config['port']):
                            continue
                        
                        peer_addr = (peer_host, peer_port)
                        
                        with self.peers_lock:
                            was_known = peer_addr in self.peers
                            self.peers[peer_addr] = time.time()
                        
                        if not was_known:
                            print(f"\nОбнаружен узел: {peer_host}:{peer_port}")
                            self.logger.info(f"Обнаружен новый узел: {peer_addr}")
                            
                except json.JSONDecodeError:
                    continue
                    
            except socket.timeout:
                continue
            except OSError as e:
                if not self.running:
                    break
                self.logger.error(f"Ошибка UDP сокета: {e}")
            except Exception as e:
                self.logger.error(f"Ошибка listen_for_peers: {e}")
    
    def send_message_to_peers(self):
        """Отправка сообщения всем доступным узлам"""
        print("ОТПРАВКА СООБЩЕНИЯ")
        
        # Получаем активных пиров
        with self.peers_lock:
            current_time = time.time()
            active_peers = [
                addr for addr, last_seen in self.peers.items()
                if current_time - last_seen < 30
            ]
        
        if not active_peers:
            print("Нет активных узлов для отправки")
            print("Ожидайте обнаружения других узлов")
            return
        
        print(f"Найдено активных узлов: {len(active_peers)}")
        print("Введите сообщение (или 'отмена' для возврата):")
        
        text = input("> ").strip()
        
        if text.lower() == 'отмена':
            print("Отправка отменена")
            return
        
        if not text:
            print("Сообщение не может быть пустым")
            return
        
        # Создаем структурированное сообщение
        message_data = {
            "type": "message",
            "text": text,
            "sender": f"{self.config['host']}:{self.config['port']}",
            "timestamp": datetime.now().isoformat(),
            "message_id": int(time.time() * 1000)
        }
        
        try:
            json_message = json.dumps(message_data, ensure_ascii=False).encode('utf-8')
        except Exception as e:
            self.logger.error(f"Ошибка кодирования JSON: {e}")
            print("Ошибка подготовки сообщения")
            return
        
        # Отображаем у себя
        display_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[{display_time}] Вы: {text}")
        self.messages.append(f"[{display_time}] Вы: {text}")
        
        # Отправка всем активным пирам
        sent_count = 0
        failed_peers = []
        
        for peer in active_peers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3.0)
                sock.connect(peer)
                sock.send(json_message)
                sock.close()
                sent_count += 1
                
                # Обновляем время активности
                with self.peers_lock:
                    self.peers[peer] = time.time()
                    
                self.logger.info(f"Сообщение отправлено к {peer}")
                
            except ConnectionRefusedError:
                failed_peers.append(peer)
                self.logger.warning(f"Соединение отклонено: {peer}")
            except socket.timeout:
                failed_peers.append(peer)
                self.logger.warning(f"Таймаут соединения: {peer}")
            except Exception as e:
                failed_peers.append(peer)
                self.logger.error(f"Ошибка отправки к {peer}: {e}")
        
        # Удаляем недоступные узлы
        if failed_peers:
            with self.peers_lock:
                for peer in failed_peers:
                    if peer in self.peers:
                        del self.peers[peer]
            
            if len(failed_peers) == 1:
                print(f"Узел {failed_peers[0]} недоступен")
            else:
                print(f"{len(failed_peers)} узлов недоступны")
        
        # Результат отправки
        if sent_count == 0:
            print("Сообщение не доставлено ни одному узлу")
        else:
            print(f"Сообщение доставлено {sent_count} узлам")
        
        self.logger.info(f"Отправка завершена: {sent_count} успешно, {len(failed_peers)} неудачно")
    
    def display_network_nodes(self):
        """Отображение списка активных сетевых узлов"""
        print("\n" + "="*50)
        print("АКТИВНЫЕ СЕТЕВЫЕ УЗЛЫ")
        
        with self.peers_lock:
            current_time = time.time()
            active_nodes = []
            
            for addr, last_seen in self.peers.items():
                age = current_time - last_seen
                if age < 60:  # Активны в последнюю минуту
                    active_nodes.append((addr, age))
            
            if not active_nodes:
                print("  Активные узлы отсутствуют")
            else:
                print(f"  Всего узлов: {len(active_nodes)}")
                for addr, age in active_nodes:
                    status = "Активен" if age < 10 else "Недавно" if age < 30 else "Давно"
                    print(f"  {addr[0]}:{addr[1]} - {status} ({age:.1f} сек назад)")
        
    
    def display_message_log(self):
        """Отображение журнала сообщений"""
        print("\n" + "="*50)
        print("ЖУРНАЛ СООБЩЕНИЙ")
        
        if not self.messages:
            print("  Журнал пуст")
        else:
            print(f"  Всего сообщений: {len(self.messages)}")
            print("  Последние 20 сообщений:")
            print("-" * 50)
            for msg in self.messages[-20:]:
                print(f"  {msg}")
        

    
    def display_system_status(self):
        """Отображение текущего состояния системы"""
        
        print(f"  Текущий узел: {self.config['host']}:{self.config['port']}")
        print(f"  Порт обнаружения: {self.config['discovery_port']}")
        print(f"  Статус: {'работает' if self.running else 'остановлен'}")
        
        with self.peers_lock:
            current_time = time.time()
            active_count = len([p for p in self.peers.values() if current_time - p < 60])
            total_peers = len(self.peers)
        
        print(f"  Активных узлов: {active_count}")
        print(f"  Всего известных узлов: {total_peers}")
        print(f"  Сообщений в журнале: {len(self.messages)}")
        
        # Информация о потоках
        print(f"  Потоков активно: {threading.active_count()}")
        
    
    def cleanup_inactive_peers(self):
        """Очистка неактивных узлов"""
        current_time = time.time()
        with self.peers_lock:
            to_remove = []
            for addr, last_seen in self.peers.items():
                if current_time - last_seen > 300:  # 5 минут неактивности
                    to_remove.append(addr)
            
            removed_count = len(to_remove)
            for addr in to_remove:
                del self.peers[addr]
            
            if removed_count > 0:
                self.logger.info(f"Удалено неактивных узлов: {removed_count}")
    
    def show_control_panel(self):
        """Отображение панели управления"""
        os.system('cls' if os.name == 'nt' else 'clear')
        print("СЕТЕВОЙ МЕССЕНДЖЕР - ПАНЕЛЬ УПРАВЛЕНИЯ")
        print(f"Узел: {self.config['host']}:{self.config['port']}")
        
        with self.peers_lock:
            current_time = time.time()
            active_count = len([p for p in self.peers.values() if current_time - p < 60])
        
        print(f"Активных узлов в сети: {active_count}")
        print("\nВЫБЕРИТЕ ДЕЙСТВИЕ:")
        print("1. Отправить сообщение")
        print("2. Показать сетевые узлы")
        print("3. Показать журнал сообщений")
        print("4. Показать статус системы")
        print("5. Обновить сетевую информацию")
        print("6. Очистить экран")
        print("7. Завершить работу")
    
    def execute(self):
        """Основной цикл выполнения программы"""
        self.logger.info("Запуск основного цикла программы")
        
        # Инициализация сервисов
        try:
            tcp_ok = self.start_tcp_server()
            udp_ok = self.start_discovery_service()
            
            if not tcp_ok or not udp_ok:
                self.logger.error("Ошибка инициализации сетевых сервисов")
                print("Ошибка запуска сетевых сервисов. Проверьте логи.")
                input("Нажмите Enter для выхода...")
                return
                
        except Exception as e:
            self.logger.error(f"Критическая ошибка инициализации: {e}", exc_info=True)
            print(f"Критическая ошибка: {e}")
            input("Нажмите Enter для выхода...")
            return
        
        print("\n" + "="*60)
        print(f"Ваш адрес: {self.config['host']}:{self.config['port']}")
        print("Ожидайте обнаружения других узлов (10-30 секунд)...")
        time.sleep(2)
        
        # Таймер для обслуживания
        cleanup_timer = time.time()
        status_timer = time.time()
        
        # Главный цикл
        while self.running:
            try:
                # Фоновая очистка (каждые 30 секунд)
                current_time = time.time()
                if current_time - cleanup_timer > 30:
                    self.cleanup_inactive_peers()
                    cleanup_timer = current_time
                
                # Периодический статус (каждые 60 секунд)
                if current_time - status_timer > 60:
                    self.logger.info("Периодический статус - система работает")
                    status_timer = current_time
                
                # Отображение меню и обработка ввода
                self.show_control_panel()
                choice = input("\nВыберите действие (1-7): ").strip()
                
                if choice == '1':
                    self.send_message_to_peers()
                    input("\nНажмите Enter для продолжения...")
                elif choice == '2':
                    self.display_network_nodes()
                    input("\nНажмите Enter для продолжения...")
                elif choice == '3':
                    self.display_message_log()
                    input("\nНажмите Enter для продолжения...")
                elif choice == '4':
                    self.display_system_status()
                    input("\nНажмите Enter для продолжения...")
                elif choice == '5':
                    print("\nОбновление сетевой информации...")
                    # Принудительная отправка discovery
                    try:
                        discovery_msg = {
                            "type": "discovery",
                            "host": self.config['host'],
                            "port": self.config['port'],
                            "timestamp": time.time()
                        }
                        message = json.dumps(discovery_msg).encode('utf-8')
                        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        temp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                        temp_sock.sendto(message, ('255.255.255.255', self.config['discovery_port']))
                        temp_sock.close()
                        print("Запрос на обновление отправлен")
                    except Exception as e:
                        self.logger.error(f"Ошибка принудительного discovery: {e}")
                    input("\nНажмите Enter для продолжения...")
                elif choice == '6':
                    continue  # Экран очистится в следующей итерации
                elif choice == '7':
                    print("\nИнициировано завершение работы...")
                    self.running = False
                else:
                    print("Неверный выбор. Введите число от 1 до 7.")
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print("\n\nПолучен сигнал прерывания")
                self.running = False
            except EOFError:
                print("\n\nКонец ввода")
                self.running = False
            except Exception as e:
                self.logger.error(f"Ошибка в основном цикле: {e}", exc_info=True)
                print(f"Ошибка: {e}")
                time.sleep(2)
        
        # Корректное завершение
        self.terminate()
    
    def terminate(self):
        """Корректное завершение работы системы"""
        self.logger.info("Начало процедуры завершения работы")
        self.running = False
        
        # Даем время потокам завершиться
        time.sleep(0.5)
        
        # Закрытие сокетов
        sockets_to_close = ['tcp_socket', 'udp_send_socket', 'udp_recv_socket']
        
        for socket_name in sockets_to_close:
            if hasattr(self, socket_name):
                try:
                    getattr(self, socket_name).close()
                    self.logger.info(f"Сокет {socket_name} закрыт")
                except Exception as e:
                    self.logger.error(f"Ошибка закрытия сокета {socket_name}: {e}")
        
        # Завершение логирования
        logging.shutdown()
        
        print("ПРОГРАММА ЗАВЕРШЕНА")
        print("Все системные ресурсы освобождены")
        print("Логи сохранены в файле: network_messenger.log")
        time.sleep(1)

if __name__ == "__main__":
    try:
        config_file = "config.json"
        if len(sys.argv) > 1:
            config_file = sys.argv[1]
            print(f"Используется конфигурационный файл: {config_file}")
        
        app = NetworkMessenger(config_file)
        app.execute()
        
    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        input("Нажмите Enter для выхода...")