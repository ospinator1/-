
import tkinter as tk
from tkinter import ttk, messagebox
import threading
from queue import Queue
import schedule
import time
from datetime import datetime

from models.database import DatabaseManager
from services.database_service import DatabaseService
from services.file_parser import FileParser
from services.export_service import ExportService

from .data_management_tab import DataManagementTab
from .filter_tab import FilterTab
from .export_tab import ExportTab

class NetworkMonitorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Загрузчик данных сетевого монитора")
        self.root.geometry("1920x1080")
        
        # Инициализация сервисов
        self.db_manager = DatabaseManager()
        self.db_service = DatabaseService(self.db_manager)
        self.file_parser = FileParser()
        self.export_service = ExportService()
        
        # Состояние приложения
        self.packets = []
        self.current_thread = None
        self.progress_queue = Queue()
        self.last_analysis_result = None
        self.current_sql_query = None
        self.current_query_params = None
        
        self.setup_ui()
        self.setup_progress_handler()
        self.setup_auto_backup_scheduler() 
        
        # Автоматически создаем начальную статистику при запуске
        self.create_initial_stats()
    
    def setup_ui(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Создание вкладок
        self.data_management_tab = DataManagementTab(self.notebook, self)
        self.filter_tab = FilterTab(self.notebook, self)
        self.export_tab = ExportTab(self.notebook, self)
        
        self.notebook.add(self.data_management_tab.frame, text="Управление данными")
        self.notebook.add(self.filter_tab.frame, text="Фильтрация данных")
        self.notebook.add(self.export_tab.frame, text="Экспорт данных")
    
    def setup_progress_handler(self):
        def check_queue():
            try:
                while True:
                    message_type, data = self.progress_queue.get_nowait()
                    if message_type == 'progress':
                        progress, text = data
                        self.data_management_tab.update_progress(progress, text)
                    elif message_type == 'complete':
                        success, message = data
                        self.on_task_complete(success, message)
                    elif message_type == 'hide_progress':
                        self.data_management_tab.hide_progress()
            except:
                pass
            self.root.after(100, check_queue)
        
        self.root.after(100, check_queue)
    
    def setup_auto_backup_scheduler(self):
        """Настройка автоматического резервного копирования"""
        def backup_job():
            """Задача автоматического резервного копирования"""
            print(f"Запуск автоматического резервного копирования: {datetime.now()}")
            success, message = self.db_service.auto_create_backup()
            if success:
                print(f"Автоматическая резервная копия создана: {message}")
            else:
                print(f"Ошибка автоматического резервного копирования: {message}")
        
        def schedule_checker():
            """Проверка расписания в отдельном потоке"""
            while True:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
        
        schedule.every().day.at("02:00").do(backup_job)

        schedule.every().sunday.at("03:00").do(backup_job)

        schedule_thread = threading.Thread(target=schedule_checker, daemon=True)
        schedule_thread.start()
        
        print("Автоматическое резервное копирование настроено")
    
    def update_progress(self, progress, text):
        self.progress_queue.put(('progress', (progress, text)))
    
    def on_task_complete(self, success, message):
        self.data_management_tab.hide_progress()
        self.data_management_tab.update_status(message)
        
        if success:
            messagebox.showinfo("Успех", message)
        else:
            messagebox.showerror("Ошибка", message)
    
    def run_in_thread(self, target, *args):
        """Запуск функции в отдельном потоке"""
        if self.current_thread and self.current_thread.is_alive():
            messagebox.showwarning("Предупреждение", "Пожалуйста, дождитесь завершения текущей операции")
            return False
        
        def thread_wrapper():
            try:
                target(*args)
            except Exception as e:
                self.progress_queue.put(('complete', (False, f"Ошибка потока: {e}")))
        
        self.current_thread = threading.Thread(target=thread_wrapper)
        self.current_thread.daemon = True
        self.current_thread.start()
        return True
    
    # Методы для доступа к прогрессу из других компонентов
    def show_progress(self):
        """Показать прогресс-бар"""
        self.data_management_tab.show_progress()
    
    def update_progress_text(self, text):
        """Обновить текст прогресса"""
        self.data_management_tab.update_progress_text(text)

    def create_initial_stats(self):
        """Создание начальной статистики при запуске приложения"""
        def task():
            # Проверяем есть ли данные в базе
            success, count = self.db_service.get_total_records_count()
            if success and count > 0:
                # Создаем начальную статистику
                self.db_service.auto_save_stats()
                
                # Создаем начальную резервную копию
                self.db_service.auto_create_backup()
        
        # Запускаем в фоновом режиме
        thread = threading.Thread(target=task)
        thread.daemon = True
        thread.start()