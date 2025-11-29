import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from datetime import datetime

class DataManagementTab:
    def __init__(self, parent, app):
        self.app = app
        self.frame = ttk.Frame(parent)
        self.setup_ui()
    
    def setup_ui(self):
        main_frame = ttk.Frame(self.frame, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(16, weight=1)
        
        # Заголовок
        title_label = ttk.Label(main_frame, text="Загрузчик данных сетевого монитора", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=10)
        
        # Секция подключения к БД
        self.setup_database_section(main_frame, 1)
        
        # Секция информации о БД
        self.setup_info_section(main_frame, 3)
        
        # Секция управления данными
        self.setup_management_section(main_frame, 5)
        
        # Секция резервного копирования
        self.setup_backup_section(main_frame, 7)
        
        # Секция загрузки файлов
        self.setup_file_section(main_frame, 9)
        
        # Прогресс-бар
        self.setup_progress_section(main_frame, 11)
        
        # Секция обработки данных
        self.setup_processing_section(main_frame, 12)
        
        # Секция просмотра данных
        self.setup_viewing_section(main_frame, 14)
        
        # Статус и таблица данных
        self.setup_results_section(main_frame, 16)
    
    def setup_database_section(self, parent, row):
        ttk.Label(parent, text="Подключение к PostgreSQL", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        db_frame = ttk.Frame(parent)
        db_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(db_frame, text="Подключиться к БД", 
                  command=self.connect_db).pack(side=tk.LEFT, padx=5)
        ttk.Button(db_frame, text="Создать таблицы", 
                  command=self.create_tables).pack(side=tk.LEFT, padx=5)
        ttk.Button(db_frame, text="Пересоздать таблицы", 
                  command=self.recreate_tables).pack(side=tk.LEFT, padx=5)
    
    def setup_info_section(self, parent, row):
        ttk.Label(parent, text="Информация о базе данных", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        info_frame = ttk.Frame(parent)
        info_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(info_frame, text="Информация о размере БД", 
                  command=self.show_database_info).pack(side=tk.LEFT, padx=5)
        ttk.Button(info_frame, text="Размеры таблиц", 
                  command=self.show_table_sizes).pack(side=tk.LEFT, padx=5)
        ttk.Button(info_frame, text="Список таблиц", 
                  command=self.show_table_list).pack(side=tk.LEFT, padx=5)
    
    def setup_management_section(self, parent, row):
        ttk.Label(parent, text="Управление данными", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        management_frame = ttk.Frame(parent)
        management_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(management_frame, text="Очистить таблицы (данные)", 
                  command=self.clear_tables).pack(side=tk.LEFT, padx=5)
        ttk.Button(management_frame, text="Полная очистка БД", 
                  command=self.clear_database).pack(side=tk.LEFT, padx=5)
        ttk.Button(management_frame, text="Удалить все таблицы", 
                  command=self.drop_tables).pack(side=tk.LEFT, padx=5)
    
    def setup_backup_section(self, parent, row):
        """Секция резервного копирования базы данных"""
        ttk.Label(parent, text="Резервное копирование базы данных", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        backup_frame = ttk.Frame(parent)
        backup_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(backup_frame, text="Создать SQL бэкап", 
                  command=self.create_sql_backup).pack(side=tk.LEFT, padx=2)
        ttk.Button(backup_frame, text="Автоматический бэкап", 
                  command=self.auto_backup).pack(side=tk.LEFT, padx=2)
        ttk.Button(backup_frame, text="История бэкапов", 
                  command=self.show_backup_history).pack(side=tk.LEFT, padx=2)
        ttk.Button(backup_frame, text="Восстановить из бэкапа", 
                  command=self.restore_from_backup).pack(side=tk.LEFT, padx=2)
        ttk.Button(backup_frame, text="Открыть папку бэкапов", 
                  command=self.open_backup_folder).pack(side=tk.LEFT, padx=2)
    
    def setup_file_section(self, parent, row):
        ttk.Label(parent, text="Загрузка файлов", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        file_frame = ttk.Frame(parent)
        file_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(file_frame, text="Выбрать файл", 
                  command=self.load_file).pack(side=tk.LEFT, padx=5)
        self.file_label = ttk.Label(file_frame, text="Файл не выбран")
        self.file_label.pack(side=tk.LEFT, padx=5)
    
    def setup_progress_section(self, parent, row):
        self.progress_frame = ttk.Frame(parent)
        self.progress_frame.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        self.progress_bar = ttk.Progressbar(self.progress_frame, mode='determinate')
        self.progress_bar.pack(fill=tk.X, expand=True)
        
        self.progress_label = ttk.Label(self.progress_frame, text="Готов к работе")
        self.progress_label.pack()
        
        self.progress_frame.grid_remove()
    
    def setup_processing_section(self, parent, row):
        ttk.Label(parent, text="Обработка данных", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        process_frame = ttk.Frame(parent)
        process_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(process_frame, text="Разобрать файл (поток)", 
                  command=self.parse_file_threaded).pack(side=tk.LEFT, padx=5)
        ttk.Button(process_frame, text="Загрузить в БД (поток)", 
                  command=self.load_to_db_threaded).pack(side=tk.LEFT, padx=5)
        ttk.Button(process_frame, text="Обновить статистику", 
                  command=self.update_stats).pack(side=tk.LEFT, padx=5)
    
    def setup_viewing_section(self, parent, row):
        ttk.Label(parent, text="Просмотр данных", 
                 font=("Arial", 12, "bold")).grid(row=row, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        view_frame = ttk.Frame(parent)
        view_frame.grid(row=row+1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(view_frame, text="Данные пакетов", 
                  command=lambda: self.show_table('packet_data')).pack(side=tk.LEFT, padx=5)
        ttk.Button(view_frame, text="Статистика протоколов", 
                  command=lambda: self.show_table('protocol_stats')).pack(side=tk.LEFT, padx=5)
        ttk.Button(view_frame, text="IP статистика", 
                  command=lambda: self.show_table('ip_stats')).pack(side=tk.LEFT, padx=5)
    
    def setup_results_section(self, parent, row):
        self.status_label = ttk.Label(parent, text="Готов к работе", font=("Arial", 10))
        self.status_label.grid(row=row, column=0, columnspan=3, pady=10)
        
        self.tree = ttk.Treeview(parent, show='headings')
        self.tree.grid(row=row+1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        v_scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.tree.yview)
        v_scrollbar.grid(row=row+1, column=2, sticky=(tk.N, tk.S), pady=5)
        self.tree.configure(yscrollcommand=v_scrollbar.set)
        
        h_scrollbar = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=self.tree.xview)
        h_scrollbar.grid(row=row+2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        self.tree.configure(xscrollcommand=h_scrollbar.set)
        
        parent.rowconfigure(row+1, weight=1)
    
    def show_progress(self):
        """Показать прогресс-бар"""
        self.progress_frame.grid()
        self.progress_bar['value'] = 0
    
    def hide_progress(self):
        """Скрыть прогресс-бар"""
        self.progress_frame.grid_remove()
        self.progress_bar['value'] = 0
    
    def update_progress(self, progress, text):
        """Обновить прогресс и текст"""
        self.progress_bar['value'] = progress
        self.progress_label.config(text=text)
    
    def update_progress_text(self, text):
        """Обновить только текст прогресса"""
        self.progress_label.config(text=text)
    
    def update_status(self, text):
        """Обновить статус"""
        self.status_label.config(text=text)
    
    # Методы работы с базой данных
    def connect_db(self):
        def task():
            success, message = self.app.db_service.db_manager.connect()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Подключение к базе данных...")
    
    def create_tables(self):
        def task():
            success, message = self.app.db_service.create_tables()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Создание таблиц...")
    
    def recreate_tables(self):
        if messagebox.askyesno("Подтверждение", 
                              "Это удалит все существующие данные и пересоздаст таблицы. Продолжить?"):
            def task():
                success, message = self.app.db_service.drop_and_recreate_tables()
                self.app.progress_queue.put(('complete', (success, message)))
            
            self.app.run_in_thread(task)
            self.update_status("Пересоздание таблиц...")
    
    def show_database_info(self):
        def task():
            success, result = self.app.db_service.get_database_size()
            if success:
                size_info = result
                message = (f"Информация о базе данных:\n\n"
                          f"Размер БД: {size_info['db_size']}\n"
                          f"Пакетов в packet_data: {size_info['packet_count']}\n"
                          f"Записей в protocol_stats: {size_info['protocol_stats_count']}\n"
                          f"Записей в ip_stats: {size_info['ip_stats_count']}")
                self.app.progress_queue.put(('complete', (True, message)))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
        self.update_status("Получение информации о БД...")
    
    def show_table_sizes(self):
        def task():
            success, result = self.app.db_service.get_table_sizes()
            if success:
                tables_info = result
                if tables_info:
                    message = "Размеры таблиц:\n\n"
                    for table in tables_info:
                        message += f"• {table[0]}: {table[1]} (таблица: {table[2]})\n"
                    self.app.progress_queue.put(('complete', (True, message)))
                else:
                    self.app.progress_queue.put(('complete', (True, "В базе данных нет таблиц")))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
        self.update_status("Получение размеров таблиц...")
    
    def show_table_list(self):
        def task():
            success, result = self.app.db_service.get_table_list()
            if success:
                tables = result
                if tables:
                    table_list = "\n".join([f"• {table}" for table in tables])
                    message = f"Таблицы в базе данных:\n\n{table_list}\n\nВсего: {len(tables)} таблиц"
                    self.app.progress_queue.put(('complete', (True, message)))
                else:
                    self.app.progress_queue.put(('complete', (True, "В базе данных нет таблиц")))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
        self.update_status("Получение списка таблиц...")
    
    def clear_database(self):
        if not messagebox.askyesno("Подтверждение полной очистки", 
                                  "ВНИМАНИЕ: Это удалит ВСЕ данные из ВСЕХ таблиц!\n\n"
                                  "Таблицы останутся, но все данные будут удалены безвозвратно.\n\n"
                                  "Продолжить?"):
            return
        
        def task():
            success, message = self.app.db_service.clear_database()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Полная очистка базы данных...")
    
    def clear_tables(self):
        if not messagebox.askyesno("Подтверждение очистки", 
                                  "Это удалит все данные из таблиц, но сохранит их структуру.\n\n"
                                  "Продолжить?"):
            return
        
        def task():
            success, message = self.app.db_service.clear_tables()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Очистка таблиц...")
    
    def drop_tables(self):
        if not messagebox.askyesno("Подтверждение удаления", 
                                  "ВНИМАНИЕ: Это удалит ВСЕ таблицы и данные без возможности восстановления!\n\n"
                                  "Продолжить?"):
            return
        
        def task():
            success, message = self.app.db_service.drop_tables()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Удаление таблиц...")

    def create_sql_backup(self):
        """Создание SQL резервной копии"""
        def task():
            self.show_progress()
            self.update_progress_text("Создание SQL резервной копии...")
            
            success, message = self.app.db_service.create_sql_backup_manual()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Создание SQL резервной копии...")
    
    def auto_backup(self):
        """Автоматическое создание резервной копии"""
        def task():
            self.show_progress()
            self.update_progress_text("Автоматическое создание резервной копии...")
            
            success, message = self.app.db_service.auto_create_backup()
            self.app.progress_queue.put(('complete', (success, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Автоматическое создание резервной копии...")
    
    def show_backup_history(self):
        """Показать историю резервных копий"""
        try:
            backup_history = self.app.db_service.get_backup_history()
            
            if not backup_history:
                messagebox.showinfo("История резервных копий", "Резервные копии не найдены")
                return
            
            history_dialog = tk.Toplevel(self.frame)
            history_dialog.title("История резервных копий")
            history_dialog.geometry("800x400")
            history_dialog.transient(self.frame)
            history_dialog.grab_set()
            
            tree = ttk.Treeview(history_dialog, columns=('filename', 'size', 'date'), show='headings')
            tree.heading('filename', text='Имя файла')
            tree.heading('size', text='Размер')
            tree.heading('date', text='Дата создания')
            
            tree.column('filename', width=400)
            tree.column('size', width=150)
            tree.column('date', width=200)
            
            for backup in backup_history:
                size_mb = backup['size'] / (1024 * 1024)
                tree.insert('', tk.END, values=(
                    backup['filename'],
                    f"{size_mb:.2f} MB",
                    backup['created_time'].strftime('%Y-%m-%d %H:%M:%S')
                ))
            
            tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            
            button_frame = ttk.Frame(history_dialog)
            button_frame.pack(pady=10)
            
            ttk.Button(button_frame, text="Закрыть", 
                      command=history_dialog.destroy).pack(side=tk.LEFT, padx=5)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить историю бэкапов: {e}")
    
    def restore_from_backup(self):
        """Восстановление из резервной копии"""
        if not messagebox.askyesno("Подтверждение", 
                                  "ВНИМАНИЕ: Это перезапишет все текущие данные!\n\n"
                                  "Убедитесь, что у вас есть свежая резервная копия.\n\n"
                                  "Продолжить?"):
            return
        
        backup_path = filedialog.askopenfilename(
            title="Выберите файл резервной копии (.sql)",
            filetypes=[("SQL files", "*.sql"), ("All files", "*.*")],
            initialdir=self.app.db_service.backup_dir
        )
        
        if backup_path:
            def task():
                self.show_progress()
                self.update_progress_text("Восстановление из резервной копии...")
                
                success, message = self.app.db_service.restore_from_sql_backup(backup_path)
                self.app.progress_queue.put(('complete', (success, message)))
            
            self.app.run_in_thread(task)
            self.update_status("Восстановление из резервной копии...")
    
    def open_backup_folder(self):
        """Открыть папку с резервными копиями"""
        try:
            backup_dir = self.app.db_service.backup_dir
            if os.path.exists(backup_dir):
                os.startfile(backup_dir) 
            else:
                messagebox.showinfo("Информация", "Папка с резервными копиями не существует")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось открыть папку: {e}")
    
    def _get_timestamp(self):
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def load_file(self):
        file_path = filedialog.askopenfilename(
            title="Выберите файл лога",
            filetypes=[("Текстовые файлы", "*.txt"), ("Все файлы", "*.*")]
        )
        if file_path:
            self.file_label.config(text=file_path)
            self.update_status(f"Выбран файл: {file_path}")
    
    def parse_file_threaded(self):
        if not self.file_label.cget('text') or self.file_label.cget('text') == "Файл не выбран":
            messagebox.showerror("Ошибка", "Сначала выберите файл!")
            return
        
        def task():
            self.show_progress()
            self.update_progress_text("Начало разбора файла...")
            
            file_path = self.file_label.cget('text')
            packets = self.app.file_parser.parse_log_file(file_path, progress_callback=self.app.update_progress)
            
            if packets:
                self.app.packets = packets
                protocols = set(p['protocol'] for p in packets if p['protocol'])
                message = f"Разбор завершен. Найдено {len(packets)} пакетов. Протоколы: {', '.join(protocols)}"
                self.app.progress_queue.put(('complete', (True, message)))
            else:
                self.app.progress_queue.put(('complete', (False, "Не удалось разобрать файл")))
        
        self.app.run_in_thread(task)
        self.update_status("Разбор файла...")
    
    def load_to_db_threaded(self):
        if not self.app.packets:
            messagebox.showerror("Ошибка", "Сначала разберите файл!")
            return
        
        def task():
            self.show_progress()
            self.update_progress_text("Начало загрузки в БД...")
            
            success, message = self.app.db_service.insert_packet_data(
                self.app.packets, 
                progress_callback=self.app.update_progress
            )
            
            if success:
                # Автоматически создаем резервную копию после загрузки данных
                self.app.db_service.auto_create_backup()
                
                final_message = f"{message}\n\nАвтоматическая статистика включена!\n"
                final_message += "Статистика автоматически сохраняется в файлы в папке C:\\Users\\Assa\\source\\repos\\network_stats"
                self.app.progress_queue.put(('complete', (True, final_message)))
            else:
                self.app.progress_queue.put(('complete', (False, message)))
        
        self.app.run_in_thread(task)
        self.update_status("Загрузка данных в БД...")
    
    def update_stats(self):
        def task():
            self.show_progress()
            self.update_progress_text("Обновление статистики...")
            
            success_proto, message_proto = self.app.db_service.update_protocol_stats()
            success_ip, message_ip = self.app.db_service.update_ip_stats()
            
            if success_proto and success_ip:
                message = f"Статистика обновлена:\n{message_proto}\n{message_ip}"
                self.app.progress_queue.put(('complete', (True, message)))
            else:
                self.app.progress_queue.put(('complete', (False, f"Ошибка обновления статистики: {message_proto} {message_ip}")))
        
        self.app.run_in_thread(task)
        self.update_status("Обновление статистики...")
    
    def show_table(self, table_name):
        columns, data = self.app.db_service.get_table_data(table_name)
        
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = columns
        
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, minwidth=50, stretch=True)
        
        for row in data:
            self.tree.insert("", tk.END, values=row)
        
        self.update_status(f"Отображена таблица: {table_name} (записей: {len(data)})")