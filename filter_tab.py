# gui/filter_tab.py
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import re

class FilterTab:
    def __init__(self, parent, app):
        self.app = app
        self.frame = ttk.Frame(parent)
        self.total_records = 0
        self.current_limit = None
        self.current_offset = 0
        self.available_ips = []
        self.setup_ui()
    
    def setup_ui(self):
        # Создаем основной контейнер без лишних скроллбаров
        main_container = ttk.Frame(self.frame)
        main_container.pack(fill=tk.BOTH, expand=True)
        
        container_frame = ttk.Frame(main_container)
        container_frame.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(container_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.canvas = tk.Canvas(container_frame, yscrollcommand=scrollbar.set, bg='white', highlightthickness=0)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.canvas.yview)
        
        self.content_frame = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.content_frame, anchor="nw")
        
        # Привязываем события для обновления скроллрегиона и ширины
        self.content_frame.bind("<Configure>", self._on_frame_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)
        
        # Привязываем колесо мыши к прокрутке
        self.canvas.bind("<MouseWheel>", self._on_mousewheel)
        self.content_frame.bind("<MouseWheel>", self._on_mousewheel)
        
        # Заголовок и описание
        title_label = ttk.Label(self.content_frame, text="Система фильтрации пакетов", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=4, pady=10, sticky=tk.W)
        
        desc_label = ttk.Label(self.content_frame, 
                              text="Создавайте фильтры для поиска конкретных пакетов. Система автоматически сгенерирует SQL-запросы.",
                              font=("Arial", 10), wraplength=1000)
        desc_label.grid(row=1, column=0, columnspan=4, pady=5, sticky=tk.W)
        
        # Секция ограничения данных
        self.setup_limitation_section(self.content_frame, 2)
        
        # Секция фильтров протоколов
        self.setup_protocol_section(self.content_frame, 5)
        
        # Секция IP-фильтров
        self.setup_ip_section(self.content_frame, 7)
        
        # Секция активных фильтров
        self.setup_filters_section(self.content_frame, 9)
        
        # Секция нового фильтра
        self.setup_new_filter_section(self.content_frame, 12)
        
        # Секция примеров
        self.setup_examples_section(self.content_frame, 14)
        
        # Секция выполнения запросов
        self.setup_execution_section(self.content_frame, 16)
        
        # Отображение SQL-запроса
        self.setup_sql_section(self.content_frame, 18)
        
        # Секция результатов
        self.setup_results_section(self.content_frame, 20)
        
        # Статус
        self.filter_status = ttk.Label(self.content_frame, text="Готов к фильтрации", font=("Arial", 10))
        self.filter_status.grid(row=23, column=0, columnspan=4, pady=10, sticky=tk.W)
        
        # Настраиваем веса для правильного расширения
        self.content_frame.columnconfigure(0, weight=1)
        self.content_frame.columnconfigure(1, weight=1)
        self.content_frame.columnconfigure(2, weight=1)
        self.content_frame.columnconfigure(3, weight=1)
        self.content_frame.rowconfigure(21, weight=1)
    
    def _on_frame_configure(self, event=None):
        """Обновление скроллрегиона при изменении размера фрейма"""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def _on_canvas_configure(self, event=None):
        """Обновление ширины внутреннего окна при изменении размера canvas"""
        self.canvas.itemconfig(self.canvas_window, width=event.width)
    
    def _on_mousewheel(self, event):
        """Обработчик прокрутки колесом мыши"""
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    
    def setup_limitation_section(self, parent, row):
        ttk.Label(parent, text="Ограничение данных:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=5, sticky=tk.W)
        
        limit_frame = ttk.Frame(parent)
        limit_frame.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Label(limit_frame, text="Показать записей:").grid(row=0, column=0, padx=5, pady=5)
        
        self.limit_var = tk.StringVar(value="Все")
        self.limit_combobox = ttk.Combobox(limit_frame, textvariable=self.limit_var, 
                                          values=["100", "500", "1000", "5000", "10000", "Все"], 
                                          state="readonly", width=10)
        self.limit_combobox.grid(row=0, column=1, padx=5, pady=5)
        self.limit_combobox.set("Все")
        
        ttk.Label(limit_frame, text="Начать с записи:").grid(row=0, column=2, padx=5, pady=5)
        
        self.offset_var = tk.StringVar(value="0")
        self.offset_entry = ttk.Entry(limit_frame, textvariable=self.offset_var, width=10)
        self.offset_entry.grid(row=0, column=3, padx=5, pady=5)
        
        ttk.Button(limit_frame, text="Обновить статистику", 
                  command=self.update_records_info).grid(row=0, column=4, padx=5, pady=5)
        
        self.records_info_label = ttk.Label(parent, text="Всего записей: неизвестно", 
                                           font=("Arial", 9))
        self.records_info_label.grid(row=row+2, column=0, columnspan=4, pady=2, sticky=tk.W)
    
    def setup_protocol_section(self, parent, row):
        ttk.Label(parent, text="Быстрые фильтры протоколов:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=5, sticky=tk.W)
        
        protocol_frame = ttk.Frame(parent)
        protocol_frame.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        self.protocol_vars = {}
        common_protocols = ['TCP', 'UDP', 'SSL', 'TLSv1.2', 'HTTP', 'DNS', 'NBNS', 'DHCP', 'MDNS', 'ARP', 'NTP']
        
        for i, protocol in enumerate(common_protocols):
            var = tk.BooleanVar()
            self.protocol_vars[protocol] = var
            cb = ttk.Checkbutton(protocol_frame, text=protocol, variable=var)
            cb.grid(row=i//6, column=i%6, sticky=tk.W, padx=5, pady=2)
        
        ttk.Button(protocol_frame, text="Добавить выбранные протоколы", 
                  command=self.add_selected_protocols).grid(row=2, column=0, columnspan=6, pady=5)
    
    def setup_ip_section(self, parent, row):
        ttk.Label(parent, text="Быстрые IP-фильтры:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=5, sticky=tk.W)
        
        ip_frame = ttk.Frame(parent)
        ip_frame.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Label(ip_frame, text="Выберите IP:").grid(row=0, column=0, padx=5, pady=5)
        
        self.ip_combobox = ttk.Combobox(ip_frame, width=20, state="readonly")
        self.ip_combobox.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Button(ip_frame, text="Обновить список IP", 
                  command=self.update_ip_list).grid(row=0, column=2, padx=5, pady=5)
        
        ttk.Label(ip_frame, text="Тип IP:").grid(row=0, column=3, padx=5, pady=5)
        
        self.ip_type = ttk.Combobox(ip_frame, values=['source_ip', 'destination_ip'], 
                                   state="readonly", width=15)
        self.ip_type.set('source_ip')
        self.ip_type.grid(row=0, column=4, padx=5, pady=5)
        
        ttk.Button(ip_frame, text="Добавить IP-фильтр", 
                  command=self.add_ip_filter).grid(row=0, column=5, padx=5, pady=5)
    
    def setup_filters_section(self, parent, row):
        ttk.Label(parent, text="Активные фильтры:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=5, sticky=tk.W)
        
        filters_container = ttk.Frame(parent)
        filters_container.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        self.filters_tree = ttk.Treeview(filters_container, columns=('field', 'condition', 'value', 'description'), 
                                       show='headings', height=6)
        self.filters_tree.heading('field', text='Поле')
        self.filters_tree.heading('condition', text='Условие')
        self.filters_tree.heading('value', text='Значение')
        self.filters_tree.heading('description', text='Описание')
        self.filters_tree.column('field', width=100)
        self.filters_tree.column('condition', width=100)
        self.filters_tree.column('value', width=120)
        self.filters_tree.column('description', width=200)
        
        # Скроллбар для дерева фильтров
        filter_scrollbar = ttk.Scrollbar(filters_container, orient=tk.VERTICAL, command=self.filters_tree.yview)
        self.filters_tree.configure(yscrollcommand=filter_scrollbar.set)
        
        self.filters_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        filter_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        filter_buttons_frame = ttk.Frame(parent)
        filter_buttons_frame.grid(row=row+2, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(filter_buttons_frame, text="Добавить фильтр", 
                  command=self.add_filter).grid(row=0, column=0, padx=5)
        ttk.Button(filter_buttons_frame, text="Управление фильтрами", 
                  command=self.manage_filters).grid(row=0, column=1, padx=5)
        ttk.Button(filter_buttons_frame, text="Очистить все", 
                  command=self.clear_filters).grid(row=0, column=2, padx=5)
    
    def setup_new_filter_section(self, parent, row):
        ttk.Label(parent, text="Добавить новый фильтр:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=10, sticky=tk.W)
        
        filter_form_frame = ttk.Frame(parent)
        filter_form_frame.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Label(filter_form_frame, text="Поле:").grid(row=0, column=0, padx=5, pady=5)
        
        available_fields = self.app.db_service.get_available_fields()
        self.filter_field = ttk.Combobox(filter_form_frame, values=available_fields, state="readonly", width=15)
        self.filter_field.grid(row=0, column=1, padx=5, pady=5)
        self.filter_field.set('protocol')
        self.filter_field.bind('<<ComboboxSelected>>', self.on_field_change)
        
        ttk.Label(filter_form_frame, text="Условие:").grid(row=0, column=2, padx=5, pady=5)
        
        self.filter_condition = ttk.Combobox(filter_form_frame, values=[
            'равно', 'содержит', 'больше чем', 'меньше чем', 'больше или равно', 
            'меньше или равно', 'не равно', 'в списке', 'между', 'начинается с', 'заканчивается на'
        ], state="readonly", width=15)
        self.filter_condition.grid(row=0, column=3, padx=5, pady=5)
        self.filter_condition.set('равно')
        self.filter_condition.bind('<<ComboboxSelected>>', self.on_condition_change)
        
        ttk.Label(filter_form_frame, text="Значение:").grid(row=0, column=4, padx=5, pady=5)
        self.filter_value = ttk.Entry(filter_form_frame, width=20)
        self.filter_value.grid(row=0, column=5, padx=5, pady=5)
        self.filter_value.bind('<KeyRelease>', self.on_value_change)
        
        ttk.Button(filter_form_frame, text="Добавить", 
                  command=self.add_filter).grid(row=0, column=6, padx=10, pady=5)
        
        hints_frame = ttk.Frame(filter_form_frame)
        hints_frame.grid(row=1, column=0, columnspan=7, sticky=tk.W, pady=5)
        
        ttk.Label(hints_frame, text="Подсказки:", font=("Arial", 9, "bold")).grid(row=0, column=0, sticky=tk.W)
        ttk.Label(hints_frame, text="• 'в списке': TCP,UDP,SSL", font=("Arial", 8)).grid(row=1, column=0, sticky=tk.W)
        ttk.Label(hints_frame, text="• 'между': 100,500", font=("Arial", 8)).grid(row=1, column=1, sticky=tk.W, padx=10)
        ttk.Label(hints_frame, text="• Для IP: 192.168.1.1", font=("Arial", 8)).grid(row=1, column=2, sticky=tk.W, padx=10)
        ttk.Label(hints_frame, text="• Для размеров: числа", font=("Arial", 8)).grid(row=1, column=3, sticky=tk.W, padx=10)
        
        # Описание текущего фильтра
        self.filter_description = ttk.Label(filter_form_frame, text="", font=("Arial", 9), foreground="blue")
        self.filter_description.grid(row=2, column=0, columnspan=7, sticky=tk.W, pady=2)
        
        self.update_filter_description()
    
    def setup_examples_section(self, parent, row):
        ttk.Label(parent, text="Примеры фильтров:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=10, sticky=tk.W)
        
        examples_frame = ttk.Frame(parent)
        examples_frame.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        examples = [
            ("protocol", "равно", "TCP", "Только TCP пакеты"),
            ("packet_size", "больше чем", "1000", "Крупные пакеты (>1000 байт)"),
            ("source_ip", "начинается с", "192.168.1", "IP из сети 192.168.1.x"),
            ("protocol", "в списке", "TCP,UDP,HTTP", "Основные протоколы"),
            ("packet_size", "между", "100,500", "Пакеты 100-500 байт"),
            ("destination_ip", "равно", "8.8.4.4", "Трафик к DNS серверу"),
            ("protocol", "не равно", "ARP", "Исключить ARP пакеты"),
            ("packet_size", "меньше чем", "100", "Мелкие пакеты (<100 байт)"),
            ("source_ip", "содержит", "10.0", "IP из сети 10.0.x.x")
        ]
        
        for i, (field, condition, value, desc) in enumerate(examples):
            example_frame = ttk.Frame(examples_frame)
            example_frame.grid(row=i//3, column=(i%3), sticky=tk.W, padx=5, pady=2)
            
            ttk.Button(example_frame, text="Добавить", width=8, 
                      command=lambda f=field, c=condition, v=value: self.add_example_filter(f, c, v)).pack(side=tk.LEFT)
            ttk.Label(example_frame, text=desc, font=("Arial", 9), wraplength=200).pack(side=tk.LEFT, padx=5)
    
    def setup_execution_section(self, parent, row):
        ttk.Label(parent, text="Выполнение запроса:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=10, sticky=tk.W)
        
        execute_frame = ttk.Frame(parent)
        execute_frame.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(execute_frame, text="Выполнить запрос с фильтрами", 
                  command=self.execute_filtered_query).grid(row=0, column=0, padx=5)
        ttk.Button(execute_frame, text="Показать SQL запрос", 
                  command=self.show_sql_query).grid(row=0, column=1, padx=5)
        ttk.Button(execute_frame, text="Следующие записи", 
                  command=self.next_records).grid(row=0, column=2, padx=5)
        ttk.Button(execute_frame, text="Предыдущие записи", 
                  command=self.prev_records).grid(row=0, column=3, padx=5)
    
    def setup_sql_section(self, parent, row):
        ttk.Label(parent, text="SQL запрос:", 
                 font=("Arial", 10, "bold")).grid(row=row, column=0, columnspan=4, pady=5, sticky=tk.W)
        
        sql_container = ttk.Frame(parent)
        sql_container.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        self.sql_query_text = tk.Text(sql_container, height=3, wrap=tk.WORD, font=("Courier", 9))
        sql_scrollbar = ttk.Scrollbar(sql_container, orient=tk.VERTICAL, command=self.sql_query_text.yview)
        self.sql_query_text.configure(yscrollcommand=sql_scrollbar.set)
        
        self.sql_query_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sql_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def setup_results_section(self, parent, row):
        ttk.Label(parent, text="Результаты фильтрации:", 
                 font=("Arial", 11, "bold")).grid(row=row, column=0, columnspan=4, pady=10, sticky=tk.W)
        
        results_container = ttk.Frame(parent)
        results_container.grid(row=row+1, column=0, columnspan=4, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        self.results_tree = ttk.Treeview(results_container, show='headings', height=8)
        
        # Вертикальный скроллбар
        v_scrollbar_results = ttk.Scrollbar(results_container, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=v_scrollbar_results.set)
        
        # Горизонтальный скроллбар
        h_scrollbar_results = ttk.Scrollbar(results_container, orient=tk.HORIZONTAL, command=self.results_tree.xview)
        self.results_tree.configure(xscrollcommand=h_scrollbar_results.set)
        
        # Размещаем элементы
        self.results_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        h_scrollbar_results.pack(side=tk.BOTTOM, fill=tk.X)
        v_scrollbar_results.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Настраиваем вес строки для правильного расширения
        parent.rowconfigure(row+1, weight=1)

    def on_field_change(self, event=None):
        self.update_filter_description()
    
    def on_condition_change(self, event=None):
        self.update_filter_description()
    
    def on_value_change(self, event=None):
        self.update_filter_description()
    
    def update_filter_description(self):
        field = self.filter_field.get()
        condition = self.filter_condition.get()
        value = self.filter_value.get()
        
        descriptions = {
            'равно': 'точное совпадение',
            'содержит': 'содержит текст',
            'больше чем': 'значения больше',
            'меньше чем': 'значения меньше', 
            'больше или равно': 'значения больше или равны',
            'меньше или равно': 'значения меньше или равны',
            'не равно': 'исключить значения',
            'в списке': 'любое из значений',
            'между': 'в диапазоне',
            'начинается с': 'начинается с текста',
            'заканчивается на': 'заканчивается текстом'
        }
        
        if field and condition:
            desc = f"Фильтр: {field} {descriptions.get(condition, condition)}"
            if value:
                desc += f" '{value}'"
            self.filter_description.config(text=desc)
    
    def validate_filter_value(self, field, condition, value):
        """Проверка корректности значения фильтра"""
        if not value:
            return False, "Значение не может быть пустым"
        
        # Проверка числовых полей
        numeric_fields = ['packet_size', 'length', 'size', 'port', 'src_port', 'dst_port']
        if field in numeric_fields:
            if condition in ['в списке', 'между']:
                # Для диапазонов и списков проверяем каждое число
                parts = [part.strip() for part in value.split(',')]
                for part in parts:
                    if condition == 'между' and len(parts) != 2:
                        return False, "Для 'между' нужно 2 значения через запятую"
                    if not part.isdigit():
                        return False, f"Для поля {field} значения должны быть числами"
            else:
                if not value.isdigit():
                    return False, f"Для поля {field} значение должно быть числом"
        
        # Проверка IP-адресов
        ip_fields = ['source_ip', 'destination_ip', 'src_ip', 'dst_ip']
        if field in ip_fields:
            ip_pattern = r'^(\d{1,3}\.){1,3}\d{1,3}'
            if condition in ['начинается с', 'содержит']:
                if not re.match(ip_pattern, value):
                    return False, "Неверный формат IP-адреса"
            elif condition == 'равно':
                ip_full_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
                if not re.match(ip_full_pattern, value):
                    return False, "Неверный формат IP-адреса"
        
        return True, "OK"
    
    def add_filter(self):
        field = self.filter_field.get().strip()
        condition = self.filter_condition.get().strip()
        value = self.filter_value.get().strip()
        
        if not field or not condition or not value:
            messagebox.showwarning("Предупреждение", "Пожалуйста, заполните все поля фильтра")
            return
        
        # Валидация значения
        is_valid, error_msg = self.validate_filter_value(field, condition, value)
        if not is_valid:
            messagebox.showwarning("Неверное значение", error_msg)
            return
        
        # Создание описания фильтра
        description = self.create_filter_description(field, condition, value)
        
        # Добавление в дерево
        self.filters_tree.insert('', tk.END, values=(field, condition, value, description))
        self.filter_value.delete(0, tk.END)
        self.filter_status.config(text=f"Добавлен фильтр: {description}")
        self.update_filter_description()
    
    def create_filter_description(self, field, condition, value):
        """Создает понятное описание фильтра"""
        condition_descriptions = {
            'равно': 'равно',
            'содержит': 'содержит',
            'больше чем': 'больше чем',
            'меньше чем': 'меньше чем',
            'больше или равно': 'больше или равно',
            'меньше или равно': 'меньше или равно',
            'не равно': 'не равно',
            'в списке': 'в списке',
            'между': 'между',
            'начинается с': 'начинается с',
            'заканчивается на': 'заканчивается на'
        }
        
        cond_symbol = condition_descriptions.get(condition, condition)
        
        if condition == 'в списке':
            return f"{field} {cond_symbol} {value}"
        elif condition == 'между':
            parts = value.split(',')
            if len(parts) == 2:
                return f"{field} {cond_symbol} {parts[0]} и {parts[1]}"
        elif condition in ['начинается с', 'заканчивается на']:
            return f"{field} {cond_symbol} {value}"
        
        return f"{field} {cond_symbol} {value}"
    
    def add_selected_protocols(self):
        selected_protocols = []
        for protocol, var in self.protocol_vars.items():
            if var.get():
                selected_protocols.append(protocol)
        
        if not selected_protocols:
            messagebox.showwarning("Предупреждение", "Пожалуйста, выберите хотя бы один протокол")
            return
        
        if len(selected_protocols) > 1:
            protocols_str = ','.join(selected_protocols)
            description = f"protocol в списке {protocols_str}"
            self.filters_tree.insert('', tk.END, values=('protocol', 'в списке', protocols_str, description))
            self.filter_status.config(text=f"Добавлены протоколы: {protocols_str}")
        else:
            description = f"protocol равно {selected_protocols[0]}"
            self.filters_tree.insert('', tk.END, values=('protocol', 'равно', selected_protocols[0], description))
            self.filter_status.config(text=f"Добавлен протокол: {selected_protocols[0]}")
    
    def update_ip_list(self):
        def task():
            self.app.data_management_tab.show_progress()
            self.app.data_management_tab.update_progress_text("Получение списка IP-адресов...")
            
            success, result = self.app.db_service.get_available_ips()
            
            if success:
                self.available_ips = result
                self.ip_combobox['values'] = self.available_ips
                if self.available_ips:
                    self.ip_combobox.set(self.available_ips[0])
                self.app.progress_queue.put(('complete', (True, f"Получено {len(self.available_ips)} IP-адресов")))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
    
    def add_ip_filter(self):
        ip_address = self.ip_combobox.get().strip()
        ip_type = self.ip_type.get().strip()
        
        if not ip_address:
            messagebox.showwarning("Предупреждение", "Пожалуйста, выберите IP-адрес")
            return
        
        if not ip_type:
            messagebox.showwarning("Предупреждение", "Пожалуйста, выберите тип IP")
            return
        
        description = f"{ip_type} равно {ip_address}"
        self.filters_tree.insert('', tk.END, values=(ip_type, 'равно', ip_address, description))
        self.filter_status.config(text=f"Добавлен IP-фильтр: {description}")
    
    def add_example_filter(self, field, condition, value):
        description = self.create_filter_description(field, condition, value)
        self.filters_tree.insert('', tk.END, values=(field, condition, value, description))
        self.filter_status.config(text=f"Добавлен пример: {description}")
    
    def manage_filters(self):
        """Окно управления всеми фильтрами"""
        manage_dialog = tk.Toplevel(self.frame)
        manage_dialog.title("Управление фильтрами")
        manage_dialog.geometry("600x400")
        manage_dialog.transient(self.frame)
        manage_dialog.grab_set()
        manage_dialog.resizable(True, True)
        
        # Центрируем окно
        manage_dialog.update_idletasks()
        x = (manage_dialog.winfo_screenwidth() - manage_dialog.winfo_width()) // 2
        y = (manage_dialog.winfo_screenheight() - manage_dialog.winfo_height()) // 2
        manage_dialog.geometry(f"+{x}+{y}")
        
        # Заголовок
        ttk.Label(manage_dialog, text="Управление фильтрами", 
                 font=("Arial", 14, "bold")).pack(pady=10)
        
        # Фрейм для списка фильтров
        list_frame = ttk.Frame(manage_dialog)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Treeview для отображения всех фильтров
        manage_tree = ttk.Treeview(list_frame, columns=('field', 'condition', 'value', 'description'), 
                                 show='headings', height=12)
        manage_tree.heading('field', text='Поле')
        manage_tree.heading('condition', text='Условие')
        manage_tree.heading('value', text='Значение')
        manage_tree.heading('description', text='Описание')
        manage_tree.column('field', width=100)
        manage_tree.column('condition', width=100)
        manage_tree.column('value', width=120)
        manage_tree.column('description', width=250)
        manage_tree.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        
        # Скроллбар
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=manage_tree.yview)
        scrollbar.pack(fill=tk.Y, side=tk.RIGHT)
        manage_tree.configure(yscrollcommand=scrollbar.set)
        
        # Заполняем список всеми фильтрами
        all_items = self.filters_tree.get_children()
        for item in all_items:
            values = self.filters_tree.item(item, 'values')
            manage_tree.insert('', tk.END, values=values, iid=item)
        
        # Кнопки управления
        button_frame = ttk.Frame(manage_dialog)
        button_frame.pack(pady=10)
        
        def delete_selected():
            selected = manage_tree.selection()
            if not selected:
                messagebox.showwarning("Предупреждение", "Выберите фильтры для удаления")
                return
            
            # Удаляем из основного дерева и из этого окна
            for item in selected:
                self.filters_tree.delete(item)
                manage_tree.delete(item)
            
            messagebox.showinfo("Успех", f"Удалено {len(selected)} фильтров")
        
        def close_management():
            manage_dialog.destroy()
        
        ttk.Button(button_frame, text="Удалить выбранные", 
                  command=delete_selected).grid(row=0, column=0, padx=5)
        ttk.Button(button_frame, text="Закрыть", 
                  command=close_management).grid(row=0, column=1, padx=5)
    
    def clear_filters(self):
        if not self.filters_tree.get_children():
            messagebox.showinfo("Информация", "Нет активных фильтров для очистки")
            return
            
        if messagebox.askyesno("Подтверждение", "Очистить все фильтры?"):
            for item in self.filters_tree.get_children():
                self.filters_tree.delete(item)
            self.filter_status.config(text="Все фильтры очищены")
    
    def get_current_filters(self):
        filters = []
        for item in self.filters_tree.get_children():
            values = self.filters_tree.item(item, 'values')
            if len(values) >= 3:
                filters.append((values[0], values[1], values[2]))
        return filters
    
    def show_sql_query(self):
        filters = self.get_current_filters()
        
        if not filters:
            query_display = "Нет активных фильтров. SQL запрос будет SELECT * FROM packets"
            self.sql_query_text.delete(1.0, tk.END)
            self.sql_query_text.insert(1.0, query_display)
            self.filter_status.config(text="Нет фильтров для отображения SQL")
            return
        
        # Генерация SQL запроса
        where_conditions = []
        for field, condition, value in filters:
            sql_condition = self.convert_to_sql_condition(field, condition, value)
            if sql_condition:
                where_conditions.append(sql_condition)
        
        if where_conditions:
            sql_query = f"SELECT * FROM packets WHERE {' AND '.join(where_conditions)}"
        else:
            sql_query = "SELECT * FROM packets"
        
        # Добавляем LIMIT и OFFSET
        limit, offset = self.get_limit_and_offset()
        if limit is not None:
            sql_query += f" LIMIT {limit} OFFSET {offset}"
        
        query_display = f"SQL запрос:\n{sql_query}\n\n"
        query_display += f"Активные фильтры ({len(filters)}):\n"
        for i, (field, condition, value) in enumerate(filters, 1):
            query_display += f"  {i}. {field} {condition} {value}\n"
        
        self.sql_query_text.delete(1.0, tk.END)
        self.sql_query_text.insert(1.0, query_display)
        self.filter_status.config(text=f"SQL запрос сгенерирован ({len(filters)} фильтров)")
    
    def convert_to_sql_condition(self, field, condition, value):
        """Конвертирует условия фильтра в SQL"""
        conditions_map = {
            'равно': '=',
            'не равно': '!=',
            'больше чем': '>',
            'меньше чем': '<',
            'больше или равно': '>=',
            'меньше или равно': '<='
        }
        
        if condition in conditions_map:
            return f"{field} {conditions_map[condition]} '{value}'"
        elif condition == 'содержит':
            return f"{field} LIKE '%{value}%'"
        elif condition == 'начинается с':
            return f"{field} LIKE '{value}%'"
        elif condition == 'заканчивается на':
            return f"{field} LIKE '%{value}'"
        elif condition == 'в списке':
            items = [f"'{item.strip()}'" for item in value.split(',')]
            return f"{field} IN ({', '.join(items)})"
        elif condition == 'между':
            parts = value.split(',')
            if len(parts) == 2:
                return f"{field} BETWEEN {parts[0].strip()} AND {parts[1].strip()}"
        
        return None
    
    def update_records_info(self):
        def task():
            success, result = self.app.db_service.get_total_records_count()
            if success:
                self.total_records = result
                self.records_info_label.config(text=f"Всего записей в базе данных: {self.total_records}")
                self.app.progress_queue.put(('complete', (True, f"Обновлена информация: {self.total_records} записей")))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
    
    def get_limit_and_offset(self):
        try:
            limit_str = self.limit_var.get()
            if limit_str == "Все":
                limit = None
            else:
                limit = int(limit_str)
            
            offset = int(self.offset_var.get())
            if offset < 0:
                offset = 0
            
            return limit, offset
        except ValueError:
            messagebox.showerror("Ошибка", "Неверное значение лимита или смещения")
            return None, None
    
    def next_records(self):
        if self.current_limit is None:
            return
        
        limit, offset = self.get_limit_and_offset()
        if limit is None:
            return
        
        new_offset = offset + limit
        if new_offset < self.total_records:
            self.offset_var.set(str(new_offset))
            self.execute_filtered_query()
        else:
            messagebox.showinfo("Информация", "Достигнут конец данных")
    
    def prev_records(self):
        if self.current_limit is None:
            return
        
        limit, offset = self.get_limit_and_offset()
        if limit is None:
            return
        
        new_offset = max(0, offset - limit)
        if new_offset != offset:
            self.offset_var.set(str(new_offset))
            self.execute_filtered_query()
        else:
            messagebox.showinfo("Информация", "Уже в начале данных")
    
    def execute_filtered_query(self):
        filters = self.get_current_filters()
        limit, offset = self.get_limit_and_offset()
        
        if not filters:
            if not messagebox.askyesno("Подтверждение", "Нет активных фильтров. Загрузить все данные?"):
                return
        
        def task():
            self.app.data_management_tab.show_progress()
            self.app.data_management_tab.update_progress_text("Выполнение запроса с фильтрами...")
            
            success, result = self.app.db_service.get_filtered_data(filters, limit, offset)
            
            if success:
                columns, data, sql_query, params = result
                self.display_filtered_results(columns, data)
                
                self.current_limit = limit
                self.current_offset = offset
                
                query_display = f"SQL запрос:\n{sql_query}\n\n"
                query_display += f"Параметры: {params}\n\n"
                query_display += f"Найдено записей: {len(data)}\n"
                query_display += f"Активных фильтров: {len(filters)}"
                
                self.sql_query_text.delete(1.0, tk.END)
                self.sql_query_text.insert(1.0, query_display)
                
                self.app.current_sql_query = sql_query
                self.app.current_query_params = params
                self.app.last_analysis_result = {
                    'columns': columns,
                    'data': data,
                    'name': 'Отфильтрованные данные пакетов',
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'query': sql_query,
                    'limit': limit,
                    'offset': offset,
                    'filters': filters
                }
                
                self.app.export_tab.update_export_preview(columns, data, "Отфильтрованные данные пакетов")
                
                if limit is None:
                    status_text = f"Найдено {len(data)} записей (все данные)"
                else:
                    status_text = f"Найдено {len(data)} записей (позиция: {offset + 1}-{offset + len(data)})"
                
                if filters:
                    status_text += f", фильтров: {len(filters)}"
                
                self.filter_status.config(text=status_text)
                self.app.progress_queue.put(('complete', (True, f"Фильтрация завершена. {status_text}")))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
    
    def display_filtered_results(self, columns, data):
        """Отображение отфильтрованных результатов в Treeview"""
        # Очищаем существующие данные
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        
        # Устанавливаем колонки
        self.results_tree["columns"] = columns
        
        # Настраиваем заголовки колонок
        for col in columns:
            self.results_tree.heading(col, text=col)
            # Автоподбор ширины колонок
            self.results_tree.column(col, width=120, minwidth=50, stretch=True)
        
        # Добавляем данные
        for row in data:
            self.results_tree.insert("", tk.END, values=row)
        
        # Обновляем отображение
        self.results_tree.update()