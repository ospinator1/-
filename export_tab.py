import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import json
from datetime import datetime
import os

class ExportTab:
    def __init__(self, parent, app):
        self.app = app
        self.frame = ttk.Frame(parent)
        self.setup_ui()
    
    def setup_ui(self):
        main_frame = ttk.Frame(self.frame, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        title_label = ttk.Label(main_frame, text="Экспорт результатов анализа", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=10)
        
        ttk.Label(main_frame, text="Информация о последнем анализе:", 
                 font=("Arial", 11, "bold")).grid(row=1, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        self.last_analysis_info = ttk.Label(main_frame, text="Анализ не выполнялся", 
                                           font=("Arial", 10), wraplength=1000)
        self.last_analysis_info.grid(row=2, column=0, columnspan=3, pady=5, sticky=tk.W)

        ttk.Label(main_frame, text="Предварительный просмотр данных:", 
                 font=("Arial", 11, "bold")).grid(row=3, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        preview_frame = ttk.Frame(main_frame)
        preview_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        ttk.Label(preview_frame, text="JSON (первые 100 записей):").grid(row=0, column=0, sticky=tk.W)
        
        self.json_preview = tk.Text(preview_frame, height=12, width=80, wrap=tk.WORD)
        json_scrollbar = ttk.Scrollbar(preview_frame, orient=tk.VERTICAL, command=self.json_preview.yview)
        self.json_preview.configure(yscrollcommand=json_scrollbar.set)
        
        self.json_preview.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        json_scrollbar.grid(row=1, column=1, sticky=(tk.N, tk.S))
        
        ttk.Label(main_frame, text="Настройки экспорта:", 
                 font=("Arial", 11, "bold")).grid(row=5, column=0, columnspan=3, pady=5, sticky=tk.W)
        
        options_frame = ttk.Frame(main_frame)
        options_frame.grid(row=6, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        self.preview_limit_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Ограничить превью (первые 100 записей)", 
                       variable=self.preview_limit_var).grid(row=0, column=0, sticky=tk.W, padx=5)
        
        self.stream_export_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Потоковый экспорт для больших файлов", 
                       variable=self.stream_export_var).grid(row=0, column=1, sticky=tk.W, padx=5)
        
        ttk.Label(main_frame, text="Экспорт данных:", 
                 font=("Arial", 11, "bold")).grid(row=7, column=0, columnspan=3, pady=10, sticky=tk.W)
        
        export_buttons_frame = ttk.Frame(main_frame)
        export_buttons_frame.grid(row=8, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(export_buttons_frame, text="Экспорт в JSON", 
                  command=self.export_to_json).pack(side=tk.LEFT, padx=5)
        ttk.Button(export_buttons_frame, text="Экспорт в XLSX", 
                  command=self.export_to_xlsx).pack(side=tk.LEFT, padx=5)
        ttk.Button(export_buttons_frame, text="Экспорт в оба формата", 
                  command=self.export_to_both).pack(side=tk.LEFT, padx=5)
        ttk.Button(export_buttons_frame, text="Очистить превью", 
                  command=self.clear_preview).pack(side=tk.LEFT, padx=5)

        ttk.Button(export_buttons_frame, text="Экспорт ВСЕХ данных", 
                  command=self.export_all_data).pack(side=tk.LEFT, padx=5)
        
        self.export_status = ttk.Label(main_frame, text="Готов к экспорту", font=("Arial", 10))
        self.export_status.grid(row=9, column=0, columnspan=3, pady=10)
        
        # Секция авто-статистики
        ttk.Label(main_frame, text="Автоматическая статистика:", 
                 font=("Arial", 11, "bold")).grid(row=10, column=0, columnspan=3, pady=10, sticky=tk.W)
        
        stats_frame = ttk.Frame(main_frame)
        stats_frame.grid(row=11, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        # Информация о статистике
        self.stats_info_label = ttk.Label(stats_frame, text="Статистика: не загружена", 
                                         font=("Arial", 9))
        self.stats_info_label.pack(side=tk.TOP, fill=tk.X, pady=2)
        
        # Область для отображения быстрой статистики
        self.stats_text = tk.Text(stats_frame, height=8, width=80, wrap=tk.WORD, font=("Courier", 9))
        stats_scrollbar = ttk.Scrollbar(stats_frame, orient=tk.VERTICAL, command=self.stats_text.yview)
        self.stats_text.configure(yscrollcommand=stats_scrollbar.set)
        
        self.stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stats_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(1, weight=1)
        
        # Автоматическое обновление статистики при запуске
        self.refresh_stats_info()
    
    def update_export_preview(self, columns, data, analysis_name):
        self.app.last_analysis_result = {
            'columns': columns,
            'data': data,
            'name': analysis_name,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_records': len(data)
        }
        
        info_text = f"Анализ: {analysis_name}\nВремя: {self.app.last_analysis_result['timestamp']}\nЗаписей: {len(data)}"
        self.last_analysis_info.config(text=info_text)
        
        preview_data = self.app.last_analysis_result.copy()
        if self.preview_limit_var.get() and len(data) > 100:
            preview_data['data'] = data[:100]
            preview_data['preview_note'] = f"Показано первых 100 записей из {len(data)}"
        
        formatted_json = self.app.export_service.format_for_preview(preview_data)
        self.json_preview.delete(1.0, tk.END)
        self.json_preview.insert(1.0, formatted_json)
        
        status_text = f"Данные готовы для экспорта ({len(data)} записей)"
        if len(data) > 10000:
            status_text += " - рекомендуется использовать потоковый экспорт"
        self.export_status.config(text=status_text)
        
        self.refresh_stats_info()
    
    def export_to_json(self):
        if not self.app.last_analysis_result:
            messagebox.showwarning("Внимание", "Сначала выполните анализ данных")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Сохранить как JSON",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if file_path:
            try:
                export_options = {
                    'stream_large_files': self.stream_export_var.get(),
                    'preview_limit': self.preview_limit_var.get()
                }
                
                success, message = self.app.export_service.export_to_json(
                    self.app.last_analysis_result, file_path, export_options
                )
                
                if success:
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    file_size_mb = file_size / (1024 * 1024)
                    
                    status_text = f"Данные успешно экспортированы в JSON: {file_path} ({file_size_mb:.2f} MB)"
                    self.export_status.config(text=status_text)
                    messagebox.showinfo("Успех", 
                                      f"Данные экспортированы в:\n{file_path}\n"
                                      f"Размер файла: {file_size_mb:.2f} MB\n"
                                      f"Записей: {len(self.app.last_analysis_result['data'])}")
                else:
                    raise Exception(message)
                
            except Exception as e:
                error_msg = f"Ошибка при экспорте в JSON: {str(e)}"
                self.export_status.config(text=error_msg)
                messagebox.showerror("Ошибка", error_msg)
    
    def export_to_xlsx(self):
        if not self.app.last_analysis_result:
            messagebox.showwarning("Внимание", "Сначала выполните анализ данных")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Сохранить как XLSX",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        
        if file_path:
            try:
                export_options = {
                    'stream_large_files': self.stream_export_var.get(),
                    'preview_limit': self.preview_limit_var.get()
                }
                
                success, message = self.app.export_service.export_to_xlsx(
                    self.app.last_analysis_result, file_path, export_options
                )
                
                if success:
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    file_size_mb = file_size / (1024 * 1024)
                    
                    status_text = f"Данные успешно экспортированы в XLSX: {file_path} ({file_size_mb:.2f} MB)"
                    self.export_status.config(text=status_text)
                    messagebox.showinfo("Успех", 
                                      f"Данные экспортированы в:\n{file_path}\n"
                                      f"Размер файла: {file_size_mb:.2f} MB\n"
                                      f"Записей: {len(self.app.last_analysis_result['data'])}")
                else:
                    raise Exception(message)
                
            except Exception as e:
                error_msg = f"Ошибка при экспорте в XLSX: {str(e)}"
                self.export_status.config(text=error_msg)
                messagebox.showerror("Ошибка", error_msg)
    
    def export_to_both(self):
        if not self.app.last_analysis_result:
            messagebox.showwarning("Внимание", "Сначала выполните анализ данных")
            return
        
        export_dir = filedialog.askdirectory(title="Выберите папку для экспорта")
        if not export_dir:
            return
        
        base_filename = f"network_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            export_options = {
                'stream_large_files': self.stream_export_var.get(),
                'preview_limit': self.preview_limit_var.get()
            }
            
            success, json_path, xlsx_path = self.app.export_service.export_to_both(
                self.app.last_analysis_result, export_dir, base_filename, export_options
            )
            
            if success:
                json_size = os.path.getsize(json_path) if os.path.exists(json_path) else 0
                xlsx_size = os.path.getsize(xlsx_path) if os.path.exists(xlsx_path) else 0
                
                status_text = (f"Данные экспортированы в оба формата:\n"
                             f"JSON: {json_path} ({json_size/1024/1024:.2f} MB)\n"
                             f"XLSX: {xlsx_path} ({xlsx_size/1024/1024:.2f} MB)")
                self.export_status.config(text=status_text)
                
                messagebox.showinfo("Успех", 
                                  f"Данные экспортированы в оба формата:\n\n"
                                  f"JSON: {json_path}\n"
                                  f"Размер: {json_size/1024/1024:.2f} MB\n\n"
                                  f"XLSX: {xlsx_path}\n"
                                  f"Размер: {xlsx_size/1024/1024:.2f} MB\n\n"
                                  f"Всего записей: {len(self.app.last_analysis_result['data'])}")
            else:
                raise Exception("Не удалось экспортировать в оба формата")
                
        except Exception as e:
            error_msg = f"Ошибка при экспорте: {str(e)}"
            self.export_status.config(text=error_msg)
            messagebox.showerror("Ошибка", error_msg)
    
    def export_all_data(self):
        """Экспорт всех данных без ограничений"""
        if not hasattr(self.app, 'db_service') or not self.app.db_service:
            messagebox.showwarning("Внимание", "Сервис базы данных не доступен")
            return

        response = messagebox.askyesno(
            "Подтверждение", 
            "Эта операция загрузит ВСЕ данные из базы данных без ограничений.\n"
            "Это может занять много времени и памяти для больших наборов данных.\n\n"
            "Продолжить?"
        )
        
        if not response:
            return
        
        def task():
            self.app.data_management_tab.show_progress()
            self.app.data_management_tab.update_progress_text("Загрузка всех данных...")
            
            success, result = self.app.db_service.get_all_data()
            
            if success:
                columns, data = result
                analysis_name = "Все данные из базы данных"

                self.app.root.after(0, lambda: self.update_export_preview(columns, data, analysis_name))
                self.app.progress_queue.put(('complete', (True, f"Загружено {len(data)} записей")))
            else:
                self.app.progress_queue.put(('complete', (False, result)))
        
        self.app.run_in_thread(task)
    
    def clear_preview(self):
        self.app.last_analysis_result = None
        self.app.current_sql_query = None
        self.app.current_query_params = None
        self.last_analysis_info.config(text="Анализ не выполнялся")
        self.json_preview.delete(1.0, tk.END)
        self.export_status.config(text="Превью очищено")

    def refresh_stats_info(self):
        """Обновление информации о статистике"""
        def task():
            stats = self.app.db_service.get_stats_summary()
            self.app.root.after(0, lambda: self.update_stats_display(stats))
        
        self.app.root.after(100, task)

    def update_stats_display(self, stats):
        """Обновление отображения статистики"""
        if 'error' in stats:
            self.stats_info_label.config(text=f"Ошибка: {stats['error']}")
            self.stats_text.delete(1.0, tk.END)
            self.stats_text.insert(1.0, f"Ошибка загрузки статистики: {stats['error']}")
            return
        
        self.stats_info_label.config(
            text=f"Статистика: {stats['total_packets']} пакетов, "
                 f"{stats['unique_protocols']} протоколов, "
                 f"{stats['unique_ips']} IP-адресов, "
                 f"обновлено: {stats['last_updated']}\n"
                 f"Файлы статистики сохраняются автоматически в C:\\Users\\Assa\\source\\repos\\network_stats"
        )
        
        # Формируем текст для отображения
        stats_text = f"Общая статистика сетевых данных\n"
        stats_text += f"Всего пакетов: {stats['total_packets']:,}\n"
        stats_text += f"Уникальных протоколов: {stats['unique_protocols']}\n"
        stats_text += f"Уникальных IP-адресов: {stats['unique_ips']}\n"
        stats_text += f"Общий объем трафика: {stats['total_traffic']:,} байт\n"
        stats_text += f"Последнее обновление: {stats['last_updated']}\n"
        
        self.stats_text.delete(1.0, tk.END)
        self.stats_text.insert(1.0, stats_text)