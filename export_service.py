import json
import pandas as pd
import openpyxl
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import os
import gc
import math

class ExportService:
    
    @staticmethod
    def export_to_json(data: Dict[str, Any], file_path: str, export_options: Optional[Dict] = None) -> Tuple[bool, str]:
        """
        Экспорт данных в JSON формат без ограничений на количество записей
        """
        try:
            if export_options is None:
                export_options = {}
            
            total_records = len(data['data'])
            
            # Для очень больших файлов используем потоковую запись
            if export_options.get('stream_large_files', False) and total_records > 10000:
                return ExportService._export_json_streaming(data, file_path, export_options)
            else:
                return ExportService._export_json_standard(data, file_path, export_options)
                
        except Exception as e:
            error_msg = f"Ошибка при экспорте в JSON: {str(e)}"
            print(error_msg)
            return False, error_msg
    
    @staticmethod
    def _export_json_standard(data: Dict[str, Any], file_path: str, export_options: Dict) -> Tuple[bool, str]:
        """Стандартный экспорт в JSON для небольших объемов данных"""
        try:
            export_data = {
                "analysis_name": data.get('name', 'Unknown Analysis'),
                "timestamp": data.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "columns": data.get('columns', []),
                "metadata": {
                    "total_records": len(data['data']),
                    "export_format": "JSON",
                    "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "file_size_estimate": "standard"
                },
                "data": []
            }
            
            # Обрабатываем все данные без ограничений
            for row in data['data']:
                row_dict = {}
                for i, col in enumerate(data['columns']):
                    value = row[i] if i < len(row) else None
                    # Конвертируем специальные типы данных
                    if hasattr(value, 'isoformat'):
                        value = value.isoformat()
                    elif value is None:
                        value = ""
                    row_dict[col] = value
                export_data["data"].append(row_dict)
            
            # Записываем весь файл
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            # Очищаем память
            del export_data
            gc.collect()
            
            return True, "Успешный экспорт"
            
        except Exception as e:
            return False, f"Ошибка стандартного экспорта: {str(e)}"
    
    @staticmethod
    def _export_json_streaming(data: Dict[str, Any], file_path: str, export_options: Dict) -> Tuple[bool, str]:
        """Потоковый экспорт в JSON для больших объемов данных"""
        try:
            total_records = len(data['data'])
            batch_size = export_options.get('batch_size', 10000)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                # Записываем начало файла
                f.write('{\n')
                f.write(f'  "analysis_name": "{data.get("name", "Unknown Analysis")}",\n')
                f.write(f'  "timestamp": "{data.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}",\n')
                f.write('  "columns": [\n')
                
                # Записываем колонки
                columns = data.get('columns', [])
                for i, col in enumerate(columns):
                    f.write(f'    "{col}"')
                    if i < len(columns) - 1:
                        f.write(',')
                    f.write('\n')
                
                f.write('  ],\n')
                f.write('  "metadata": {\n')
                f.write(f'    "total_records": {total_records},\n')
                f.write(f'    "export_format": "JSON",\n')
                f.write(f'    "exported_at": "{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}",\n')
                f.write(f'    "file_size_estimate": "large_streaming"\n')
                f.write('  },\n')
                f.write('  "data": [\n')
                
                # Потоковая запись данных
                for i, row in enumerate(data['data']):
                    row_dict = {}
                    for j, col in enumerate(columns):
                        value = row[j] if j < len(row) else None
                        if hasattr(value, 'isoformat'):
                            value = value.isoformat()
                        elif value is None:
                            value = ""
                        row_dict[col] = value
                    
                    # Записываем строку
                    json_line = json.dumps(row_dict, ensure_ascii=False, default=str)
                    f.write('    ' + json_line)
                    
                    if i < total_records - 1:
                        f.write(',')
                    f.write('\n')
                    
                    # Периодически сбрасываем буфер
                    if i % batch_size == 0:
                        f.flush()
                        os.fsync(f.fileno())
                
                # Записываем конец файла
                f.write('  ]\n')
                f.write('}\n')
            
            return True, "Успешный потоковый экспорт"
            
        except Exception as e:
            return False, f"Ошибка потокового экспорта: {str(e)}"
    
    @staticmethod
    def export_to_xlsx(data: Dict[str, Any], file_path: str, export_options: Optional[Dict] = None) -> Tuple[bool, str]:
        """
        Экспорт данных в XLSX формат без ограничений на количество записей
        """
        try:
            if export_options is None:
                export_options = {}
            
            total_records = len(data['data'])
            
            # Для очень больших файлов используем пакетную обработку
            if export_options.get('stream_large_files', False) and total_records > 50000:
                return ExportService._export_xlsx_batched(data, file_path, export_options)
            else:
                return ExportService._export_xlsx_standard(data, file_path, export_options)
                
        except Exception as e:
            error_msg = f"Ошибка при экспорте в XLSX: {str(e)}"
            print(error_msg)
            return False, error_msg
    
    @staticmethod
    def _export_xlsx_standard(data: Dict[str, Any], file_path: str, export_options: Dict) -> Tuple[bool, str]:
        """Стандартный экспорт в XLSX"""
        try:
            # Создаем DataFrame со всеми данными
            df = pd.DataFrame(
                data['data'],
                columns=data['columns']
            )
            
            # Оптимизация памяти для больших DataFrame
            if len(df) > 10000:
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype('string')
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Лист с данными
                df.to_excel(writer, sheet_name='Данные анализа', index=False)
                
                # Лист с метаданными
                metadata = {
                    'Параметр': [
                        'Название анализа',
                        'Время анализа', 
                        'Время экспорта',
                        'Всего записей',
                        'Количество колонок',
                        'Размер данных (приблизительно)'
                    ],
                    'Значение': [
                        data.get('name', 'Unknown'),
                        data.get('timestamp', 'Unknown'),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        len(data['data']),
                        len(data['columns']),
                        f"{len(data['data']) * len(data['columns']) * 50 / 1024 / 1024:.2f} MB"
                    ]
                }
                
                pd.DataFrame(metadata).to_excel(writer, sheet_name='Метаданные', index=False)
                
                # Форматирование
                workbook = writer.book
                data_sheet = workbook['Данные анализа']
                meta_sheet = workbook['Метаданные']
                
                # Автоподбор ширины колонок для данных
                for column in data_sheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            cell_value = str(cell.value) if cell.value is not None else ""
                            if len(cell_value) > max_length:
                                max_length = len(cell_value)
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 100)  # Увеличил максимальную ширину
                    data_sheet.column_dimensions[column_letter].width = adjusted_width
                
                # Форматирование метаданных
                for column in meta_sheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    meta_sheet.column_dimensions[column_letter].width = adjusted_width
            
            # Очистка памяти
            del df
            gc.collect()
            
            return True, "Успешный экспорт в XLSX"
            
        except Exception as e:
            return False, f"Ошибка стандартного экспорта в XLSX: {str(e)}"
    
    @staticmethod
    def _export_xlsx_batched(data: Dict[str, Any], file_path: str, export_options: Dict) -> Tuple[bool, str]:
        """Пакетный экспорт в XLSX для очень больших объемов данных"""
        try:
            total_records = len(data['data'])
            batch_size = export_options.get('batch_size', 100000)  # 100k записей на лист
            num_batches = math.ceil(total_records / batch_size)
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Экспортируем данные пакетами
                for batch_num in range(num_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, total_records)
                    
                    batch_data = data['data'][start_idx:end_idx]
                    
                    df_batch = pd.DataFrame(
                        batch_data,
                        columns=data['columns']
                    )
                    
                    sheet_name = f'Данные_{batch_num + 1}'
                    if num_batches == 1:
                        sheet_name = 'Данные анализа'
                    
                    df_batch.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Очистка памяти после каждого пакета
                    del df_batch
                    gc.collect()
                
                # Лист с метаданными
                metadata = {
                    'Параметр': [
                        'Название анализа',
                        'Время анализа',
                        'Время экспорта', 
                        'Всего записей',
                        'Количество колонок',
                        'Количество пакетов',
                        'Размер пакета',
                        'Общий размер (приблизительно)'
                    ],
                    'Значение': [
                        data.get('name', 'Unknown'),
                        data.get('timestamp', 'Unknown'),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        total_records,
                        len(data['columns']),
                        num_batches,
                        batch_size,
                        f"{total_records * len(data['columns']) * 50 / 1024 / 1024:.2f} MB"
                    ]
                }
                
                pd.DataFrame(metadata).to_excel(writer, sheet_name='Метаданные', index=False)
                
                # Форматирование всех листов
                workbook = writer.book
                for sheet_name in workbook.sheetnames:
                    sheet = workbook[sheet_name]
                    for column in sheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                cell_value = str(cell.value) if cell.value is not None else ""
                                if len(cell_value) > max_length:
                                    max_length = len(cell_value)
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 100)
                        sheet.column_dimensions[column_letter].width = adjusted_width
            
            return True, f"Успешный пакетный экспорт в XLSX ({num_batches} пакетов)"
            
        except Exception as e:
            return False, f"Ошибка пакетного экспорта в XLSX: {str(e)}"
    
    @staticmethod
    def export_to_both(data: Dict[str, Any], export_dir: str, base_filename: str, 
                      export_options: Optional[Dict] = None) -> Tuple[bool, str, str]:
        """
        Экспорт данных в оба формата (JSON и XLSX) без ограничений
        """
        try:
            if export_options is None:
                export_options = {}
            
            json_path = os.path.join(export_dir, f"{base_filename}.json")
            xlsx_path = os.path.join(export_dir, f"{base_filename}.xlsx")
            
            # Экспорт в JSON
            success_json, json_msg = ExportService.export_to_json(data, json_path, export_options)
            if not success_json:
                return False, json_msg, ""
            
            # Экспорт в XLSX  
            success_xlsx, xlsx_msg = ExportService.export_to_xlsx(data, xlsx_path, export_options)
            if not success_xlsx:
                # Удаляем частично созданный JSON файл если XLSX не удался
                if os.path.exists(json_path):
                    os.remove(json_path)
                return False, xlsx_msg, ""
            
            return True, json_path, xlsx_path
            
        except Exception as e:
            error_msg = f"Ошибка при экспорте в оба формата: {str(e)}"
            print(error_msg)
            return False, error_msg, ""
    
    @staticmethod
    def format_for_preview(data: Dict[str, Any]) -> str:
        """
        Форматирование данных для превью (ограниченное количество записей)
        """
        try:
            preview_limit = 10  # Показываем только первые 10 записей в превью
            total_records = len(data.get('data', []))
            
            preview_data = {
                "analysis_name": data.get('name', 'Unknown Analysis'),
                "timestamp": data.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "columns": data.get('columns', []),
                "metadata": {
                    "total_records": total_records,
                    "preview_records": min(preview_limit, total_records),
                    "export_format": "PREVIEW",
                    "preview_note": f"Показано первых {min(preview_limit, total_records)} из {total_records} записей",
                    "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
                "data_preview": []
            }
            
            # Берем только ограниченное количество записей для превью
            data_preview = data['data'][:preview_limit] if total_records > 0 else []
            
            for row in data_preview:
                row_dict = {}
                for i, col in enumerate(data['columns']):
                    value = row[i] if i < len(row) else None
                    # Ограничиваем длину строк для превью
                    if isinstance(value, str) and len(value) > 100:
                        value = value[:100] + "..."
                    elif hasattr(value, 'isoformat'):
                        value = value.isoformat()
                    elif value is None:
                        value = ""
                    row_dict[col] = value
                preview_data["data_preview"].append(row_dict)
            
            return json.dumps(preview_data, indent=2, ensure_ascii=False, default=str)
            
        except Exception as e:
            return f"Ошибка форматирования превью: {str(e)}"
    
    @staticmethod
    def estimate_file_size(data: Dict[str, Any]) -> Dict[str, float]:
        """
        Оценка размера файлов для экспорта
        """
        total_records = len(data.get('data', []))
        num_columns = len(data.get('columns', []))
        
        # Приблизительная оценка размера (в байтах)
        avg_record_size = num_columns * 50  # 50 байт на поле в среднем
        
        json_size = total_records * avg_record_size * 1.2  # JSON обычно больше из-за форматирования
        xlsx_size = total_records * avg_record_size * 0.8  # XLSX обычно более компактный
        
        return {
            "json_mb": json_size / (1024 * 1024),
            "xlsx_mb": xlsx_size / (1024 * 1024),
            "total_records": total_records,
            "estimated_memory_mb": (total_records * num_columns * 100) / (1024 * 1024)  # Оценка использования памяти
        }