from sqlalchemy import text, func, and_, or_
from sqlalchemy.orm import Session
from models.models import PacketData, ProtocolStats, IPStats
from models.database import DatabaseManager
import re
from typing import List, Tuple, Optional, Dict, Any
import os
from datetime import datetime
import subprocess
import shutil

class DatabaseService:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        # Автоматически создаем директорию для статистики
        self.stats_dir = 'C:\\Users\\Assa\\source\\repos\\network_stats'
        self.backup_dir = 'C:\\Users\\Assa\\source\\repos\\db_backups'
        os.makedirs(self.stats_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def get_session(self) -> Session:
        return self.db_manager.get_session()
    
    def create_tables(self) -> Tuple[bool, str]:
        return self.db_manager.create_tables()
    
    def drop_tables(self) -> Tuple[bool, str]:
        return self.db_manager.drop_tables()
    
    def drop_and_recreate_tables(self) -> Tuple[bool, str]:
        success_drop, message_drop = self.drop_tables()
        if not success_drop:
            return False, message_drop
        return self.create_tables()
    
    def clear_tables(self) -> Tuple[bool, str]:
        try:
            session = self.get_session()
            
            session.query(IPStats).delete()
            session.query(ProtocolStats).delete()
            session.query(PacketData).delete()
            
            session.commit()
            
            # Автоматически создаем статистику после очистки
            self.auto_save_stats()
            
            return True, "Все таблицы успешно очищены"
        except Exception as e:
            session.rollback()
            return False, f"Ошибка очистки таблиц: {e}"
        finally:
            session.close()
    
    def clear_database(self) -> Tuple[bool, str]:
        try:
            session = self.get_session()
            
            # Получаем все таблицы
            result = session.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result]
            
            if not tables:
                return True, "База данных уже пуста"
            
            # Отключаем ограничения
            session.execute(text("SET session_replication_role = replica;"))
            
            # Очищаем каждую таблицу
            for table in tables:
                session.execute(text(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE'))
            
            # Включаем ограничения обратно
            session.execute(text("SET session_replication_role = DEFAULT;"))
            
            session.commit()
            
            # Автоматически создаем статистику после очистки
            self.auto_save_stats()
            
            return True, f"База данных полностью очищена. Затронуто таблиц: {len(tables)}"
        except Exception as e:
            session.rollback()
            return False, f"Ошибка очистки базы данных: {e}"
        finally:
            session.close()
    
    def auto_save_stats(self):
        """Автоматическое сохранение статистики в TXT файл"""
        try:
            stats = self.get_stats_summary()
            if 'error' not in stats:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"auto_stats_{timestamp}.txt"
                filepath = os.path.join(self.stats_dir, filename)
                
                stats_text = self._format_stats_for_file(stats)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(stats_text)
                
                print(f"Автоматически сохранена статистика: {filepath}")
                return True, filepath
            return False, "Нет данных для статистики"
        except Exception as e:
            print(f"Ошибка автоматического сохранения статистики: {e}")
            return False, str(e)
    
    def _format_stats_for_file(self, stats: dict) -> str:
        """Форматирование статистики для сохранения в файл"""
        stats_text = "АВТОМАТИЧЕСКАЯ СТАТИСТИКА СЕТЕВЫХ ДАННЫХ\n"
        stats_text += "=" * 60 + "\n"
        stats_text += f"Время создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        stats_text += f"Всего пакетов: {stats.get('total_packets', 0):,}\n"
        stats_text += f"Уникальных протоколов: {stats.get('unique_protocols', 0)}\n"
        stats_text += f"Уникальных IP-адресов: {stats.get('unique_ips', 0)}\n"
        stats_text += f"Общий объем трафика: {stats.get('total_traffic', 0):,} байт\n\n"
        
        stats_text += "ТОП-5 ПРОТОКОЛОВ:\n"
        stats_text += "-" * 40 + "\n"
        if stats.get('top_protocols'):
            for proto in stats['top_protocols']:
                stats_text += f"{proto['protocol']}: {proto['count']} пакетов\n"
        else:
            stats_text += "Нет данных\n"
        
        stats_text += f"\nФайл создан автоматически\n"
        stats_text += f"Директория: {self.stats_dir}\n"
        
        return stats_text
    
    def get_database_size(self) -> Tuple[bool, Any]:
        try:
            session = self.get_session()
            
            result = session.execute(text("""
                SELECT 
                    pg_size_pretty(pg_database_size(current_database())) as db_size,
                    (SELECT COUNT(*) FROM packet_data) as packet_count,
                    (SELECT COUNT(*) FROM protocol_stats) as protocol_stats_count,
                    (SELECT COUNT(*) FROM ip_stats) as ip_stats_count
            """))
            
            row = result.fetchone()
            if row:
                size_info = {
                    'db_size': row[0],
                    'packet_count': row[1],
                    'protocol_stats_count': row[2],
                    'ip_stats_count': row[3]
                }
                return True, size_info
            else:
                return False, "Не удалось получить информацию о размере БД"
        except Exception as e:
            return False, f"Ошибка получения информации о БД: {e}"
    
    def get_table_sizes(self) -> Tuple[bool, Any]:
        try:
            session = self.get_session()
            
            result = session.execute(text("""
                SELECT 
                    table_name,
                    pg_size_pretty(pg_total_relation_size('"' || table_schema || '"."' || table_name || '"')) as size,
                    pg_size_pretty(pg_relation_size('"' || table_schema || '"."' || table_name || '"')) as table_size
                FROM information_schema.tables t1
                WHERE table_schema = 'public'
                ORDER BY pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') DESC
            """))
            
            tables_info = result.fetchall()
            return True, tables_info
        except Exception as e:
            return False, f"Ошибка получения информации о таблицах: {e}"
    
    def get_table_list(self) -> Tuple[bool, List[str]]:
        try:
            session = self.get_session()
            
            result = session.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            
            tables = [row[0] for row in result]
            return True, tables
        except Exception as e:
            return False, f"Ошибка получения списка таблиц: {e}"
    
    def get_total_records_count(self) -> Tuple[bool, int]:
        try:
            session = self.get_session()
            count = session.query(PacketData).count()
            return True, count
        except Exception as e:
            return False, f"Ошибка получения количества записей: {e}"
    
    def get_available_protocols(self) -> Tuple[bool, List[str]]:
        try:
            session = self.get_session()
            protocols = session.query(PacketData.protocol).filter(
                PacketData.protocol.isnot(None),
                PacketData.protocol != '',
                PacketData.protocol != '-'
            ).distinct().order_by(PacketData.protocol).all()
            
            return True, [p[0] for p in protocols]
        except Exception as e:
            return False, f"Ошибка получения списка протоколов: {e}"
    
    def get_available_ips(self) -> Tuple[bool, List[str]]:
        try:
            session = self.get_session()
            
            source_ips = session.query(PacketData.source_ip).filter(
                PacketData.source_ip.isnot(None),
                PacketData.source_ip != '',
                PacketData.source_ip != '-'
            ).distinct()
            
            dest_ips = session.query(PacketData.destination_ip).filter(
                PacketData.destination_ip.isnot(None),
                PacketData.destination_ip != '',
                PacketData.destination_ip != '-'
            ).distinct()
            
            # Combine and sort
            all_ips = set([ip[0] for ip in source_ips] + [ip[0] for ip in dest_ips])
            sorted_ips = sorted(list(all_ips))
            
            return True, sorted_ips
        except Exception as e:
            return False, f"Ошибка получения списка IP-адресов: {e}"
    
    def get_filtered_data(self, filters: List[Tuple[str, str, str]], 
                         limit_records: Optional[int] = None, 
                         offset: int = 0) -> Tuple[bool, Any]:
        """
        Получение отфильтрованных данных без ограничений по количеству записей
        """
        try:
            session = self.get_session()
            
            query = session.query(
                PacketData.packet_number.label('номер_пакета'),
                PacketData.timestamp.label('время'),
                PacketData.source_ip.label('исходный_ip'),
                PacketData.destination_ip.label('целевой_ip'),
                PacketData.source_port.label('исходный_порт'),
                PacketData.destination_port.label('целевой_порт'),
                PacketData.packet_size.label('размер'),
                PacketData.protocol.label('протокол')
            )
            
            # Apply filters
            for field, condition, value in filters:
                if value is not None and value != '':
                    model_field = getattr(PacketData, field)
                    
                    if condition == 'равно':
                        query = query.filter(model_field == value)
                    elif condition == 'содержит':
                        query = query.filter(model_field.ilike(f'%{value}%'))
                    elif condition == 'больше':
                        try:
                            numeric_value = float(value)
                            query = query.filter(model_field > numeric_value)
                        except ValueError:
                            # Если не число, сравниваем как строку
                            query = query.filter(model_field > value)
                    elif condition == 'меньше':
                        try:
                            numeric_value = float(value)
                            query = query.filter(model_field < numeric_value)
                        except ValueError:
                            query = query.filter(model_field < value)
                    elif condition == 'больше или равно':
                        try:
                            numeric_value = float(value)
                            query = query.filter(model_field >= numeric_value)
                        except ValueError:
                            query = query.filter(model_field >= value)
                    elif condition == 'меньше или равно':
                        try:
                            numeric_value = float(value)
                            query = query.filter(model_field <= numeric_value)
                        except ValueError:
                            query = query.filter(model_field <= value)
                    elif condition == 'не равно':
                        query = query.filter(model_field != value)
                    elif condition == 'в списке':
                        values_list = [v.strip() for v in value.split(',')]
                        query = query.filter(model_field.in_(values_list))
                    elif condition == 'между':
                        range_values = [v.strip() for v in value.split(',')]
                        if len(range_values) == 2:
                            try:
                                start_val = float(range_values[0])
                                end_val = float(range_values[1])
                                query = query.filter(model_field.between(start_val, end_val))
                            except ValueError:
                                query = query.filter(model_field.between(range_values[0], range_values[1]))
                    elif condition == 'начинается с':
                        query = query.filter(model_field.ilike(f'{value}%'))
                    elif condition == 'заканчивается на':
                        query = query.filter(model_field.ilike(f'%{value}'))
            
            # Apply ordering
            query = query.order_by(PacketData.packet_number)
            
            # ИЗМЕНЕНИЕ: Убрано ограничение по умолчанию в 1000 записей
            # Применяем лимит и оффсет только если они указаны
            if limit_records is not None:
                query = query.limit(limit_records).offset(offset)
            # Если limit_records = None, то загружаем все данные без ограничений
            
            # Execute query
            data = query.all()
            columns = ['номер_пакета', 'время', 'исходный_ip', 'целевой_ip', 
                      'исходный_порт', 'целевой_порт', 'размер', 'протокол']
            
            return True, (columns, data, str(query), [])
            
        except Exception as e:
            return False, f"Ошибка выполнения фильтрованного запроса: {e}"
    
    def get_all_data(self) -> Tuple[bool, Any]:
        """
        Получение всех данных из таблицы packet_data без ограничений
        """
        try:
            session = self.get_session()
            
            query = session.query(
                PacketData.packet_number.label('номер_пакета'),
                PacketData.timestamp.label('время'),
                PacketData.source_ip.label('исходный_ip'),
                PacketData.destination_ip.label('целевой_ip'),
                PacketData.source_port.label('исходный_порт'),
                PacketData.destination_port.label('целевой_порт'),
                PacketData.packet_size.label('размер'),
                PacketData.protocol.label('протокол')
            ).order_by(PacketData.packet_number)
            
            data = query.all()
            columns = ['номер_пакета', 'время', 'исходный_ip', 'целевой_ip', 
                      'исходный_порт', 'целевой_порт', 'размер', 'протокол']
            
            return True, (columns, data)
            
        except Exception as e:
            return False, f"Ошибка получения всех данных: {e}"
    
    def get_data_with_custom_limit(self, limit: Optional[int] = None, offset: int = 0) -> Tuple[bool, Any]:
        """
        Получение данных с пользовательским лимитом
        """
        try:
            session = self.get_session()
            
            query = session.query(
                PacketData.packet_number.label('номер_пакета'),
                PacketData.timestamp.label('время'),
                PacketData.source_ip.label('исходный_ip'),
                PacketData.destination_ip.label('целевой_ip'),
                PacketData.source_port.label('исходный_порт'),
                PacketData.destination_port.label('целевой_порт'),
                PacketData.packet_size.label('размер'),
                PacketData.protocol.label('протокол')
            ).order_by(PacketData.packet_number)
            
            if limit is not None:
                query = query.limit(limit).offset(offset)
            
            data = query.all()
            columns = ['номер_пакета', 'время', 'исходный_ip', 'целевой_ip', 
                      'исходный_порт', 'целевой_порт', 'размер', 'протокол']
            
            return True, (columns, data)
            
        except Exception as e:
            return False, f"Ошибка получения данных с лимитом: {e}"
    
    def insert_packet_data(self, packets: List[Dict], progress_callback=None) -> Tuple[bool, str]:
        try:
            session = self.get_session()
            
            # Clear existing data
            session.query(PacketData).delete()
            
            inserted_count = 0
            total_packets = len(packets)
            
            # ИЗМЕНЕНИЕ: Используем пакетную вставку для больших объемов данных
            batch_size = 1000
            for i in range(0, total_packets, batch_size):
                batch = packets[i:i + batch_size]
                
                for packet in batch:
                    if not packet.get('protocol') or packet['protocol'] == '-':
                        continue
                    
                    packet_data = PacketData(
                        packet_number=packet['number'],
                        timestamp=packet['timestamp'],
                        source_ip=packet['source_ip'],
                        destination_ip=packet['destination_ip'],
                        source_port=packet['source_port'],
                        destination_port=packet['destination_port'],
                        packet_size=packet['size'],
                        protocol=packet['protocol']
                    )
                    
                    session.add(packet_data)
                    inserted_count += 1
                
                # Коммитим пакет
                session.commit()
                
                if progress_callback:
                    progress = min((i + len(batch)) / total_packets * 100, 100)
                    progress_callback(progress, f"Загружено {i + len(batch)}/{total_packets} пакетов")
            
            # АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ СТАТИСТИКИ ПОСЛЕ ЗАГРУЗКИ ДАННЫХ
            self.auto_save_stats()
            
            return True, f"Успешно загружено {inserted_count} пакетов"
            
        except Exception as e:
            session.rollback()
            return False, f"Ошибка вставки данных пакетов: {e}"
        finally:
            session.close()
    
    def update_protocol_stats(self) -> Tuple[bool, str]:
        try:
            session = self.get_session()
            
            # Clear existing stats
            session.query(ProtocolStats).delete()
            
            # Calculate new stats
            result = session.query(
                PacketData.protocol,
                func.count(PacketData.id).label('packet_count'),
                func.sum(PacketData.packet_size).label('total_size'),
                func.avg(PacketData.packet_size).label('avg_size')
            ).filter(
                PacketData.protocol.isnot(None),
                PacketData.protocol != '',
                PacketData.protocol != '-'
            ).group_by(PacketData.protocol).all()
            
            for protocol, count, total_size, avg_size in result:
                protocol_stat = ProtocolStats(
                    protocol_name=protocol,
                    packet_count=count,
                    total_size=total_size or 0,
                    avg_size=avg_size or 0
                )
                session.add(protocol_stat)
            
            session.commit()
            count = len(result)
            
            # АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ СТАТИСТИКИ
            self.auto_save_stats()
            
            return True, f"Статистика протоколов обновлена ({count} записей)"
            
        except Exception as e:
            session.rollback()
            return False, f"Ошибка обновления статистики протоколов: {e}"
        finally:
            session.close()
    
    def update_ip_stats(self) -> Tuple[bool, str]:
        try:
            session = self.get_session()
            
            # Clear existing stats
            session.query(IPStats).delete()
            
            # Source IP stats
            source_stats = session.query(
                PacketData.source_ip,
                func.count(PacketData.id).label('packet_count'),
                func.sum(PacketData.packet_size).label('total_traffic')
            ).filter(
                PacketData.source_ip.isnot(None),
                PacketData.source_ip != '',
                PacketData.source_ip != '-'
            ).group_by(PacketData.source_ip).all()
            
            for ip, count, traffic in source_stats:
                ip_stat = IPStats(
                    ip_address=ip,
                    role='src',
                    packet_count=count,
                    total_traffic=traffic or 0
                )
                session.add(ip_stat)
            
            # Destination IP stats
            dest_stats = session.query(
                PacketData.destination_ip,
                func.count(PacketData.id).label('packet_count'),
                func.sum(PacketData.packet_size).label('total_traffic')
            ).filter(
                PacketData.destination_ip.isnot(None),
                PacketData.destination_ip != '',
                PacketData.destination_ip != '-'
            ).group_by(PacketData.destination_ip).all()
            
            for ip, count, traffic in dest_stats:
                ip_stat = IPStats(
                    ip_address=ip,
                    role='dst',
                    packet_count=count,
                    total_traffic=traffic or 0
                )
                session.add(ip_stat)
            
            session.commit()
            count = len(source_stats) + len(dest_stats)
            
            # АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ СТАТИСТИКИ
            self.auto_save_stats()
            
            return True, f"Статистика IP обновлена ({count} записей)"
            
        except Exception as e:
            session.rollback()
            return False, f"Ошибка обновления статистики IP: {e}"
        finally:
            session.close()
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None) -> Tuple[List[str], List]:
        """
        Получение данных из таблицы с возможностью ограничения
        """
        try:
            session = self.get_session()
            
            if table_name == 'packet_data':
                query = session.query(PacketData)
                if limit is not None:
                    query = query.limit(limit)
                data = query.all()
                columns = [column.name for column in PacketData.__table__.columns]
                rows = [[getattr(row, col) for col in columns] for row in data]
            elif table_name == 'protocol_stats':
                query = session.query(ProtocolStats)
                if limit is not None:
                    query = query.limit(limit)
                data = query.all()
                columns = [column.name for column in ProtocolStats.__table__.columns]
                rows = [[getattr(row, col) for col in columns] for row in data]
            elif table_name == 'ip_stats':
                query = session.query(IPStats)
                if limit is not None:
                    query = query.limit(limit)
                data = query.all()
                columns = [column.name for column in IPStats.__table__.columns]
                rows = [[getattr(row, col) for col in columns] for row in data]
            else:
                return [], []
            
            return columns, rows
            
        except Exception as e:
            print(f"Ошибка получения данных из {table_name}: {e}")
            return [], []
        finally:
            session.close()
    
    def get_table_data_all(self, table_name: str) -> Tuple[List[str], List]:
        """
        Получение ВСЕХ данных из таблицы без ограничений
        """
        return self.get_table_data(table_name, limit=None)
    
    def get_available_fields(self) -> List[str]:
        return [column.name for column in PacketData.__table__.columns]
    
    def get_data_count_by_protocol(self) -> Tuple[bool, Any]:
        """
        Получение количества записей по протоколам
        """
        try:
            session = self.get_session()
            
            result = session.query(
                PacketData.protocol,
                func.count(PacketData.id).label('count')
            ).filter(
                PacketData.protocol.isnot(None),
                PacketData.protocol != '',
                PacketData.protocol != '-'
            ).group_by(PacketData.protocol).order_by(func.count(PacketData.id).desc()).all()
            
            return True, [(protocol, count) for protocol, count in result]
        except Exception as e:
            return False, f"Ошибка получения статистики по протоколам: {e}"
    
    def get_large_data_warning(self, estimated_count: int) -> str:
        """
        Генерирует предупреждение для больших объемов данных
        """
        if estimated_count > 100000:
            return f"ВНИМАНИЕ: Запрос вернет примерно {estimated_count:,} записей. Это может занять много времени и памяти."
        elif estimated_count > 50000:
            return f"Запрос вернет примерно {estimated_count:,} записей. Рекомендуется использовать фильтры или экспорт."
        else:
            return f"Запрос вернет {estimated_count:,} записей."

    def setup_auto_stats_trigger(self) -> Tuple[bool, str]:
        """
        Установка автоматического триггера для экспорта статистики
        """
        try:
            session = self.get_session()
            
            # Создаем функцию триггера
            session.execute(text("""
                CREATE OR REPLACE FUNCTION update_stats_and_export()
                RETURNS TRIGGER AS $$
                DECLARE
                    stats_text TEXT;
                    file_path TEXT;
                    protocol_count INT;
                    ip_count INT;
                    total_packets INT;
                    stats_timestamp TIMESTAMP;
                    export_dir TEXT;
                BEGIN
                    -- Ждем небольшую задержку для завершения транзакции
                    PERFORM pg_sleep(0.1);
                    
                    -- Получаем текущее время
                    stats_timestamp := NOW();
                    
                    -- Получаем общее количество пакетов
                    SELECT COUNT(*) INTO total_packets FROM packet_data;
                    
                    -- Получаем количество уникальных протоколов
                    SELECT COUNT(DISTINCT protocol) INTO protocol_count 
                    FROM packet_data 
                    WHERE protocol IS NOT NULL AND protocol != '' AND protocol != '-';
                    
                    -- Получаем количество уникальных IP адресов
                    SELECT COUNT(DISTINCT ip) INTO ip_count FROM (
                        SELECT source_ip as ip FROM packet_data 
                        WHERE source_ip IS NOT NULL AND source_ip != '' AND source_ip != '-'
                        UNION 
                        SELECT destination_ip as ip FROM packet_data 
                        WHERE destination_ip IS NOT NULL AND destination_ip != '' AND destination_ip != '-'
                    ) AS unique_ips;
                    
                    -- Формируем текст статистики
                    stats_text := 'СТАТИСТИКА СЕТЕВЫХ ПАКЕТОВ' || E'\\n';
                    stats_text := stats_text || '================================' || E'\\n';
                    stats_text := stats_text || 'Время обновления: ' || TO_CHAR(stats_timestamp, 'YYYY-MM-DD HH24:MI:SS') || E'\\n';
                    stats_text := stats_text || 'Общее количество пакетов: ' || total_packets || E'\\n';
                    stats_text := stats_text || 'Уникальных протоколов: ' || protocol_count || E'\\n';
                    stats_text := stats_text || 'Уникальных IP-адресов: ' || ip_count || E'\\n\\n';
                    
                    -- Статистика по протоколам (топ-10)
                    stats_text := stats_text || 'ТОП-10 ПРОТОКОЛОВ:' || E'\\n';
                    stats_text := stats_text || '------------------' || E'\\n';
                    
                    WITH protocol_stats AS (
                        SELECT 
                            protocol,
                            COUNT(*) as packet_count,
                            SUM(packet_size) as total_size,
                            ROUND(AVG(packet_size)::numeric, 2) as avg_size
                        FROM packet_data 
                        WHERE protocol IS NOT NULL AND protocol != '' AND protocol != '-'
                        GROUP BY protocol
                        ORDER BY packet_count DESC
                        LIMIT 10
                    )
                    SELECT 
                        COALESCE(
                            STRING_AGG(
                                FORMAT('%-15s | %6s пакетов | %10s байт | %8s ср.размер', 
                                      protocol, packet_count::text, total_size::text, avg_size::text),
                                E'\\n'
                            ),
                            'Нет данных'
                        )
                    INTO stats_text
                    FROM (SELECT stats_text as base_text) t, protocol_stats;
                    
                    stats_text := stats_text || E'\\n\\n';
                    
                    -- Статистика по IP (топ-10 источников)
                    stats_text := stats_text || 'ТОП-10 ИСТОЧНИКОВ (по пакетам):' || E'\\n';
                    stats_text := stats_text || '-------------------------------' || E'\\n';
                    
                    WITH source_stats AS (
                        SELECT 
                            source_ip as ip,
                            COUNT(*) as packet_count,
                            SUM(packet_size) as total_traffic
                        FROM packet_data 
                        WHERE source_ip IS NOT NULL AND source_ip != '' AND source_ip != '-'
                        GROUP BY source_ip
                        ORDER BY packet_count DESC
                        LIMIT 10
                    )
                    SELECT 
                        COALESCE(
                            STRING_AGG(
                                FORMAT('%-15s | %6s пакетов | %12s байт', 
                                      ip, packet_count::text, total_traffic::text),
                                E'\\n'
                            ),
                            'Нет данных'
                        )
                    INTO stats_text
                    FROM (SELECT stats_text as base_text) t, source_stats;
                    
                    stats_text := stats_text || E'\\n\\n';
                    
                    -- Статистика по IP (топ-10 получателей)
                    stats_text := stats_text || 'ТОП-10 ПОЛУЧАТЕЛЕЙ (по пакетов):' || E'\\n';
                    stats_text := stats_text || '--------------------------------' || E'\\n';
                    
                    WITH dest_stats AS (
                        SELECT 
                            destination_ip as ip,
                            COUNT(*) as packet_count,
                            SUM(packet_size) as total_traffic
                        FROM packet_data 
                        WHERE destination_ip IS NOT NULL AND destination_ip != '' AND destination_ip != '-'
                        GROUP BY destination_ip
                        ORDER BY packet_count DESC
                        LIMIT 10
                    )
                    SELECT 
                        COALESCE(
                            STRING_AGG(
                                FORMAT('%-15s | %6s пакетов | %12s байт', 
                                      ip, packet_count::text, total_traffic::text),
                                E'\\n'
                            ),
                            'Нет данных'
                        )
                    INTO stats_text
                    FROM (SELECT stats_text as base_text) t, dest_stats;
                    
                    -- Статистика по размерам пакетов
                    stats_text := stats_text || E'\\nСТАТИСТИКА ПО РАЗМЕРАМ ПАКЕТОВ:' || E'\\n';
                    stats_text := stats_text || '-------------------------------' || E'\\n';
                    
                    WITH size_stats AS (
                        SELECT 
                            COUNT(*) as total_count,
                            SUM(packet_size) as total_bytes,
                            ROUND(AVG(packet_size)::numeric, 2) as avg_size,
                            MIN(packet_size) as min_size,
                            MAX(packet_size) as max_size
                        FROM packet_data
                        WHERE packet_size > 0
                    )
                    SELECT 
                        COALESCE(
                            FORMAT('Всего пакетов: %s\\nОбщий объем: %s байт\\nСредний размер: %s байт\\nМин. размер: %s байт\\nМакс. размер: %s байт',
                                  total_count::text, total_bytes::text, avg_size::text, min_size::text, max_size::text),
                            'Нет данных'
                        )
                    INTO stats_text
                    FROM (SELECT stats_text as base_text) t, size_stats;
                    
                    -- Определяем путь для сохранения файла
                    export_dir := 'C:\\Users\\Assa\\source\\repos\\network_stats';
                    file_path := export_dir || '\\network_stats_' || TO_CHAR(stats_timestamp, 'YYYYMMDD_HH24MISS') || '.txt';
                    
                    -- Сохраняем в файл (если есть права)
                    BEGIN
                        -- Создаем директорию если не существует
                        PERFORM dblink_exec('dbname=' || current_database(), 
                                          'CREATE DIRECTORY IF NOT EXISTS ' || quote_ident(export_dir));
                        
                        -- Сохраняем статистику
                        PERFORM pg_catalog.pg_write_file(file_path, stats_text, false);
                        RAISE NOTICE 'Статистика автоматически сохранена в файл: %', file_path;
                    EXCEPTION
                        WHEN OTHERS THEN
                            -- Если не удалось сохранить в файл, просто логируем
                            RAISE NOTICE 'Не удалось сохранить статистику в файл. Статистика собрана:%', E'\\n' || stats_text;
                    END;
                    
                    -- Также обновляем таблицы статистики в БД
                    DELETE FROM protocol_stats;
                    
                    INSERT INTO protocol_stats (protocol_name, packet_count, total_size, avg_size, created_at)
                    SELECT 
                        protocol,
                        COUNT(*) as packet_count,
                        SUM(packet_size) as total_size,
                        AVG(packet_size) as avg_size,
                        stats_timestamp
                    FROM packet_data 
                    WHERE protocol IS NOT NULL AND protocol != '' AND protocol != '-'
                    GROUP BY protocol;
                    
                    -- Обновляем IP статистику
                    DELETE FROM ip_stats;
                    
                    -- Источники
                    INSERT INTO ip_stats (ip_address, role, packet_count, total_traffic, created_at)
                    SELECT 
                        source_ip,
                        'src',
                        COUNT(*) as packet_count,
                        SUM(packet_size) as total_traffic,
                        stats_timestamp
                    FROM packet_data 
                    WHERE source_ip IS NOT NULL AND source_ip != '' AND source_ip != '-'
                    GROUP BY source_ip;
                    
                    -- Получатели
                    INSERT INTO ip_stats (ip_address, role, packet_count, total_traffic, created_at)
                    SELECT 
                        destination_ip,
                        'dst',
                        COUNT(*) as packet_count,
                        SUM(packet_size) as total_traffic,
                        stats_timestamp
                    FROM packet_data 
                    WHERE destination_ip IS NOT NULL AND destination_ip != '' AND destination_ip != '-'
                    GROUP BY destination_ip;
                    
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """))
            
            # Создаем триггер
            session.execute(text("""
                DROP TRIGGER IF EXISTS auto_update_stats_trigger ON packet_data;
                CREATE TRIGGER auto_update_stats_trigger
                    AFTER INSERT ON packet_data
                    FOR EACH STATEMENT
                    EXECUTE FUNCTION update_stats_and_export();
            """))
            
            session.commit()
            return True, "Автоматический триггер статистики установлен. Статистика будет сохраняться автоматически в C:\\Users\\Assa\\source\\repos\\network_stats"
            
        except Exception as e:
            session.rollback()
            return False, f"Ошибка установки триггера: {e}"
        finally:
            session.close()

    def remove_auto_stats_trigger(self) -> Tuple[bool, str]:
        """
        Удаление автоматического триггера статистики
        """
        try:
            session = self.get_session()
            
            session.execute(text("""
                DROP TRIGGER IF EXISTS auto_update_stats_trigger ON packet_data;
                DROP FUNCTION IF EXISTS update_stats_and_export();
            """))
            
            session.commit()
            return True, "Автоматический триггер статистики удален"
            
        except Exception as e:
            session.rollback()
            return False, f"Ошибка удаления триггера: {e}"
        finally:
            session.close()

    def export_stats_to_txt(self, export_dir: str = None) -> Tuple[bool, str]:
        
        try:
            import os
            from datetime import datetime
            
            # По умолчанию сохраняем в C:\Users\Assa\source\repos\network_stats
            if export_dir is None:
                export_dir = self.stats_dir
            
            # Создаем директорию если не существует
            os.makedirs(export_dir, exist_ok=True)
            
            session = self.get_session()
            
            # Общее количество пакетов
            total_packets = session.query(func.count(PacketData.id)).scalar() or 0
            
            # Если нет данных, не создаем файл
            if total_packets == 0:
                return True, "Нет данных для экспорта статистики"
            
            # Статистика по протоколам
            protocol_stats = session.query(
                PacketData.protocol,
                func.count(PacketData.id).label('count'),
                func.sum(PacketData.packet_size).label('total_size'),
                func.avg(PacketData.packet_size).label('avg_size')
            ).filter(
                PacketData.protocol.isnot(None),
                PacketData.protocol != '',
                PacketData.protocol != '-'
            ).group_by(PacketData.protocol).order_by(func.count(PacketData.id).desc()).limit(10).all()
            
            # Статистика по IP источникам
            source_ip_stats = session.query(
                PacketData.source_ip,
                func.count(PacketData.id).label('count'),
                func.sum(PacketData.packet_size).label('total_traffic')
            ).filter(
                PacketData.source_ip.isnot(None),
                PacketData.source_ip != '',
                PacketData.source_ip != '-'
            ).group_by(PacketData.source_ip).order_by(func.count(PacketData.id).desc()).limit(10).all()
            
            # Статистика по IP получателям
            dest_ip_stats = session.query(
                PacketData.destination_ip,
                func.count(PacketData.id).label('count'),
                func.sum(PacketData.packet_size).label('total_traffic')
            ).filter(
                PacketData.destination_ip.isnot(None),
                PacketData.destination_ip != '',
                PacketData.destination_ip != '-'
            ).group_by(PacketData.destination_ip).order_by(func.count(PacketData.id).desc()).limit(10).all()
            
            # Статистика по размерам пакетов
            size_stats = session.query(
                func.count(PacketData.id).label('total_count'),
                func.sum(PacketData.packet_size).label('total_bytes'),
                func.avg(PacketData.packet_size).label('avg_size'),
                func.min(PacketData.packet_size).label('min_size'),
                func.max(PacketData.packet_size).label('max_size')
            ).filter(PacketData.packet_size > 0).first()
            
            # Формируем текст статистики
            stats_text = "СТАТИСТИКА СЕТЕВЫХ ПАКЕТОВ\n"
            stats_text += "=" * 50 + "\n"
            stats_text += f"Время генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            stats_text += f"Общее количество пакетов: {total_packets}\n"
            
            if size_stats:
                total_count, total_bytes, avg_size, min_size, max_size = size_stats
                stats_text += f"Общий объем трафика: {total_bytes or 0:,} байт\n"
                stats_text += f"Средний размер пакета: {avg_size or 0:.2f} байт\n"
                stats_text += f"Минимальный размер: {min_size or 0} байт\n"
                stats_text += f"Максимальный размер: {max_size or 0} байт\n"
            
            stats_text += "\n" + "ТОП-10 ПРОТОКОЛОВ:\n"
            stats_text += "-" * 60 + "\n"
            if protocol_stats:
                for protocol, count, total_size, avg_size in protocol_stats:
                    stats_text += f"{protocol:<20} | {count:>8} пакетов | {total_size or 0:>12} байт | {avg_size or 0:>8.2f} ср.размер\n"
            else:
                stats_text += "Нет данных\n"
            
            stats_text += "\n" + "ТОП-10 ИСТОЧНИКОВ:\n"
            stats_text += "-" * 50 + "\n"
            if source_ip_stats:
                for ip, count, traffic in source_ip_stats:
                    stats_text += f"{ip:<20} | {count:>8} пакетов | {traffic or 0:>12} байт\n"
            else:
                stats_text += "Нет данных\n"
            
            stats_text += "\n" + "ТОП-10 ПОЛУЧАТЕЛЕЙ:\n"
            stats_text += "-" * 50 + "\n"
            if dest_ip_stats:
                for ip, count, traffic in dest_ip_stats:
                    stats_text += f"{ip:<20} | {count:>8} пакетов | {traffic or 0:>12} байт\n"
            else:
                stats_text += "Нет данных\n"
            
            # Сохраняем в файл
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"network_stats_{timestamp}.txt"
            filepath = os.path.join(export_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(stats_text)
            
            return True, f"Статистика экспортирована в: {filepath}"
            
        except Exception as e:
            return False, f"Ошибка экспорта статистики: {e}"
        finally:
            session.close()

    def get_stats_summary(self) -> dict:
        """
        Получение краткой статистики для отображения в UI
        """
        try:
            session = self.get_session()
            
            # Основные метрики
            total_packets = session.query(func.count(PacketData.id)).scalar() or 0
            unique_protocols = session.query(PacketData.protocol).filter(
                PacketData.protocol.isnot(None),
                PacketData.protocol != '',
                PacketData.protocol != '-'
            ).distinct().count()
            
            unique_ips = session.query(PacketData.source_ip).filter(
                PacketData.source_ip.isnot(None),
                PacketData.source_ip != '',
                PacketData.source_ip != '-'
            ).distinct().count()
            
            total_traffic = session.query(func.sum(PacketData.packet_size)).scalar() or 0
            
            # Топ протоколов
            top_protocols = session.query(
                PacketData.protocol,
                func.count(PacketData.id).label('count')
            ).filter(
                PacketData.protocol.isnot(None),
                PacketData.protocol != '',
                PacketData.protocol != '-'
            ).group_by(PacketData.protocol).order_by(func.count(PacketData.id).desc()).limit(5).all()
            
            from datetime import datetime
            return {
                'total_packets': total_packets,
                'unique_protocols': unique_protocols,
                'unique_ips': unique_ips,
                'total_traffic': total_traffic,
                'top_protocols': [{'protocol': p[0], 'count': p[1]} for p in top_protocols],
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            return {'error': str(e)}
        finally:
            session.close()

    def create_sql_backup_manual(self, backup_path: str = None) -> Tuple[bool, str]:
       
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = os.path.join(self.backup_dir, f"network_monitor_backup_{timestamp}.sql")
            else:
                # Убеждаемся, что файл сохраняется в нужную папку
                if not backup_path.startswith(self.backup_dir):
                    backup_path = os.path.join(self.backup_dir, os.path.basename(backup_path))
            
            # Убеждаемся, что это .sql файл
            if not backup_path.endswith('.sql'):
                backup_path += '.sql'
            
            session = self.get_session()
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                # Записываем заголовок
                f.write("-- Резервная копия базы данных Network Monitor\n")
                f.write(f"-- Создана: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"-- База данных: network_monitor\n")
                f.write("SET client_encoding = 'UTF8';\n\n")
                
                # 1. Резервное копирование таблицы packet_data
                self._backup_table_data(session, f, 'packet_data', [
                    'id', 'packet_number', 'timestamp', 'source_ip', 'destination_ip',
                    'source_port', 'destination_port', 'packet_size', 'protocol', 'created_at'
                ])
                
                # 2. Резервное копирование таблицы protocol_stats
                self._backup_table_data(session, f, 'protocol_stats', [
                    'id', 'protocol_name', 'packet_count', 'total_size', 'avg_size', 'created_at'
                ])
                
                # 3. Резервное копирование таблицы ip_stats
                self._backup_table_data(session, f, 'ip_stats', [
                    'id', 'ip_address', 'role', 'packet_count', 'total_traffic', 'created_at'
                ])
                
                f.write("\n-- Резервное копирование завершено успешно\n")
                f.write(f"-- Всего таблиц: 3\n")
                f.write(f"-- Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            file_size = os.path.getsize(backup_path)
            file_size_mb = file_size / (1024 * 1024)
            
            return True, f"SQL резервная копия создана: {backup_path} ({file_size_mb:.2f} MB)"
            
        except Exception as e:
            return False, f"Ошибка создания резервной копии: {str(e)}"
    
    def _backup_table_data(self, session, file_handle, table_name: str, columns: List[str]):
        """
        Резервное копирование данных конкретной таблицы
        """
        try:
            file_handle.write(f"\n-- Резервное копирование таблицы {table_name}\n")
            file_handle.write(f"DELETE FROM {table_name};\n\n")
            
            # Получаем данные из таблицы
            result = session.execute(text(f"SELECT {', '.join(columns)} FROM {table_name}"))
            rows = result.fetchall()
            
            if not rows:
                file_handle.write(f"-- Таблица {table_name} пуста\n\n")
                return
            
            batch_size = 100  # Вставляем пачками по 100 записей
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"
                
                values = []
                for row in batch:
                    row_values = []
                    for value in row:
                        if value is None:
                            row_values.append("NULL")
                        elif isinstance(value, (int, float)):
                            row_values.append(str(value))
                        elif isinstance(value, datetime):
                            row_values.append(f"'{value.isoformat()}'")
                        else:
                            # Экранируем специальные символы для SQL
                            escaped_value = str(value).replace("'", "''")
                            row_values.append(f"'{escaped_value}'")
                    
                    values.append(f"({', '.join(row_values)})")
                
                insert_sql += ",\n".join(values) + ";\n"
                file_handle.write(insert_sql)
            
            file_handle.write(f"\n-- Всего записей в {table_name}: {len(rows)}\n\n")
            
        except Exception as e:
            file_handle.write(f"-- Ошибка резервного копирования таблицы {table_name}: {str(e)}\n\n")
    
    def auto_create_backup(self) -> Tuple[bool, str]:
        """
        Автоматическое создание резервной копии (вызывается по расписанию)
        """
        try:
            print(f"Запуск автоматического резервного копирования: {datetime.now()}")
            
            # Создаем SQL бэкап
            success, message = self.create_sql_backup_manual()
            
            if success:
                return True, f"Автоматическое резервное копирование завершено. {message}"
            else:
                return False, f"Ошибка автоматического резервного копирования: {message}"
                
        except Exception as e:
            return False, f"Исключение при автоматическом резервном копировании: {str(e)}"
    
    def get_backup_history(self) -> List[Dict[str, Any]]:
        """
        Получение истории резервных копий
        """
        try:
            if not os.path.exists(self.backup_dir):
                return []
            
            backup_files = []
            for filename in os.listdir(self.backup_dir):
                if filename.endswith('.sql'):
                    file_path = os.path.join(self.backup_dir, filename)
                    file_stat = os.stat(file_path)
                    
                    backup_files.append({
                        'filename': filename,
                        'path': file_path,
                        'size': file_stat.st_size,
                        'created_time': datetime.fromtimestamp(file_stat.st_ctime),
                        'type': 'SQL'
                    })
            
            # Сортируем по дате создания (новые сначала)
            backup_files.sort(key=lambda x: x['created_time'], reverse=True)
            return backup_files
            
        except Exception as e:
            print(f"Ошибка получения истории бэкапов: {e}")
            return []
    
    def restore_from_sql_backup(self, backup_path: str) -> Tuple[bool, str]:
        """
        Восстановление из SQL резервной копии
        """
        try:
            if not os.path.exists(backup_path):
                return False, "Файл резервной копии не существует"
            
            session = self.get_session()
            
            # Читаем SQL файл
            with open(backup_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Разделяем на отдельные SQL команды
            sql_commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
            
            # Выполняем команды
            for command in sql_commands:
                if command and not command.startswith('--'):
                    try:
                        session.execute(text(command))
                    except Exception as e:
                        print(f"Ошибка выполнения команды: {command[:100]}... - {e}")
            
            session.commit()
            return True, f"База данных восстановлена из резервной копии: {backup_path}"
            
        except Exception as e:
            session.rollback()
            return False, f"Ошибка восстановления из резервной копии: {str(e)}"
        finally:
            session.close()