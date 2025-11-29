import re
from typing import List, Dict, Optional, Callable

class FileParser:
    @staticmethod
    def parse_log_file(file_path: str, progress_callback: Optional[Callable] = None) -> List[Dict]:
        packets = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            print(f"Начало парсинга файла: {file_path}")
            
            lines = content.split('\n')
            packet_lines = []
            
            # Ищем все строки с пакетами
            for i, line in enumerate(lines):
                if '║' in line and '│' in line:
                    # Проверяем, что это строка с данными пакета
                    if re.search(r'║\s*\d+\s*│', line):
                        packet_lines.append((i, line))
            
            total_lines = len(packet_lines)
            print(f"Найдено строк с пакетами: {total_lines}")
            
            for idx, (line_num, line) in enumerate(packet_lines):
                clean_line = line.replace('║', '|').strip('|')
                parts = [part.strip() for part in clean_line.split('│')]
                
                if len(parts) >= 9:
                    packet = {
                        'number': FileParser.clean_number(parts[0]),
                        'timestamp': FileParser.clean_text(parts[1]),
                        'source_ip': FileParser.clean_text(parts[2]),
                        'destination_ip': FileParser.clean_text(parts[3]),
                        'source_port': FileParser.parse_port(parts[4]),
                        'destination_port': FileParser.parse_port(parts[5]),
                        'size': FileParser.clean_number(parts[6]),
                        'flags': FileParser.clean_text(parts[7]),
                        'protocol': FileParser.clean_text(parts[8])
                    }
                    packets.append(packet)
                
                if progress_callback and idx % 10 == 0:
                    progress = (idx + 1) / total_lines * 100
                    progress_callback(progress, f"Обработано {idx + 1}/{total_lines} пакетов")
            
            if progress_callback:
                progress_callback(100, f"Парсинг завершен. Обработано {len(packets)} пакетов")
            
            if packets:
                protocols = set(p['protocol'] for p in packets if p['protocol'])
                print(f"Найдено протоколов: {protocols}")
                print(f"Всего пакетов: {len(packets)}")
                print("Первые 5 пакетов:")
                for i, p in enumerate(packets[:5]):
                    print(f"  {i+1}. №{p['number']} | {p['source_ip']} -> {p['destination_ip']} | Протокол: {p['protocol']}")
                print("Последние 5 пакетов:")
                for i, p in enumerate(packets[-5:]):
                    print(f"  {i+1}. №{p['number']} | {p['source_ip']} -> {p['destination_ip']} | Протокол: {p['protocol']}")
            
            return packets
            
        except Exception as e:
            print(f"Ошибка парсинга файла: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ''
        cleaned = re.sub(r'[║│\-\s]', ' ', text).strip()
        return cleaned if cleaned else ''
    
    @staticmethod
    def clean_number(text: str) -> int:
        if not text or text == '-':
            return 0
        cleaned = re.sub(r'[^\d]', '', text)
        return int(cleaned) if cleaned else 0
    
    @staticmethod
    def parse_port(port_text: str) -> int:
        if not port_text:
            return 0
        cleaned = FileParser.clean_text(port_text)
        if not cleaned or cleaned == '-':
            return 0
        try:
            return int(cleaned)
        except:
            return 0