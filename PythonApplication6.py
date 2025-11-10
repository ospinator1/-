import tkinter as tk
from gui.main_window import NetworkMonitorGUI

def main():
    try:
        import psycopg2
        import pandas as pd
        import openpyxl
        from sqlalchemy import create_engine
    except ImportError as e:
        print(f"Ошибка: Не установлены необходимые библиотеки: {e}")
        print("Установите их командами:")
        print("pip install psycopg2-binary pandas openpyxl sqlalchemy")
        return
    
    root = tk.Tk()
    app = NetworkMonitorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()