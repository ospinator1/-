from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import StaticPool
import os
import getpass
import tkinter as tk
from tkinter import simpledialog
Base = declarative_base()

class DatabaseManager:
    def __init__(self, host='localhost', database='network_monitor', 
                 user='postgres', password=None, port=5432):
        # Получаем пароль из переменных окружения или запрашиваем у пользователя
        if password is None:
            password = self.get_password()
        
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = None
        self.SessionLocal = None
        self.current_session = None
    
    def get_password(self):
        """
        Получает пароль из переменных окружения или запрашивает у пользователя
        """
        # Пытаемся получить пароль из переменных окружения
        env_password = os.getenv('DB_PASSWORD')
        if env_password:
            return env_password
        
        # Если пароль не найден в переменных окружения, запрашиваем у пользователя
        try:
            
            
            root = tk.Tk()
            root.withdraw()  # Скрываем основное окно
            
            password = simpledialog.askstring(
                "Пароль базы данных", 
                "Введите пароль для подключения к PostgreSQL:",
                show='*'
            )
            root.destroy()
            
            if password:
                return password
            else:
                raise ValueError("Пароль не введен")
                
        except (ImportError, Exception):
            # Если графический интерфейс недоступен, используем консольный ввод
            print("Подключение к базе данных PostgreSQL")
            return getpass.getpass("Введите пароль базы данных: ")
    
    def connect(self):
        try:
            self.engine = create_engine(
                self.connection_string,
                poolclass=StaticPool,
                echo=False
            )
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            self.current_session = self.SessionLocal()
         
            with self.engine.connect() as conn:
                pass
                
            return True, "Успешное подключение к базе данных"
        except Exception as e:
            error_msg = str(e)
            if "password authentication failed" in error_msg.lower():
                return False, "Ошибка аутентификации: неверный пароль"
            elif "connection refused" in error_msg.lower():
                return False, "Ошибка подключения: сервер базы данных недоступен"
            else:
                return False, f"Ошибка подключения: {e}"
    
    def disconnect(self):
        if self.current_session:
            self.current_session.close()
        if self.engine:
            self.engine.dispose()
    
    def create_tables(self):
        try:
            Base.metadata.create_all(bind=self.engine)
            return True, "Таблицы успешно созданы"
        except Exception as e:
            return False, f"Ошибка создания таблиц: {e}"
    
    def drop_tables(self):
        try:
            Base.metadata.drop_all(bind=self.engine)
            return True, "Таблицы успешно удалены"
        except Exception as e:
            return False, f"Ошибка удаления таблиц: {e}"
    
    def get_session(self):
        if not self.current_session:
            self.connect()
        return self.current_session
