# init_db.py
from database.db_manager import DatabaseManager

if __name__ == "__main__":
    db = DatabaseManager() 
    db.close()
    print("База данных инициализирована")