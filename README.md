## Задание 1
### Инструкции по запуску
1. Создать пользователя и БД в MySQL:
	- Открыть MySQL Command Line Client
	- Создать нового пользователя: `CREATE USER my_db_user;`
	- Создать новую БД: `CREATE DATABASE my_db;`
	- Предоставить пользователю привилегии на БД: `GRANT ALL PRIVILEGES ON my_db.* TO my_db_user;`
	- Обновить привилегии: `FLUSH PRIVILEGES;`
2. Установить необходимые python пакеты:
	2.1 Установка через командную строку:
		- Открыть командную строку
		- Перейти в папку с проектом
		- Запустить команду `pip install -r requirements.txt`
	2.2 Установка через Anaconda:
		- Запустить Anaconda Navigator
		- Перейти на вкладку "Environments"
		- В поле "Search Packages" вбить и установить поочередно пакеты `mysql-connector`, `openpyxl`, `pandas`
3. Запустить сам скрипт:
	- Открыть командную строку
	- Перейти в папку с проектом
	- Запустить `python task1.py`