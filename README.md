# MySQLORM
strange mysql orm on python

Как выглядить файл connopt.py, который импортит ОРМ:
```python
import pymysql

def get_c(login = "login", password = "password", db_name = "db_name"):
    connection = pymysql.connect(host='localhost', user=login,
                                 password=password, db=db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection 
```
