import sqlite3
from sqlite3 import Error

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except Error as e:
       print(e)

   return conn

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_employees(conn, employees):
   """
   Create a new employee into the table
   :param conn:
   :param employee:
   :return: employee id
   """
   sql = '''INSERT INTO employees(employee_id, name, surname, status, date_of_employment, date_of_dismissal)
             VALUES(?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, employees)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   """
   update status, date of employment and date of dismissal
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)
       
def delete_where(conn, table, **kwargs):
   """
   Delete from table where attributes from
   :param conn:  Connection to the SQLite database
   :param table: table name
   :param kwargs: dict of attributes and values
   :return:
   """
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

if __name__ == '__main__':
   create_employees_sql = """
   -- employees table
   CREATE TABLE IF NOT EXISTS employees (
      id integer PRIMARY KEY, 
      employee_id integer NOT NULL,
      name text NOT NULL,
      surname text NOT NULL,
      status VARCHAR(55) NOT NULL,
      date_of_employment text NOT NULL,
      date_of_dismissal text NOT NULL
   );
   """
   db_file = "database.db"

   conn = create_connection(db_file)
   if conn is not None:
       execute_sql(conn, create_employees_sql)
       conn.close()
   
   employees = [(1, "Ігор", "Добуш", "працює", "02.02.2015", ""),
                (2, "Юлія", "Гнилюх", "працює", "07.10.2016", ""),
                (3, "Володимир", "Мандюк", "звільнений", "28.11.2016", "01.06.2022"),
                (4, "Борис", "Хонько", "працює", "21.03.2018", ""),
                (5, "Михайло", "Глєбов", "звільнений", "26.10.2020", "24.05.2022")]
   conn = create_connection("database.db")
   for employee in employees:
    add_employees(conn, employee)
    conn.commit()
   
   
   select_all(conn, "employees")
   select_where(conn, "employees", employee_id=1)
   
   update(conn, "employees", 3, date_of_dismissal="24.05.2022")
      
   delete_where(conn, "employees", employee_id=4)
   

   