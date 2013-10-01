import logging
import ConfigParser
import MySQLdb

CONFIG_PATH = 'db_config.conf'

def get_connection_kwargs(config_section_name):
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_PATH)
    db_host = config.get(config_section_name, 'db_host')
    db_user = config.get(config_section_name, 'db_user')
    db_passwd = config.get(config_section_name, 'db_passwd')
    db_name = config.get(config_section_name, 'db_name')
    return {'db_host': db_host,
            'db_user': db_user,
            'db_passwd': db_passwd,
            'db_name': db_name}


class dbconn:
    def __init__(self, db_host, db_user, db_passwd, db_name):
        self.__host = db_host
        self.__user = db_user
        self.__passwd = db_passwd
        self.__name = db_name

    def __call__(self, func):
        def wrapper_func(*args, **kwargs):
            db_connect = MySQLdb.Connect(self.__host, self.__user, self.__passwd, self.__name)
            cursor = db_connect.cursor()
            try:
                return func(cursor, *args, **kwargs)
            finally:
                cursor.close()
                db_connect.close()
        return wrapper_func

conn_kwargs = get_connection_kwargs('db_connection4')

@dbconn(**conn_kwargs)
def select_by_id(cursor):
    request = "SELECT id FROM table1 WHERE data='asd'"
    cursor.execute(request)
    return cursor.fetchall()


@dbconn(**conn_kwargs)
def select_all(cursor):
    request = "SELECT * FROM table1"
    cursor.execute(request)
    return cursor.fetchall()

conn_kwargs = get_connection_kwargs('db_connection3')

@dbconn(**conn_kwargs)
def put(cursor):
    request = "INSERT INTO table1 (data) VALUES ('hostiaka')"
    cursor.execute(request)

###############
##Usage example
###############

def main():
    print select_by_id()
    print select_all()
    put()

if __name__ == '__main__':
    main()