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



class ApiDbException(Exception):
    def __init__(self, message):
        self.message = str(message)
    def __str__(self):
        return self.message


class ApiDbDepartmentError(ApiDbException):
    def __init__(self, val):
        self.message = 'DepartmentError: %s' % (val,)
    def __str__(self):
        return self.message

class ApiDbEployeeError(ApiDbException):
    def __init__(self, val):
        self.message = 'EployeeError: %s' % (val,)
    def __str__(self):
        return self.message

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
                cursor.execute('START TRANSACTION')
                try:
                    return func(cursor, *args, **kwargs)
                except (MySQLdb.ProgrammingError, MySQLdb.OperationalError), e:
                    cursor.execute('ROLLBACK')
                    raise ApiDbException(e)
                except ApiDbEployeeError, e:
                    print e
                except ApiDbDepartmentError, e:
                    print e
            finally:
                cursor.execute('COMMIT')
                cursor.close()
                db_connect.close()
        wrapper_func.__doc__ = func.__doc__
        return wrapper_func

conn_kwargs = get_connection_kwargs('db_connection1')


@dbconn(**conn_kwargs)
def put_employee(cursor, first_name, last_name, dept_name):
    """Put new employee

    :Parameters:
        - `firs_name`: first name employee
        - `last_name`: last name employee
        - `dept_name`: department name
    """

    get_epml = 'SELECT employee_id FROM employees WHERE first_name=%s AND last_name=%s'
    cursor.execute(get_epml, (first_name, last_name,))
    empl_id = cursor.fetchone()
    if empl_id is None:
        get_dept_id = 'SELECT department_id FROM departments WHERE department=%s'
        cursor.execute(get_dept_id, (dept_name,))
        dept_id = cursor.fetchone()
        if dept_id is None:
            raise ApiDbDepartmentError('Department `%s` not exist' % (dept_name,))
        put_empl = """INSERT INTO employees (first_name, last_name, department_id) VALUES (%s, %s, %s)"""
        cursor.execute(put_empl, (first_name, last_name, dept_id[0],))
    else:
        raise ApiDbEployeeError('Employee `%s, %s` already exist' % (first_name, last_name,))


@dbconn(**conn_kwargs)
def del_employee(cursor, first_name, last_name):
    """Delete employee by first name and last name

    :Parameters:
        - `firs_name`: first name employee
        - `last_name`: last name employee
    """

    del_empl = 'DELETE FROM employees WHERE first_name=%s AND last_name=%s'
    cursor.execute(del_empl, (first_name, last_name,))


@dbconn(**conn_kwargs)
def put_departments(cursor, dept_name):
    """Put new departament

    :Parameters:
        - `dept_name`: department name
    """

    get_dept_id = 'SELECT department_id FROM departments WHERE department=%s'
    cursor.execute(get_dept_id, (dept_name,))
    dept_id = cursor.fetchone()
    if dept_id is None:
        put_dept = 'INSERT INTO departments (department) VALUES (%s)'
        cursor.execute(put_dept, (dept_name,))
    else:
        raise ApiDbDepartmentError('Department `%s` already exist' % (dept_name,))


@dbconn(**conn_kwargs)
def del_departments(cursor, dept_name):
    """Delete departament by name from database

    :Parameters:
        - `dept_name`: department name
    """

    del_dept = 'DELETE FROM departments WHERE department=%s'
    cursor.execute(del_dept, (dept_name,))

@dbconn(**conn_kwargs)
def get_all_employees(cursor):
    """Get all employees in a department

    :Return:
        [(id, first_name, last_name, department), ...]
    """
    get_all_empl = """SELECT empl.employee_id, empl.first_name, empl.last_name, dept.department
                        FROM employees AS empl
                        INNER JOIN departments AS dept
                        WHERE empl.department_id=dept.department_id"""
    cursor.execute(get_all_empl)
    return_data = cursor.fetchall()
    return return_data


@dbconn(**conn_kwargs)
def get_all_employees_by_dept(cursor, dept_name):
    """Get all employees in a department

    :Parameters:
        - `dept_name`: department name

    :Return:
        [(id, first_name, last_name), ...]
    """

    get_empl_by_dept = """SELECT employee_id, first_name, last_name
                        FROM employees AS empl
                        INNER JOIN departments AS dept
                        ON (empl.department_id=dept.department_id)
                        WHERE dept.department=%s"""
    cursor.execute(get_empl_by_dept, (dept_name,))
    return_data = cursor.fetchall()
    return return_data


@dbconn(**conn_kwargs)
def get_all_departments(cursor):
    """Get all departments

    :Return:
        [dept_1, dept_2, ...]
    """

    get_all_depts = 'SELECT * FROM departments'
    cursor.execute(get_all_depts)
    return_data = cursor.fetchall()
    return return_data


@dbconn(**conn_kwargs)
def get_sum_salary_by_employee_id(cursor, empl_id):
    """Get sum of salary by employee

    :Parameters:
        - `empl_id`: employee id in database

    :Return:
        <int>
    """

    get_sal_by_empl = 'SELECT SUM(income) FROM salary WHERE employee_id=%s'
    cursor.execute(get_sal_by_empl, (empl_id,))
    (return_data,) = cursor.fetchone()
    return return_data


@dbconn(**conn_kwargs)
def get_sum_salary_by_employee(cursor, first_name, last_name):
    """Get sum of salary by employee

    :Parameters:
        - `empl_id`: employee id in database

    :Return:
        <int>
    """
    get_sal_by_empl = """SELECT sum(income) FROM salary AS sal
                    INNER JOIN employees AS empl
                    ON (sal.employee_id=empl.employee_id)
                    WHERE empl.first_name=%s AND empl.last_name=%s"""
    cursor.execute(get_sal_by_empl, (first_name, last_name,))
    (return_data,) = cursor.fetchone()
    return return_data


@dbconn(**conn_kwargs)
def get_sum_salary_by_dept(cursor, dept_name):
    """Get sum of salary by department

    :Parameters:
        - `dept_name`: department name

    :Return:
        <int>
    """

    get_sal_by_dept = """SELECT sum(income) FROM salary AS sal
                    INNER JOIN employees AS empl
                    ON (sal.employee_id=empl.employee_id)
                    INNER JOIN departments AS dept
                    ON (dept.department_id=empl.department_id)
                    WHERE dept.department=%s"""
    cursor.execute(get_sal_by_dept, (dept_name,))
    (return_data,) = cursor.fetchone()
    return return_data


###############
##Usage example
###############

def main():
    put_employee('FName', 'LName', 'Exp1')
    put_departments('Star Craft')
    print get_all_employees()
    print get_all_departments()
    print get_all_employees_by_dept('Exp1')
    print get_sum_salary_by_employee_id(27)
    print get_sum_salary_by_employee('Karlos', 'Santana')
    print get_sum_salary_by_dept('Exp1')

if __name__ == '__main__':
    main()