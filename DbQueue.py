import cPickle
import logging
import MySQLdb
import time


class DBQueueException(Exception):

    """DBQueueException, raised all exception"""

asdsad    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message


class DBQueueEmpty(DBQueueException):

    """DBQueueEmpty, raised exception if DBQueue is Empty"""

    def __init__(self, val):
        self.message = 'Queue in table `%s` is empty' % (val,)
    def __str__(self):
        return self.message


class DBQueueFull(DBQueueException):

    """DBQueueFull, raised exception if DBQueue is Full"""

    def __init__(self, val):
        self.message = 'Queue in table `%s` is full' % (val,)
    def __str__(self):
        return self.message


class DBQueue:
    def __init__(self, conn, table_name, queue_size=5000, log=None):

        """Initialize a queue object.

        :Parameters:
            - `conn`: MySQLdb connection object.
            - `table_name`: table name.
            - `queue_size`: maximum size of queue
            - `log`: take logger
        """

        if log:
            self.logger = log
        else:
            self.logger = logging.getLogger()

        self.__conn = conn
        self.__table_name = table_name
        self.__sleep_time = 0.2
        self.__max_queue_size = queue_size
        self.__table_exists_request = "SHOW TABLES LIKE %s"
        self.__fields_type_request = 'SHOW FIELDS FROM %s' % (self.__table_name,)
        self.__create_table_request = """CREATE TABLE IF NOT EXISTS %s
                                (id INT NOT NULL AUTO_INCREMENT, data BLOB,
                                PRIMARY KEY (id))""" % (self.__table_name,)
        self.__get_data_request = """SELECT id, data FROM %s ORDER BY id
                                ASC LIMIT 1 FOR UPDATE""" % (self.__table_name,)
        self.__get_qsize_request = 'SELECT COUNT(id) FROM %s' % (self.__table_name,)
        self.__create_table()


    def __create_table(self):

        """Create table in MySQL database.

        :Exceptions:
            - `DBQueueException`: Raise MySQLdb.ProgrammingError
            or MySQLdb.OperationalError or if Table not correspond to a given scheme"""

        # Use DictCursor for get from cursor.fetchall a dictionary
        cursor = self.__conn.cursor(MySQLdb.cursors.DictCursor)
        try:
            try:
                self.logger.debug('Try create table if not exists')
                cursor.execute(self.__table_exists_request, (self.__table_name,))
                (table_exists,) = cursor.fetchone()
                cursor.execute(self.__fields_type_request)
                fileds_type = cursor.fetchall()
                if not table_exists:
                    print 1
                    cursor.execute(self.__create_table_request)
                    self.logger.debug('Table has been created')
                else:
                    for i in fileds_type:
                        # Check a Type for fields 'id' and 'data'
                        if i['Field'] == 'id' and i['Type'] != 'int(11)':
                            raise DBQueueException('Table not correspond to a given scheme')
                        if i['Field'] == 'data' and i['Type'] != 'blob':
                            raise DBQueueException('Table not correspond to a given scheme')
                    self.logger.debug('Table %s already exist', self.__table_name)
            except (MySQLdb.ProgrammingError, MySQLdb.OperationalError), e:
                # Raise and log exception without error code
                self.logger.error('Create table Error: %s' % (e[1],))
                raise DBQueueException('Create table Error: %s' % (e[1],))
        finally:
            cursor.close()

    def qsize(self):

        """Return the size of the queue

        :Exceptions:
            - `DBQueueException`: Raise MySQLdb.OperationalError"""

        cursor = self.__conn.cursor()
        try:
            try:
                cursor.execute(self.__get_qsize_request)
                (db_qsize,) = cursor.fetchone()
            except (MySQLdb.ProgrammingError, MySQLdb.OperationalError), e:
                self.logger.error('Can`t get queue size: %s', e[1])
                raise DBQueueException('Can`t select from %s' % (self.__table_name,))
        finally:
            cursor.close()

        return db_qsize

    def put(self, item, block=True, timeout=None):

        """Putting item to queue. Use cPickle for putting data

        :Parameters:
            - `item`: any data, for putting to queue

        :Exceptions:
            - `DBQueueException`: Raise MySQLdb.OperationalError
        """

        assert timeout > 0 or timeout is None, '`timeout` must be a positive number'

        self.logger.debug('Try to put data')
        __time_start = time.time()
        if block:
            while self.qsize() >= self.__max_queue_size:
                if timeout and timeout <= (time.time() - __time_start):
                    self.logger.warning('Queue is Full')
                    raise DBQueueFull(self.__table_name)
                time.sleep(self.__sleep_time)
        else:
            if self.qsize() >= self.__max_queue_size:
                self.logger.warning('Queue is Full')
                raise DBQueueFull(self.__table_name)

        val = cPickle.dumps(item)
        insert_request = 'INSERT INTO %s (data) VALUES ("%s")' % (self.__table_name, val,)
        cursor = self.__conn.cursor()
        try:
            try:
                cursor.execute('START TRANSACTION')
                cursor.execute(insert_request)
                cursor.execute('COMMIT')
                self.logger.debug('Put data successful')
            except (MySQLdb.ProgrammingError, MySQLdb.OperationalError), e:
                self.logger.error('Transaction Error: %s', e[1])
                try:
                    cursor.execute('ROLLBACK')
                except MySQLdb.OperationalError:
                    pass
                raise DBQueueException('Can`t put %s' % (val,))
        finally:
            cursor.close()

    def get(self, block=True, timeout=None):

        """Getting item from queue. and delete them.

        :Return:
            Return data from queue, if exist.

        :Exceptions:
            - `DBQueueException`: Raise MySQLdb.OperationalError
        """

        assert timeout > 0 or timeout is None, '`timeout` must be a positive number'

        self.logger.debug('Try to getting data')
        __time_start = time.time()
        if block:
            while self.qsize() <= 0:
                if timeout and timeout > (time.time() - __time_start):
                    self.logger.warning('Queue is Empty')
                    raise DBQueueEmpty(self.__table_name)
                time.sleep(self.__sleep_time)
        else:
            if self.qsize() <= 0:
                self.logger.warning('Queue is Empty')
                raise DBQueueEmpty(self.__table_name)

        cursor = self.__conn.cursor()
        try:
            try:
                cursor.execute(self.__get_data_request)
                data = cursor.fetchone()
                if data:
                    id, pdata = data
                    try:
                        self.logger.debug('Delete data')
                        delete_request = 'DELETE FROM %s WHERE id=%s' % (self.__table_name, id,)
                        cursor.execute(delete_request)
                    except MySQLdb.OperationalError, e:
                        self.logger.error(e)
                        raise DBQueueException('Can`t delete data')
                try:
                    result = cPickle.loads(pdata)
                except EOFError:
                    self.logger.error('Data in queue corrupted')
                    raise DBQueueException('Data in queue corrupted')
            except (MySQLdb.ProgrammingError, MySQLdb.OperationalError), e:
                self.logger.error(e[1])
                raise DBQueueException('Can`t get data from %s' % (self.__table_name,))
        finally:
            cursor.close()

        self.logger.debug('Get data successful')
        return result
sad0
