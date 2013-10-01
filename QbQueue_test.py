import logging
import optparse
import sys

import DbQueue
import MySQLdb

MAX_QUEUE_SIZE = 50
TABLE_NAME = 'table1'

def get_options():
    parse = optparse.OptionParser()
    parse.add_option('-x','--hostname', action='store', dest='host',
                    help='Hostname or IP addres My server', type='string',
                    default = '192.168.40.57')
    parse.add_option('-p','--port', action='store', dest='port',
                    help='Port', type='int', default = 3306)
    parse.add_option('--user', action='store', dest='user',
                    help='User name', type='str', default = 'writer')
    parse.add_option('--password', action='store',dest='psw',
                    help='Password', type='str', default = 'writer')
    parse.add_option('--db', action='store',dest='db',
                    help='Name of DataBase', type='str', default = 'olazar')
    parse.add_option('-v','--verbose', action='store', dest='verb',
                    help='Level', type='choice',
                    choices=('DEBUG','WARNING','CRITICAL','ERROR'), default = 'DEBUG')
    (option,args) = parse.parse_args()
    return option

def main():
    format = '%(asctime)s - %(name)s %(levelname)-7s %(message)s'
    datefmt = '%a, %d %b %Y %H:%M:%S'
    logging.basicConfig(level=logging.DEBUG, format=format,
                        datefmt=datefmt)
    logger = logging.getLogger()
    opts = get_options()
    db_connect = MySQLdb.Connect(opts.host, opts.user, opts.psw, opts.db)
    try:
        try:
            db_client = DbQueue.DBQueue(db_connect, TABLE_NAME, MAX_QUEUE_SIZE)
            db_client.put('asd')
            print 'Queue get: %s' % db_client.get()
            print 'Queue size: %s' %  db_client.qsize()
        except Exception, e:
            print e
    finally:
        db_connect.close()

if __name__ == '__main__':
    sys.exit(main())