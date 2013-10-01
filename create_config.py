import ConfigParser

class CreateConfig:
    def __init__(self, path):
        self.path = path

    def create_config(self):
        self.__config = ConfigParser.ConfigParser()
        self.__config.add_section('db_connection1')
        self.__config.set('db_connection1', 'db_host', '192.168.40.57')
        self.__config.set('db_connection1', 'db_user', 'writer')
        self.__config.set('db_connection1', 'passwd', 'writer')
        self.__config.set('db_connection1', 'db1', 'python4eg')
		self.__config.add_section('db_connection2')
        self.__config.set('db_connection1', 'db_host', '192.168.40.57')
        self.__config.set('db_connection1', 'db_user', 'reader')
        self.__config.set('db_connection1', 'passwd', 'reader')
        self.__config.set('db_connection1', 'db1', 'python4eg')
        self.__configfile = open(self.path, 'wb')
        self.__config.write(self.__configfile)

def main():
    conf = CreateConfig('db_config.conf')
    conf.create_config()

if __name__ == '__main__':
    main()