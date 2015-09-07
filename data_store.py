import MySqldb
    
class data_store:
    def __init__(self, hostname, username, password, dbname):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.dbname = dbname

    class DBWrapper:
        self.db = None
        self.cursor = None

        def __init__(self, db):
            self.db = db
            self.cursor = db.cursor()

    def __enter__(self):
        self.db = DBWrapper( MySQLdb.connect(self.hostname, self.username, self.password, self.dbname) )
        return self.db.cursor

    def __exit__(self):
        self.db.commit()
        self.db.close()
