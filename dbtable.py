import MySqldb
    
class DBTable:
    self.table_name = ""
    self.fields = []
    def __init__(self, table_name, fields):
        self.table_name = table_name
        self.fields = fields

    def insert(self, cursor, data):
        sql="INSERT INTO #{table_name}(#{fields}) VALUES (#place_holders)".format(table_name=self.table_name, 
                                                                                  fields=",".join(self.fields),
                                                                                  place_holders=",".join( ["%s"]*length(self.fields) )
                                                                                  )
        cursor.execute(sql)
    
