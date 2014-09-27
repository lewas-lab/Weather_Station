class DataInserter:
    self.table = None
    self.dataFilter = None
    self.cursor

    def __init__(self, dbtable, cursor = None, dataFilter = None):
        self.dbtable = dbtable
        self.dataFilter = dataFilter

    def insert(self, data):
        data=parseNormalString(data)
        if self.dataFilter:
            data = self.dataFilter(data)
        checkDataLists(data)
        self.table.insert(self.cursor, data)

    def parseNormalString(data):
        data=data[4:]
        listData=data.split(',')
        index=0
        dataList=[]
        for item in listData:
            index=index+1
            if index!=len(listData):
                dataList.append(item[3:-1])
            else:
                dataList.append(item[3:-3])
                for char in dataList[0]:
                    if char=='R':
                        dataList=dataList[1:]
                        return dataList

    def checkDataLists(data):
        for item in data:
            if '#' in item:
                readError(data)
