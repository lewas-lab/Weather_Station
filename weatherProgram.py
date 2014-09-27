# Authors: John Purviance and Debarati Basu
# Date: 9/11/14
# LEWAS Lab
# Near real time Vaisala Weather Transmitter WXT520 python program

########
# pySerial python library
import serial
########


import sys
import MySQLdb
import time
from time import gmtime, strftime

import data_inserter
import dbtable

##########
# Functions for termial use


read_codes = { 'wind': '0R1',
               'PTH': '0R2',
               'precipitation': '0R3',
               'selfcheck': '0R5',
               'all': '0R0'
               }

def read(metric):
    try:
        weatherStation.write(read_codes[metric] + "\r\n")
    except KeyError:
        throw RunTimeError("{0}: unknown metric")
    else:
        return weatherStation.readline() ## or read multiple lines, need logic for that

# checks the settings for wind measurment
def checkWind():
    weatherStation.write('0WU\r\n')
    print(weatherStation.readline())

# checks the settings for pressure, temperature and humidity
def checkPTH():
    weatherStation.write('0TU\r\n')
    print(weatherStation.readline())

# checks the settings for precipitation
def checkPrecipitation():
    weatherStation.write('0RU\r\n')
    print(weatherStation.readline())

# checks the setting for the supurvisor data
def checkSelfCheck():
    weatherStation.write('0SU\r\n')
    print(weatherStation.readline())

def selfCheckParser(data):
    data=data[4:]
    listData=data.split(',')
    index=0
    dataList=[]
    for item in listData:
        index=index+1
        if index!=len(listData):
            dataList.append(item[3:-1])
        else:
            dataList.append(item[3:-2])
    for char in dataList[0]:
        if char=='R':
            dataList=dataList[1:]
    return dataList

def readError(line):
    log.write('Read error in line: '+line+'\n')
    log.write('At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')

def resetRain():
    weatherStation.write('0XZRU\r\n')
    lineNum=0
    lastReads=[]

    dispatch = { 'TX,Rain reset\r\n': True,
                 'TX,Sync/address error\r\n': False,
                 'TX,Unknown cmd error\r\n': False
             }

    for line in weatherStation: ## This loop can be cleaned up more but I need to see the output of the sensor to have a better idea of what needs to happen
        if lineNum==2:
            for itme in lastReads:
                log.write("Responsen from bad rain reset: "+item+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
            return False
        else:
            index=line.find('T')
            line=line[index:]
    
            try:
                if not dispatch[line]:
                    log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
                    return False
                return True
            except KeyError:
                lastReads.append(line)
                lineNum=lineNum+1


def resetIntensity():
    weatherStation.write('0XZRI\r\n')
    lineNum=0
    lastReads=[]
    for line in weatherStation:
        if lineNum==2:
            for itme in lastReads:
                log.write("Responsen from bad rain reset: "+item+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
            return False
        else:
            index=line.find('T')
            line=line[index:]
            if line=='TX,Inty reset\r\n':
                return True
            elif line=='TX,Sync/address error\r\n':
                log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
                return False
            elif line=='TX,Unknown cmd error\r\n':
                log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
                return False
            else:
                lastReads.append(line)
                lineNum=lineNum+1

def precipitatonReset(Time):
    if Time<=time.time():
        if not resetRain():
            log.write('Rain Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
            return -1
        if not resetIntensity():
            log.write('Intensity Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
            return -1
        return Time+30
    else:
        return Time


def start(weatherStation, cursor):

    dispatch = {
        "0R1": 'wind',
        "0R2": 'ptm',
        "0R3": 'precipitation',
        "0R5": 'selfcheck'
    }

    inserters = { 'wind': DataInserter(DBTable("WindData", ["DirecMin", "DirecAvg", "DirecMax", "Speed", "Gust", "Lull"]), cursor),
               'ptm': DataInserter(DBTable("PTH", ["Temp", "Humidity", "Pressure"]), cursor),
               'precipitation': DataInserter(DBTable("Precipitation", ["RainAcc", "RainDur", "RainIn", "HailAcc", "HailDur", "HailIn", "RainPeakIn", "HailPeakIn"]), cursor),
                  'selfcheck': DataInserter(DBTable("SelfCheck", ["HeatingTemp", "HeatingV", "SupplyV", "RefV"]), cursor, selfCheckParser)
           }

    startTime=time.time()+30
    stopTime=time.time()+120
    for line in weatherStation:
        if time.time()<stopTime:
            index=line.find('R')
            dataType='0'+line[index:(index+2)]
            try:
                inserters[dispatch[dataType]].insert(cursor, line)
                db.commit()
            except KeyError:
                plog.write('Rain Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
                return
        else:
            return
        startTime=precipitatonReset(startTime)

###########
#Opens the crash_log.log with privilages to appened to it.
#The serical object 'weatherStation' represents the tty connection that data flows two and from. The connection has
#no parity bits, a data packet byte size of eight bits, one stop bit, baud rate of 19200 baud. Timeout of 12000 is
#the duration that the oject tries to read form the tty file.
with open("crash_log.log", 'a', buffering=1) as log:
    weatherStation=serial.Serial('/dev/ttyUSB0')
    weatherStation.parity=serial.PARITY_NONE
    weatherStation.bytesize=serial.EIGHTBITS
    weatherStation.stopbits=serial.STOPBITS_ONE
    weatherStation.baudrate=19200
    weatherStation.timeout=12000

    ## internal locoal database connection settings.
    db=MySQLdb.connect("localhost", "root", "mysql", "LEWAS")
    cursor=db.cursor()

    try:
        start(weatherStation, cursor)
    finally:
        db.close()
