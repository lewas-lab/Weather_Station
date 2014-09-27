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

import data_store
import data_inserter
import dbtable

##########
# Functions for termial use

read_codes = { 'read wind': '0R1',
               'read PTH': '0R2',
               'read precipitation': '0R3',
               'read selfcheck': '0R5',
               'read all': '0R0',
               'check wind': '0WU',
               'check PTH': '0TU',
               'check precipitation': '0RU',
               'check self': '0SU'
           }

def message_response(metric):
    try:
        weatherStation.write(read_codes[metric] + "\r\n")
    except KeyError:
        raise RunTimeError("{0}: unknown command".format(metric))
    else:
        return weatherStation.readline() ## or read multiple lines, need logic for that

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
    
dispatch = { 'TX,Rain reset\r\n': True,
             'TX,Sync/address error\r\n': False,
             'TX,Unknown cmd error\r\n': False
}

reset_codes = { 'rain': '0XZRU',
                'intensity': '0XZRI'
            }

def reset(code):
    try:
        weatherStation.write( reset_codes[code] + '\r\n')
    except KeyError:
        sys.stderr.write("{0}: unknown reset command\n")
        sys.exit(-1)

    lineNum=0
    lastReads=[]

    for line in weatherStation: ## This loop can be cleaned up more but I need to see the output of the sensor to have a better idea of what needs to happen
        if lineNum==2:
            for item in lastReads:
                log.write("Response from bad " + code + " reset: "+item+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
            return False
        else:
            index=line.find('T')
            line=line[index:]
    
            try:
                if not dispatch[line]:
                    log.write("Response from bad " + code + " reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
                    return False
                return True
            except KeyError:
                lastReads.append(line)
                lineNum=lineNum+1

def precipitatonReset(Time):
    if Time<=time.time():
        if not reset('rain'):
            log.write('Rain Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
            return -1
        if not reset('intensity'):
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
                log.write("{0}: Unknown datatype at: {1}\n".format(dataType, strftime("%a, %d %b %Y %H:%M:%S", gmtime()))
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

    with data_store("localhost", "root", "mysql", "LEWAS") as cursor:
        start(weatherStation, cursor)
