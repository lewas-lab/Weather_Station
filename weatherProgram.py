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


##########
# Functions for termial use

# requests an individual line of wind data
def readWind():
	weatherStation.write('0R1\r\n')
	print(weatherStation.readline())

# requests an individual line of Pressure, Temperature and Humidity data
def readPTH():
	weatherStation.write('0R2\r\n')
	print(weatherStation.readline())

# requests an individual line of precipitation data
def readPrecipitation():
	weatherStation.write('0R3\r\n')
	print(weatherStation.readline())

# requests the Supervisor data message
def selfCheck():
	weatherStation.write('0R5\r\n')
	print(weatherStation.readline())

# requests for all the data values to be read
def readAll():
	weatherStation.write('0R0\r\n')
	for line in weatherStation:
		print(line)

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

def windWrite(data):
	usableData=parseNormalString(data)
	checkDataLists(usableData)
	sql="INSERT INTO WindData(DirecMin, DirecAvg, DirecMax, Speed, Gust, Lull) VALUES ('%s', '%s','%s', '%s','%s', '%s')" %(usableData[0], usableData[1], usableData[2], usableData[3], usableData[4], usableData[5])
	cursor.execute(sql)
	db.commit()

def PTMWrite(data):
	usableData=parseNormalString(data)
	checkDataLists(usableData)
	sql="INSERT INTO PTH(Temp, Humidity, Pressure) VALUES ('%s', '%s', '%s')" %(usableData[0],usableData[1],usableData[2])
	cursor.execute(sql)
	db.commit()

def precipitationWrite(data):
	usableData=parseNormalString(data)
	checkDataLists(usableData)
	sql="INSERT INTO Precipitation(RainAcc, RainDur, RainIn, HailAcc, HailDur, HailIn, RainPeakIn, HailPeakIn) VALUES ('%s', '%s','%s', '%s','%s', '%s', '%s', '%s')"  %(usableData[0], usableData[1], usableData[2], usableData[3], usableData[4], usableData[5], usableData[6], usableData[7])
	cursor.execute(sql)
	db.commit()

def selfCheckWrite(data):
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
	checkDataLists(dataList)
	sql="INSERT INTO SelfCheck(HeatingTemp, HeatingV, SupplyV, RefV) VALUES ('%s', '%s','%s', '%s')" %(dataList[0], dataList[1], dataList[2], dataList[3])
	cursor.execute(sql)
	db.commit()

def readError(line):
	log.write('Read error in line: '+line+'\n')
	log.write('At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
	db.close()
	sys.exit()

def resetRain():
	weatherStation.write('0XZRU\r\n')
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
			if line=='TX,Rain reset\r\n':
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
			db.close()
			sys.exit()
		if not resetIntensity():
			log.write('Intensity Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
			db.close()
			sys.exit()
		return Time+30
	else:
		return Time

#  
def start(weatherStation):
	startTime=time.time()+30
	stopTime=time.time()+120
	for line in weatherStation:
		if time.time()<stopTime:
			index=line.find('R')
			dataType='0'+line[index:(index+2)]
			if dataType=="0R1":
				windWrite(line)
			elif dataType=="0R2":
				PTMWrite(line)
			elif dataType=="0R3":
				precipitationWrite(line)
			elif dataType=="0R5":
				selfCheckWrite(line)
			else:
				plog.write('Rain Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
				db.close()
				sys.exit()
		else:
			db.close()
			sys.exit()
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

	start(weatherStation)
