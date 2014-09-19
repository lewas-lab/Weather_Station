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

# Puts the data strings into a for that the can be inserted into the database. 
def parseNormalString(data):
	
	# cut off data identifier. 
	data=data[4:]
	
	#fill list with data values
	listData=data.split(',')
	index=0
	dataList=[]
	
	#remove units for datatype. the last data value has a slightly longer unit. 
	for item in listData:
		index=index+1
		if index!=len(listData):
			dataList.append(item[3:-1])
		else:
			dataList.append(item[3:-3])
			
	# possibly redundant check
	for char in dataList[0]:
		if char=='R':
			dataList=dataList[1:]
	return dataList

# determines if the data has a missing value, missing values are replaced with '#'
def checkDataLists(data):
	for item in data:
		if '#' in item:
			readError(data)

# insterts data into the wind data table.
def windWrite(data):
	usableData=parseNormalString(data)
	checkDataLists(usableData)
	sql="INSERT INTO WindData(DirecMin, DirecAvg, DirecMax, Speed, Gust, Lull) VALUES ('%s', '%s','%s', '%s','%s', '%s')" %(usableData[0], usableData[1], usableData[2], usableData[3], usableData[4], usableData[5])
	cursor.execute(sql)
	db.commit()

# inserts data into the PTH database table.
def PTMWrite(data):
	usableData=parseNormalString(data)
	checkDataLists(usableData)
	sql="INSERT INTO PTH(Temp, Humidity, Pressure) VALUES ('%s', '%s', '%s')" %(usableData[0],usableData[1],usableData[2])
	cursor.execute(sql)
	db.commit()
	
# inserts precipitation data into the database. 
def precipitationWrite(data):
	usableData=parseNormalString(data)
	checkDataLists(usableData)
	sql="INSERT INTO Precipitation(RainAcc, RainDur, RainIn, HailAcc, HailDur, HailIn, RainPeakIn, HailPeakIn) VALUES ('%s', '%s','%s', '%s','%s', '%s', '%s', '%s')"  %(usableData[0], usableData[1], usableData[2], usableData[3], usableData[4], usableData[5], usableData[6], usableData[7])
	cursor.execute(sql)
	db.commit()

# writes the diagnostic data to the database. 
def selfCheckWrite(data):
	
	# cut off old header
	data=data[4:]
	
	# create a list of the data values
	listData=data.split(',')
	index=0
	dataList=[]
	for item in listData:
		index=index+1
		
		# remove data units for each value. the last value in the list has a larger unit.
		if index!=len(listData):
			dataList.append(item[3:-1])
		else:
			dataList.append(item[3:-2])
			
	# possible redundant check. 
	for char in dataList[0]:
		if char=='R':
			dataList=dataList[1:]
	checkDataLists(dataList)
	sql="INSERT INTO SelfCheck(HeatingTemp, HeatingV, SupplyV, RefV) VALUES ('%s', '%s','%s', '%s')" %(dataList[0], dataList[1], dataList[2], dataList[3])
	cursor.execute(sql)
	db.commit()
	
# Writes the line of bad data to the log along with the time and terminates data collection. 
def readError(line):
	log.write('Read error in line: '+line+'\n')
	log.write('At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
	db.close()
	sys.exit()

# restes rain statistics. 
def resetRain():
	
	# sends rest commad
	weatherStation.write('0XZRU\r\n')
	lineNum=0
	lastReads=[]
	
	# Looks for the reset achnoledgement. 
	for line in weatherStation:
		if lineNum==2:
			#go throught the last few lines of data, write them to log. 
			for itme in lastReads:
				log.write("Responsen from bad rain reset: "+item+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
			return False
		else:
			index=line.find('T')
			line=line[index:]
			
			# good reset
			if line=='TX,Rain reset\r\n':
				return True
				
			# could not find the device
			elif line=='TX,Sync/address error\r\n':
				log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
				return False
				
			# did not reconise the command
			elif line=='TX,Unknown cmd error\r\n':
				log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
				return False
			
			#unkown returns. 
			else:
				lastReads.append(line)
				lineNum=lineNum+1

# rest rain intensity
def resetIntensity():
	
	# send rest commad
	weatherStation.write('0XZRI\r\n')
	lineNum=0
	lastReads=[]
	
	# Looks for the reset achnoledgement. 
	for line in weatherStation:
		#go throught the last few lines of data, write them to log. 
		if lineNum==2:
			for itme in lastReads:
				log.write("Responsen from bad rain reset: "+item+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
			return False
		else:
			index=line.find('T')
			line=line[index:]
			
			# good reset
			if line=='TX,Inty reset\r\n':
				return True
				
			# could not find the device
			elif line=='TX,Sync/address error\r\n':
				log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
				return False
				
			# did not reconise the command
			elif line=='TX,Unknown cmd error\r\n':
				log.write("Responsen from bad rain reset: "+line+" At: "+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
				return False
				
			#unkown returns.
			else:
				lastReads.append(line)
				lineNum=lineNum+1
				
# determines if it time to rest the rain, does so and then updates to when the rain needs to be rest to. 
def precipitatonReset(Time):
	if Time=>time.time():
		
		# writes error mesage if reseting of rain statistics fails. 
		if not resetRain():
			log.write('Rain Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
			db.close()
			sys.exit()
		
		# writes erorr message if resting of rain statistics fails. 
		if not resetIntensity():
			log.write('Intensity Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
			db.close()
			sys.exit()
		return Time+30
	else:
		return Time


def start():
	
	# time to reset the precipitation paramiters
	restTime=time.time()+30
	
	# time to stop collecting data
	stopTime=time.time()+120
	for line in weatherStation:
		if time.time()<stopTime:
			
			#cuts off bad starting characters and then rests the beginning of the line to as it should be.
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
				
			#If line is not as it is expected to be then it is a error message from preciptitation rest.
			else:
				plog.write('Rain Reset Failed At: '+strftime("%a, %d %b %Y %H:%M:%S", gmtime())+'\n')
				db.close()
				sys.exit()
		else:
			db.close()
			sys.exit()
			
		# rest the rain statistics depending on time interval
		restTime=precipitatonReset(restTime)

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

	start()
