from settings.database import Database
from os import path
import os
import time
import psycopg2
import collections
import requests
import datetime
import math
import gc
import json
import csv
import copy
import calendar




class Stock:

	id = None
	Mdir = None
	HRdir=None
	FRdir=None
	FTdir=None
	Fivedir=None
	Qdir=None
	DOdir=None
	WOdir=None
	startT = None
	elapsedT = None
	
	filenameM= None
	filenameHR= None
	filenameFT= None
	filename5= None
	filename10= None
	filenameFR= None
	filenameDO= None
	filenameWO= None
	
	doneM=0
	done10=0
	done15=0
	done5=0
	doneHR=0
	doneFF=0
	doneFT=0
	doneFR=0
	doneDO=0
	doneWO=0
	
	DSLEVEL= None
	DRLEVEL= None
	HRSLEVEL= None
	HRRLEVEL= None
	FRSLEVEL= None
	FRRLEVEL= None
	
	fullQ = []
	fullM = []
	fullF = []
	fullHR = []
	fullFR = []
	fullDO = []
	
	mfile = None
	csvHRdir = None
	csvMdir = None
	csvFRdir = None
	csvFTdir = None
	csvFivedir = None
	csvQdir = None
	csvDOdir = None
	csvWOdir = None
	
	jsonHRdir = None
	jsonMdir = None
	jsonFRdir = None
	jsonFTdir = None
	jsonFivedir = None
	jsonQdir = None
	jsonDOdir = None
	jsonWOdir = None

	def __init__(self, symbol, array):
		self.connM = array[0]
		self.connF5 = array[1]
		self.connFF = array[2]
		self.connQ = array[3]
		self.connFT = array[4]
		self.connHR = array[5]
		self.connFR = array[6]
		self.connSTKS = array[7]
		self.connDO = array[8]
		self.connWO = array[9]
		self.connPUB = array[10]
		
		self.id= symbol
		now = datetime.datetime.now()
		mth = str(now.month)
		dy = str(now.day)
		yr = str(now.year)
		mfile = mth + '-' + dy + '-' + yr
		
		
		self.csvHRdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/HR'
		self.csvMdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/M'
		self.csvFRdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/FR'
		self.csvFTdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/FT'
		self.csvFivedir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/Five'
		self.csvQdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/Q'
		self.csvDOdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/DO'
		self.csvWOdir='/pythonscripts/Stocky/storage/CSV/'+ mfile +'/WO'
		
		self.jsonHRdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/HR'
		self.jsonMdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/M'
		self.jsonFRdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/FR'
		self.jsonFTdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/FT'
		self.jsonFivedir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/Five'
		self.jsonQdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/Q'
		self.jsonDOdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/DO'
		self.jsonWOdir='/pythonscripts/Stocky/storage/JSON/'+ mfile +'/WO'
		self.mfile = mfile
	
	def quickSort(self, arr,low,high): 
		# Create an auxiliary stack 
		size = high - low + 1
		stack = [0] * (size)
		# initialize top of stack 
		top = -1

		# push initial values of l and h to stack 
		top = top + 1
		stack[top] = low 
		top = top + 1
		stack[top] = high
		
		while top >= 0:
		
			high = stack[top]
			top = top - 1
			low = stack[top]
			top = top - 1
			
			# Set pivot element at its correct position in 
			# sorted array 
			p = self.partition( arr, low, high )
			
			# If there are elements on left side of pivot, 
			# then push left side to stack 
			if p-1 > low: 
				top = top + 1
				stack[top] = low 
				top = top + 1
				stack[top] = p - 1
				
			# If there are elements on right side of pivot, 
			# then push right side to stack 
			if p + 1 < high: 
				top = top + 1
				stack[top] = p + 1
				top = top + 1
				stack[top] = high

	def partition(self, arr,low,high): 
		i = ( low-1 )         # index of smaller element 
		pivot = arr[high]     # pivot 
		for j in range(low , high): 
	
			# If current element is smaller than or 
			# equal to pivot 
			if  arr[j]['epoch'] >= pivot['epoch']: 
				
				# increment index of smaller element 
				i = i+1
				arr[i],arr[j] = arr[j],arr[i]
		arr[i+1],arr[high] = arr[high],arr[i+1] 
		return ( i+1 )
		
	def pgsql2csv(self):
		tablename = 'X'+ self.id +'X'
		
		cursor6 = self.connM.cursor()
		conn6= self.connM
		cursor5 = self.connDO.cursor()
		conn5= self.connDO
		cursor4 = self.connFR.cursor()
		conn4= self.connFR
		cursor3 = self.connHR.cursor()
		conn3= self.connHR
		cursor2 = self.connQ.cursor()
		conn2= self.connQ
		cursor1 = self.connF5.cursor()
		conn1= self.connF5

		FileM = self.csvMdir + '/'+ tablename + '.csv'
		FileHR = self.csvHRdir + '/'+ tablename + '.csv'
		FileFR = self.csvFRdir + '/'+ tablename + '.csv'
		FileDO = self.csvDOdir + '/'+ tablename + '.csv'
		FileFT = self.csvFTdir + '/'+ tablename + '.csv'
		FileFive = self.csvFivedir + '/'+ tablename + '.csv'
		FileQ = self.csvQdir + '/'+ tablename + '.csv'
		FileWO = self.csvWOdir + '/'+ tablename + '.csv'
		
		if not os.path.exists(self.csvMdir):
			print("CREATING DIRECTORY " + self.csvMdir + " .....")
			os.makedirs(self.csvMdir)
		else:
			print("DIRECTORY " + self.csvMdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(self.csvHRdir):
			print("CREATING DIRECTORY " + self.csvHRdir + " .....")
			os.makedirs(self.csvHRdir)
		else:
			print("DIRECTORY " + self.csvHRdir + " ALREADY EXISTS!!!")
			
		if not os.path.exists(self.csvQdir):
			print("CREATING DIRECTORY " + self.csvQdir + " .....")
			os.makedirs(self.csvQdir)
		else:
			print("DIRECTORY " + self.csvQdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(self.csvFRdir):
			print("CREATING DIRECTORY " + self.csvFRdir + " .....")
			os.makedirs(self.csvFRdir)
		else:
			print("DIRECTORY " + self.csvFRdir + " ALREADY EXISTS!!!")
			
		if not os.path.exists(self.csvFivedir):
			print("CREATING DIRECTORY " + self.csvFivedir + " .....")
			os.makedirs(self.csvFivedir)
		else:
			print("DIRECTORY " + self.csvFivedir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(self.csvFTdir):
			print("CREATING DIRECTORY " + self.csvFTdir + " .....")
			os.makedirs(self.csvFTdir)
		else:
			print("DIRECTORY " + self.csvFTdir + " ALREADY EXISTS!!!")
			
		if not os.path.exists(self.csvDOdir):
			print("CREATING DIRECTORY " + self.csvDOdir + " .....")
			os.makedirs(self.csvDOdir)
		else:
			print("DIRECTORY " + self.csvDOdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(self.csvWOdir):
			print("CREATING DIRECTORY " + self.csvWOdir + " .....")
			os.makedirs(self.csvWOdir)
		else:
			print("DIRECTORY " + self.csvWOdir + " ALREADY EXISTS!!!")
		
		sql = "COPY (SELECT SYMBOL,TDATE as tradingday, THOUR, TMIN, OPEN, HIGH, LOW, CLOSE, TYPICAL_PRICE FROM "+ tablename + " ORDER BY ID ASC) TO STDOUT WITH CSV HEADER DELIMITER ','"
		with open(FileFive, "w") as file:
			cursor1.copy_expert(sql, file)
		
		sql = "COPY (SELECT SYMBOL,TDATE as tradingday, THOUR, TMIN, OPEN, HIGH, LOW, CLOSE, TYPICAL_PRICE FROM "+ tablename + " ORDER BY ID ASC) TO STDOUT WITH CSV HEADER DELIMITER ','"
		with open(FileQ, "w") as file:
			cursor2.copy_expert(sql, file)
		
		sql = "COPY (SELECT SYMBOL,TDATE as tradingday, THOUR, OPEN, HIGH, LOW, CLOSE, TYPICAL_PRICE FROM "+ tablename + " ORDER BY ID ASC) TO STDOUT WITH CSV HEADER DELIMITER ','"
		with open(FileHR, "w") as file:
			cursor3.copy_expert(sql, file)
		
		sql = "COPY (SELECT SYMBOL,TDATE as tradingday, THOUR, OPEN, HIGH, LOW, CLOSE, TYPICAL_PRICE FROM "+ tablename + " ORDER BY ID ASC) TO STDOUT WITH CSV HEADER DELIMITER ','"
		with open(FileFR, "w") as file:
			cursor4.copy_expert(sql, file)
		
		sql = "COPY (SELECT SYMBOL,TDATE as tradingday, OPEN, HIGH, LOW, CLOSE, TYPICAL_PRICE FROM "+ tablename + " ORDER BY ID ASC) TO STDOUT WITH CSV HEADER DELIMITER ','"
		with open(FileDO, "w") as file:
			cursor5.copy_expert(sql, file)
		
		sql = "COPY (SELECT SYMBOL,TDATE as tradingday, THOUR, TMIN, OPEN, HIGH, LOW, CLOSE, TYPICAL_PRICE FROM "+ tablename + " ORDER BY ID ASC) TO STDOUT WITH CSV HEADER DELIMITER ','"
		with open(FileM, "w") as file:
			cursor6.copy_expert(sql, file)
			
			
	def readJSONfiles(self):
		tablename = 'X'+ self.id +'X'
		dates= ('4-22-2020','4-25-2020','4-27-2020','4-29-2020','4-30-2020','5-2-2020','5-4-2020','5-5-2020','5-7-2020','5-8-2020','5-11-2020','5-12-2020','5-18-2020')
		
		
		Qdata = []
		Qdict = {}
		
		Mdata = []
		Mdict = {}
		
		Fdata = []
		Fdict = {}
		
		FRdata = []
		FRdict = {}
		
		DOdata = []
		DOdict = {}
		
		HRdata = []
		HRdict = {}
		
		for date in dates:
			print("Looking into Fuels folder")
			print("For date ")
			print(date)
			fileQ = 'C:/PythonScripts/PyStks/tmp/fuel/' + date + '/Q/' + tablename + '.json'
			fileM = 'C:/PythonScripts/PyStks/tmp/fuel/' + date + '/M/' + tablename + '.json'
			fileF5 = 'C:/PythonScripts/PyStks/tmp/fuel/' + date + '/Five/' + tablename + '.json'
			fileFR = 'C:/PythonScripts/PyStks/tmp/fuel/' + date + '/FR/' + tablename + '.json'
			fileHR = 'C:/PythonScripts/PyStks/tmp/fuel/' + date + '/HR/' + tablename + '.json'
			fileDO = 'C:/PythonScripts/PyStks/tmp/fuel/' + date + '/DO/' + tablename + '.json'
			
			if path.exists(fileQ):
				with open(fileQ, "r") as rfile:
					tempQ =json.load(rfile)
				for x in tempQ:
					if x['epoch'] not in Qdict:
						Qdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
							#print('timestamp keys deleted.')
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
							#print('volume keys not found.')
						except:
							print('volume keys not found.')
						Qdata.append(x)
				del tempQ [:]
			if path.exists(fileM):
				with open(fileM, "r") as rfile:
					tempM =json.load(rfile)
				for x in tempM:
					if x['epoch'] not in Mdict:
						Mdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
							#print('timestamp keys deleted.')
						except KeyError:
							print('timestamp keys not found.')
							
						try:
							del x['volume']
							#print('volume keys deleted.')
						except:
							print('volume keys not found.')
						
						Mdata.append(x)
				del tempM [:]
			if path.exists(fileF5):
				with open(fileF5, "r") as rfile:
					tempF5 =json.load(rfile)
				for x in tempF5:
					if x['epoch'] not in Fdict:
						Fdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
							
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						Fdata.append(x)
				del tempF5 [:]
			
			if path.exists(fileHR):
				with open(fileHR, "r") as rfile:
					tempHR =json.load(rfile)
				for x in tempHR:
					if x['epoch'] not in HRdict:
						HRdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
							
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						HRdata.append(x)
				del tempHR [:]
			if path.exists(fileFR):
				with open(fileFR, "r") as rfile:
					tempFR =json.load(rfile)
				for x in tempFR:
					if x['epoch'] not in FRdict:
						FRdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
							
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						FRdata.append(x)
				del tempFR [:]
			if path.exists(fileDO):
				with open(fileDO, "r") as rfile:
					tempDO =json.load(rfile)
				for x in tempDO:
					if x['epoch'] not in DOdict:
						DOdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
							
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						DOdata.append(x)
				del tempDO [:]

		csvM = self.csvMdir + '/'+ tablename + '.csv'
		csvHR = self.csvHRdir + '/'+ tablename + '.csv'
		csvFR = self.csvFRdir + '/'+ tablename + '.csv'
		csvDO = self.csvDOdir + '/'+ tablename + '.csv'
		csvFT = self.csvFTdir + '/'+ tablename + '.csv'
		csvFive = self.csvFivedir + '/'+ tablename + '.csv'
		csvQ = self.csvQdir + '/'+ tablename + '.csv'
		csvWO = self.csvWOdir + '/'+ tablename + '.csv'
		
		
		
		if path.exists(csvM):
			with open(csvM) as f:
				reader = csv.DictReader(f)
				tempM = [i for i in reader]
			for x in tempM:
				rawdat = x['tradingday']+ " " + x['thour'] +" "+ x['tmin']
				predat = datetime.datetime.strptime(rawdat, "%Y-%m-%d %H %M")
				x['epoch'] = calendar.timegm(predat.utctimetuple())
			print("Size of tempM : ")
			print(len(tempM))
			for x in tempM:
				if x['epoch'] not in Mdict:
					Mdict[x['epoch']] = True
					Mdata.append(x)
			del tempM [:]
		
		
		if path.exists(csvQ):
			with open(csvQ) as f:
				reader = csv.DictReader(f)
				tempQ = [i for i in reader]
			for x in tempQ:
				rawdat = x['tradingday']+ " " + x['thour'] +" "+ x['tmin']
				predat = datetime.datetime.strptime(rawdat, "%Y-%m-%d %H %M")
				x['epoch'] = calendar.timegm(predat.utctimetuple())
			for x in tempQ:
				if x['epoch'] not in Qdict:
					Qdict[x['epoch']] = True
					Qdata.append(x)
			del tempQ [:]
		
		if path.exists(csvFive):
			with open(csvFive) as f:
				reader = csv.DictReader(f)
				tempF5 = [i for i in reader]
			for x in tempF5:
				rawdat = x['tradingday']+ " " + x['thour'] +" "+ x['tmin']
				predat = datetime.datetime.strptime(rawdat, "%Y-%m-%d %H %M")
				x['epoch'] = calendar.timegm(predat.utctimetuple())
			for x in tempF5:
				if x['epoch'] not in Fdict:
					Fdict[x['epoch']] = True
					Fdata.append(x)
			del tempF5 [:]
		
		if path.exists(csvHR):
			with open(csvHR) as f:
				reader = csv.DictReader(f)
				tempHR = [i for i in reader]
			for x in tempHR:
				rawdat = x['tradingday']+ " " + x['thour']
				predat = datetime.datetime.strptime(rawdat, "%Y-%m-%d %H")
				x['epoch'] = calendar.timegm(predat.utctimetuple())
			for x in tempHR:
				if x['epoch'] not in HRdict:
					HRdict[x['epoch']] = True
					HRdata.append(x)
			del tempHR [:]
		
		if path.exists(csvFR):
			with open(csvFR) as f:
				reader = csv.DictReader(f)
				tempFR = [i for i in reader]
			for x in tempFR:
				rawdat = x['tradingday']+ " " + x['thour']
				predat = datetime.datetime.strptime(rawdat, "%Y-%m-%d %H")
				x['epoch'] = calendar.timegm(predat.utctimetuple())
			for x in tempFR:
				if x['epoch'] not in FRdict:
					FRdict[x['epoch']] = True
					FRdata.append(x)
			del tempFR [:]
			
		if path.exists(csvDO):
			with open(csvDO) as f:
				reader = csv.DictReader(f)
				tempDO = [i for i in reader]
			for x in tempDO:
				rawdat = x['tradingday']
				predat = datetime.datetime.strptime(rawdat, "%Y-%m-%d")
				x['epoch'] = calendar.timegm(predat.utctimetuple())
			for x in tempDO:
				if x['epoch'] not in DOdict:
					DOdict[x['epoch']] = True
					DOdata.append(x)
			del tempDO [:]
			
		bkDirM = 'C:/PythonScripts/PyStks/DONOTDELETE/FULL/M/' + tablename + '.json'
		bkDirFive = 'C:/PythonScripts/PyStks/DONOTDELETE/FULL/Five/' + tablename + '.json'
		bkDirQ = 'C:/PythonScripts/PyStks/DONOTDELETE/FULL/Q/' + tablename + '.json'
		bkDirHR = 'C:/PythonScripts/PyStks/DONOTDELETE/FULL/HR/' + tablename + '.json'
		bkDirFR = 'C:/PythonScripts/PyStks/DONOTDELETE/FULL/FR/' + tablename + '.json'
		bkDirDO = 'C:/PythonScripts/PyStks/DONOTDELETE/FULL/DO/' + tablename + '.json'
		
		if path.exists(bkDirM):
				with open(bkDirM, "r") as rfile:
					tempM =json.load(rfile)
				for x in tempM:
					if x['epoch'] not in Mdict:
						Mdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						Mdata.append(x)
				del tempM [:]
		
		if path.exists(bkDirFive):
				with open(bkDirFive, "r") as rfile:
					tempF5 =json.load(rfile)
				for x in tempF5:
					if x['epoch'] not in Fdict:
						Fdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						Fdata.append(x)
				del tempF5 [:]
		
		
		if path.exists(bkDirQ):
				with open(bkDirQ, "r") as rfile:
					tempQ =json.load(rfile)
				for x in tempQ:
					if x['epoch'] not in Qdict:
						Qdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						Qdata.append(x)
				del tempQ [:]
		
		if path.exists(bkDirHR):
				with open(bkDirHR, "r") as rfile:
					tempHR =json.load(rfile)
				for x in tempHR:
					if x['epoch'] not in HRdict:
						HRdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						HRdata.append(x)
				del tempHR [:]
				
				
			
		if path.exists(bkDirFR):
				with open(bkDirFR, "r") as rfile:
					tempFR =json.load(rfile)
				for x in tempFR:
					if x['epoch'] not in FRdict:
						FRdict[x['epoch']] = True
						try:
							del x['timestamp']
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						FRdata.append(x)
				del tempFR [:]
				
		if path.exists(bkDirDO):
				with open(bkDirDO, "r") as rfile:
					tempDO =json.load(rfile)
				for x in tempDO:
					if x['epoch'] not in DOdict:
						DOdict[x['epoch']] = True
						x['tradingday'] = x.pop('tradingDay')
						try:
							del x['timestamp']
						except KeyError:
							print('timestamp keys not found.')
						try:
							del x['volume']
						except:
							print('volume keys not found.')
						DOdata.append(x)
				del tempDO [:]
				
				
		qmax=	len(Qdata) -1
		mmax=	len(Mdata) -1
		fmax=	len(Fdata) -1
		hrmax=	len(HRdata) -1
		frmax=	len(FRdata) -1
		domax=	len(DOdata) -1
		print("Qmax is : ")
		print(qmax)
		print("Mmax is : ")
		print(mmax)
		if len(Qdata) > 1:
			self.quickSort(Qdata,0,qmax)
		if len(Mdata) > 1:
			self.quickSort(Mdata,0,mmax)
		if len(Fdata) > 1:
			self.quickSort(Fdata,0,fmax)
		if len(HRdata) > 1:
			self.quickSort(HRdata,0,hrmax)
		if len(FRdata) > 1:
			self.quickSort(FRdata,0,frmax)
		if len(DOdata) > 1:
			self.quickSort(DOdata,0,domax)
		
		lev1 = ['symbol','tradingday','thour', 'tmin', 'open', 'high', 'low','close','typical_price', 'epoch']
		lev2 = ['symbol','tradingday','thour', 'tmin','open', 'high', 'low','close','typical_price', 'epoch']
		lev3 = ['symbol','tradingday', 'open', 'high', 'low','close','typical_price', 'epoch']
		
		
		nMdir='/pythonscripts/stocky/Storage/CSV/backup/M/'
		nHRdir='/pythonscripts/stocky/Storage/CSV/backup/HR/'
		nFRdir='/pythonscripts/stocky/Storage/CSV/backup/FR/'
		nQdir='/pythonscripts/stocky/Storage/CSV/backup/Q/'
		nFTdir='/pythonscripts/stocky/Storage/CSV/backup/FT/'
		nFivedir='/pythonscripts/stocky/Storage/CSV/backup/Five/'
		nDOdir='/pythonscripts/stocky/Storage/CSV/backup/DO/'
		nWOdir='/pythonscripts/stocky/Storage/CSV/backup/WO/'
		
		
		if not os.path.exists(nMdir):
			print("CREATING DIRECTORY " + nMdir + " .....")
			os.makedirs(nMdir)
		else:
			print("DIRECTORY " + nMdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(nHRdir):
			print("CREATING DIRECTORY " + nHRdir + " .....")
			os.makedirs(nHRdir)
		else:
			print("DIRECTORY " + nHRdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(nFRdir):
			print("CREATING DIRECTORY " + nFRdir + " .....")
			os.makedirs(nFRdir)
		else:
			print("DIRECTORY " + nFRdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(nFivedir):
			print("CREATING DIRECTORY " + nFivedir + " .....")
			os.makedirs(nFivedir)
		else:
			print("DIRECTORY " + nFivedir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(nQdir):
			print("CREATING DIRECTORY " + nQdir + " .....")
			os.makedirs(nQdir)
		else:
			print("DIRECTORY " + nQdir + " ALREADY EXISTS!!!")
			
		if not os.path.exists(nFTdir):
			print("CREATING DIRECTORY " + nFTdir + " .....")
			os.makedirs(nFTdir)
		else:
			print("DIRECTORY " + nFTdir + " ALREADY EXISTS!!!")
		
		if not os.path.exists(nDOdir):
			print("CREATING DIRECTORY " + nDOdir + " .....")
			os.makedirs(nDOdir)
		else:
			print("DIRECTORY " + nDOdir + " ALREADY EXISTS!!!")
			
		if not os.path.exists(nWOdir):
			print("CREATING DIRECTORY " + nWOdir + " .....")
			os.makedirs(nWOdir)
		else:
			print("DIRECTORY " + nWOdir + " ALREADY EXISTS!!!")
			
		nHRfile='/pythonscripts/stocky/Storage/CSV/backup/HR/'+ tablename + '.csv'
		nMfile='/pythonscripts/stocky/Storage/CSV/backup/M/'+ tablename + '.csv'
		nFRfile='/pythonscripts/stocky/Storage/CSV/backup/FR/'+ tablename + '.csv'
		nFTfile='/pythonscripts/stocky/Storage/CSV/backup/FT/'+ tablename + '.csv'
		nFivefile='/pythonscripts/stocky/Storage/CSV/backup/Five/'+ tablename + '.csv'
		nQfile='/pythonscripts/stocky/Storage/CSV/backup/Q/'+ tablename + '.csv'
		nDOfile='/pythonscripts/stocky/Storage/CSV/backup/DO/'+ tablename + '.csv'
		nWOfile='/pythonscripts/stocky/Storage/CSV/backup/WO/'+ tablename + '.csv'
		
		
		
		
		
		try:
			with open(nHRfile, 'w') as f:
				writer = csv.DictWriter(f, fieldnames= lev2)
				writer.writeheader()
				for data in HRdata:
					#print(data)
					#print(lev2)
					writer.writerow(data)
		except IOError:
			print("I/O error Hour file")
		try:
			with open(nMfile, 'w') as f:
				writer = csv.DictWriter(f, fieldnames= lev1)
				writer.writeheader()
				for data in Mdata:
					writer.writerow(data)
		except IOError:
			print("I/O error on Minute file")
		try:
			with open(nFRfile, 'w') as f:
				writer = csv.DictWriter(f, fieldnames= lev2)
				writer.writeheader()
				for data in FRdata:
					writer.writerow(data)
		except IOError:
			print("I/O error on 4H file")
		try:
			with open(nQfile, 'w') as f:
				writer = csv.DictWriter(f, fieldnames= lev1)
				writer.writeheader()
				for data in Qdata:
					writer.writerow(data)
		except IOError:
			print("I/O error on Q file")
		try:
			with open(nFivefile, 'w') as f:
				writer = csv.DictWriter(f, fieldnames= lev1)
				writer.writeheader()
				for data in Fdata:
					writer.writerow(data)
		except IOError:
			print("I/O error on Five file")
		try:
			with open(nDOfile, 'w') as f:
				writer = csv.DictWriter(f, fieldnames= lev3)
				writer.writeheader()
				for data in DOdata:
					print('1',data)
					print('2',type(data))
					if 'openInterest' in data:
						data.pop('openInterest')
					if 'thour' in data :
						data.pop('thour')
					if 'tmin' in data:
						data.pop('tmin')
					print('*', data)
					writer.writerow(data)
		except IOError:
			print("I/O error on Five file")
		