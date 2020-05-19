import os
import collections
import requests
import datetime
import time
import math
import gc
import csv
import copy
import calendar
from random import shuffle
from decimal import *
import json 
from random import randint
from operator import itemgetter
from random import sample
from threading import Thread
import psycopg2

class Database:
	user=""
	password=""
	api= None
	stocks=[]
	collection=[]
	connList =[]
	now = datetime.datetime.now()
	mth = str(now.month)
	dy = str(now.day)
	yr = str(now.year)
	mfile = mth + '-' + dy + '-' + yr
	
	
	
	hols=(
		'12-25-2019',
		'01-01-2020',
		'01-20-2020',
		'02-17-2020',
		'04-10-2020',
		'02-17-2020',
		'05-25-2020',
		'06-03-2020',
		'09-07-2020',
		'11-26-2020',
		'12-25-2020',
		'01-01-2021',
		'01-18-2021',
		'02-15-2021',
		'04-02-2021',
		'05-31-2021',
		'07-05-2021',
		'09-06-2021',
		'11-25-2021',
		'11-26-2021',
		'12-24-2021',
		'01-17-2022',
		'02-21-2022',
		'04-15-2022',
		'05-30-2022',
		'07-04-2022',
		'09-05-2022',
		'11-24-2022',
		'12-26-2022',
		'01-02-2023',
		'01-16-2023',
		'02-20-2023',
		'04-07-2023',
		'05-29-2023',
		'07-04-2023',
		'09-04-2023',
		'11-23-2023',
		'12-25-2023')
	holidays=[]
	candyList=[]
	pubFile=None
	usedKeys= []
	apisKeys=[]
	
	csvHRdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/HR'
	csvMdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/M'
	csvFRdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/FR'
	csvFTdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/FT'
	csvFivedir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/Five'
	csvQdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/Q'
	csvDOdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/DO'
	csvWOdir='/pythonscripts/stocky/Storage/CSV/'+ mfile +'/WO'
	
	jsonHRdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/HR'
	jsonMdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/M'
	jsonFRdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/FR'
	jsonFTdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/FT'
	jsonFivedir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/Five'
	jsonQdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/Q'
	jsonDOdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/DO'
	jsonWOdir='/pythonscripts/stocky/Storage/JSON/'+ mfile +'/WO'
	
	
	
	def __init__(self):
		self.loadkeys()
		self.loadCredentials()
		self.loadConnections()
		self.fillStocks()
	
	
	def loadConnections(self):
		c = open('c:/pythonscripts/stocky/settings/settings.csv')
		
		csv_c = csv.reader(c)
		array=[]
		for row in csv_c:
			sArray = [row[0],row[1],row[2],row[3]]
			array.append(sArray)
		
		
		self.connM = psycopg2.connect(host=array[0][0],database=array[0][1], user=array[0][2], password=array[0][3])
		self.connF5 = psycopg2.connect(host=array[1][0],database=array[1][1], user=array[1][2], password=array[1][3])
		self.connFF = psycopg2.connect(host=array[2][0],database=array[2][1], user=array[2][2], password=array[2][3])
		self.connQ = psycopg2.connect(host=array[3][0],database=array[3][1], user=array[3][2], password=array[3][3])
		self.connFT = psycopg2.connect(host=array[4][0],database=array[4][1], user=array[4][2], password=array[4][3])
		self.connHR = psycopg2.connect(host=array[5][0],database=array[5][1], user=array[5][2], password=array[5][3])
		self.connFR = psycopg2.connect(host=array[6][0],database=array[6][1], user=array[6][2], password=array[6][3])
		self.connSTKS = psycopg2.connect(host=array[7][0],database=array[7][1], user=array[7][2], password=array[7][3])
		self.connDO = psycopg2.connect(host=array[8][0],database=array[8][1], user=array[8][2], password=array[8][3])
		self.connWO = psycopg2.connect(host=array[9][0],database=array[9][1], user=array[9][2], password=array[9][3])
		self.connPUB = psycopg2.connect(host=array[10][0],database=array[10][1], user=array[10][2], password=array[10][3])
		
		self.connList.append(self.connM)
		self.connList.append(self.connF5)
		self.connList.append(self.connFF)
		self.connList.append(self.connQ)
		self.connList.append(self.connFT)
		self.connList.append(self.connHR)
		self.connList.append(self.connFR)
		self.connList.append(self.connSTKS)
		self.connList.append(self.connDO)
		self.connList.append(self.connWO)
		self.connList.append(self.connPUB)
		
	
	def loadkeys(self):
		f = open('c:/pythonscripts/stocky/settings/apikeys.csv')
		csv_f = csv.reader(f)
		for x in csv_f:
			self.apisKeys = x
			
	def loadCredentials(self):
		f = open('c:/pythonscripts/stocky/settings/security.csv')
		csv_f = csv.reader(f)
		for x in csv_f:
			self.user = x[0]
			self.password = x[1]
			
	def fillStocks(self):
		connCur=self.connSTKS.cursor()
		print("Getting Stocks.")
		connCur.execute("SELECT DISTINCT SYMBOL FROM stklist WHERE VOLUME > 1400000  AND SYMBOL >= 'A' ORDER BY SYMBOL ASC ")
		stks=connCur.fetchall()
		for i,  stk in enumerate(stks): 
			print(stk[0])
			self.stocks.append(stk[0])
		connCur.close()
		if len(self.stocks) > 1:
			print(" Stocks are Ready.")
			
