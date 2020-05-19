import stock
from settings.database import Database
from stock import Stock
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





def main():
	db = Database()
	
	stocks = db.stocks
	
	for x in stocks:
		n = Stock(x, db.connList)
		
		n.pgsql2csv()
		n.readJSONfiles()


if __name__ == "__main__": main()