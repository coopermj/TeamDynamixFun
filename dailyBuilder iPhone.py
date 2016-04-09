#__        __   _                            _          _   _
#\ \      / /__| | ___ ___  _ __ ___   ___  | |_ ___   | |_| |__   ___
# \ \ /\ / / _ \ |/ __/ _ \| '_ ` _ \ / _ \ | __/ _ \  | __| '_ \ / _ \
#  \ V  V /  __/ | (_| (_) | | | | | |  __/ | || (_) | | |_| | | |  __/
#   \_/\_/ \___|_|\___\___/|_| |_| |_|\___|  \__\___/   \__|_| |_|\___|
#
# ____        _ _         ____        _ _     _
#|  _ \  __ _(_) |_   _  | __ ) _   _(_) | __| | ___ _ __
#| | | |/ _` | | | | | | |  _ \| | | | | |/ _` |/ _ \ '__|
#| |_| | (_| | | | |_| | | |_) | |_| | | | (_| |  __/ |
#|____/ \__,_|_|_|\__, | |____/ \__,_|_|_|\__,_|\___|_|
#                 |___/

# Written by Micah Cooper
# This code extracts data from TeamDynamix and places it into a local database for advanced manipulation.

#  1. Get basic info for all tickets in range
#
#       ┌─────────────────────┐                                 ┌───────────────────┐
#       │                     │◀────POST /api/tickets/search────┤                   │
#       │                     │                                 │   send_request    │          ┌──────────┐
#       │     TeamDynamix     │────────ID, CreationDate────────▶│                   │─upsert──▶│  sqlite  │
#       │                     │                                 │                   │          └──────────┘
#       │                     │                                 └───────────────────┘
#       └─────────────────────┘
#
#
#  2. Get expanded info for each ticket
#
#
#       ┌─────────────────────┐                                 ┌───────────────────┐
#       │                     │◀───GET /api/tickets/{ticketid}──┤                   │
#       │                     │                                 │     getticket     │          ┌──────────┐
#       │     TeamDynamix     │───────────all stuffs───────────▶│                   │─upsert──▶│  sqlite  │
#       │                     │                                 │                   │          └──────────┘
#       │                     │                                 └───────────────────┘
#       └─────────────────────┘
#
#
# 3. Loop through tickets to build how many
#               open each day
#
#
#         ┌─────────────────────┐              ┌───────────────────┐       count
#         │                     │◀─────────────│  getdailyopen()   │───────(int)───────────▶
#         │                     │              └───────────────────┘
#         │       SQLite        │──┐
#         │                     │  │
#         │                     │  │
#         └─────────────────────┘  │
#                    ▲             │
#                    │             │
#                    └─────────────┘



# coding: utf-8
import json
import yaml
import os
import sqlite3
# import urllib
# import urllib2
import requests
import time
from datetime import datetime
from datetime import timedelta
#import dateutil
from dateutil import parser
#from tableausdk import *
#tfrom tableausdk.Extract import *
import pytz
#from tzlocal import get_localzone
import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as md
import sys, traceback

def getdailyopen(c):
	mindate = parser.parse(c.execute('SELECT date(MIN(CreatedDate), "localtime") FROM tickets').fetchone()[0])
	maxdate = parser.parse(c.execute('SELECT date(MAX(CreatedDate), "localtime") FROM tickets').fetchone()[0])
	loopdate = mindate
	
	d = []

	# get the number of records from each day	
	while loopdate < maxdate:
		rec = [loopdate, loopdate]
		recount = c.execute('SELECT count(*) FROM tickets WHERE CreatedDate < ? AND (CompletedDate > ? OR CompletedDate IS NULL)', rec).fetchone()[0]
		print "date: " + str(loopdate) + " count: " + str(recount)
		d.append((loopdate, recount))
		loopdate = loopdate + timedelta(days=1)

	# return the list of tuples
	return d
	

def plotcreates(c):
	try:
		#rowcount = c.execute('SELECT COUNT(DISTINCT(date(CreatedDate, "localtime"))) FROM tickets').fetchone()[0]
		#print rowcount
		#X = np.arange(rowcount)
		#Y1 = (1-X/float(rowcount)) * np.random.uniform(0.5,1.0,rowcount)
		#Y2 = (1-X/float(rowcount)) * np.random.uniform(0.5,1.0,rowcount)
	
		#plt.axes([0.025,0.025,0.95,0.95])
		#plt.bar(X, +Y1, facecolor='#9999ff', edgecolor='white')
		#plt.bar(X, -Y2, facecolor='#ff9999', edgecolor='white')
		allrows = c.execute('SELECT date(CreatedDate, "localtime") AS CreatedDate, COUNT(ID) FROM tickets GROUP BY date(CreatedDate, "localtime")').fetchall()
		dailyrows = getdailyopen(c)

#		new_x = dates.datestr2num(date)
		xs = []
		for row in allrows:
			xs.append(md.datestr2num(row[0]))
		#dates = [q[0] for q in allrows]
		counts =[q[1] for q in allrows]
		counts2 = [r[1] for r in dailyrows]
		print "xs: " + str(len(xs))
		print "count: " + str(len(counts))
		print "count2: " + str(len(counts2))
	
		#fig, ax = plt.subplots()
		plt.plot_date(xs, counts, linestyle='solid',xdate=True, ydate=False)
		#plt.plot_date(xs, counts2, linestyle='solid',xdate=True, ydate=False)		
		#ax.plot_date(xs, counts2, linestyle='solid', marker='None', color='red')
		#ax.autoscale_view()
		#fig.autofmt_xdate()
		plt.show()
		#for row in allrows:
	except:
		print 'Plot failed'
		conn.commit()
		#conn.close()
		exc_type, exc_value, exc_traceback = sys.exc_info()
		print "*** print_tb:"
		traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
		print "*** print_exception:"
		traceback.print_exception(exc_type, exc_value, exc_traceback,limit=2, file=sys.stdout)
		print "*** print_exc:"
		traceback.print_exc()
		print "*** format_exc, first and last line:"
		formatted_lines = traceback.format_exc().splitlines()
		print formatted_lines[0]
		print formatted_lines[-1]
		print "*** format_exception:"
		print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
		print "*** extract_tb:"
		print repr(traceback.extract_tb(exc_traceback))
		print "*** format_tb:"
		print repr(traceback.format_tb(exc_traceback))
		print "*** tb_lineno:", exc_traceback.tb_lineno
	

def getToken():
    # Get Bearer Token
    # POST https://teamdynamix.com/TDWebApi/api/auth

    """

    :rtype: object
    """
    try:
        response = requests.post(
            url="https://app.teamdynamix.com/TDWebApi/api/auth",
            headers={
                "Content-Type": "application/json",
            },
            data=json.dumps({
                "username": username,
                "password": password
            })
        )
        print('Bearer HTTP Status Code: {status_code}'.format(
            status_code=response.status_code))
        if 200 != response.status_code:
            return None
        #print('Response HTTP Response Body: {content}'.format(
         #   content=response.content))
        return response.content
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
        conn.commit()
        conn.close()


def send_request(token, lastStart):
    # Get Recent Tickets
    # POST https://teamdynamix.com/TDWebApi/api/tickets/search
    """

    :rtype: object
    """

    querystart = lastStart.strftime("%Y-%m-%d %H:%M:%S")
    
    queryend = parser.parse(querystart) + timedelta(weeks=1)
    queryend = queryend.strftime("%Y-%m-%d %H:%M:%S")
    
    print "Query goes from {start} to {end}".format(start=querystart, end=queryend)

    try:
        #print "Bearer {token}".format(token=token)
        response = requests.post(
            url="https://teamdynamix.com/TDWebApi/api/tickets/search",
            headers={
                "Authorization": "Bearer {token}".format(token=token),
                "Content-Type": "application/json",
            },
            data=json.dumps({
                "UpdatedDateFrom": querystart,
                "UpdatedDateTo": queryend,
                "MaxResults": 0
            })
        )
        print('Response HTTP Status Code: {status_code}'.format(
            status_code=response.status_code))
        if 200 != response.status_code:
            return None
#        print('Response HTTP Response Body: {content}'.format(
#            content=response.content))
        data = response.json()
        return data
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
        conn.commit()
        conn.close()


def upsert(c, row):
    # UPDATE Contact
    # SET Name = 'Bob'
    # WHERE Id = 3;
    """

    :type row: object
    """
    rec = [row['AccountName'], row['TypeCategoryName'], row['TypeName'], row['SlaName'], row['IsSlaResolveByViolated'],
           row['CreatedDate'], row['ResponsibleGroupName'], row['ServiceName'], row['ServiceCategoryName'],
           row['CompletedDate'], row['ID']]
    c.execute(
        "UPDATE tickets SET AccountName=?, TypeCategoryName=?, TypeName=?,SlaName=?, IsSlaResolveByViolated=?, CreatedDate=?, ResponsibleGroupName=?, ServiceName=?, ServiceCategoryName=?, CompletedDate=? WHERE ID=?",
        rec)
    c.execute(
        "INSERT INTO tickets (AccountName, TypeCategoryName, TypeName, SlaName, IsSlaResolveByViolated, CreatedDate, ResponsibleGroupName, ServiceName, ServiceCategoryName, CompletedDate, ID) SELECT ?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT changes() AS change FROM tickets WHERE change <> 0)",
        rec)


def getticket(token, ticketid):
    # Get Ticket
    # GET https://app.teamdynamix.com/TDWebApi/api/tickets/481476
    print "Getting ticketid: {ticketid}".format(ticketid=ticketid)

    try:
        response = requests.get(
            url="https://app.teamdynamix.com/TDWebApi/api/tickets/{ticketid}".format(ticketid=ticketid),
            headers={
                "Authorization": "Bearer {token}".format(token=token),
                "Content-Type": "application/octet-stream",
            },
        )
        # print('Response HTTP Status Code: {status_code}'.format(
        #    status_code=response.status_code))
        if 200 != response.status_code:
            sys.exit(-1)
        #print('Response HTTP Response Body: {content}'.format(
        #    content=response.content))
        return response.json()
    except requests.exceptions.RequestException:
        print('HTTP Request failed')


def getData(c, lastStart):
    token = getToken()
    if token:
        tdson = send_request(token, lastStart)
        if tdson is None:
        	return False
        for Index in tdson:
                ticket = getticket(token, Index['ID'])
                if ticket:
                    upsert(c, ticket)
                    print Index['ID']
        return True
    else:
      return False

def getlast(c):
	#c.execute("SELECT MAX(datetime(runStart,'localtime')) FROM tdruns WHERE runEnd IS NOT NULL")
	#c.execute("SELECT MAX(datetime(CreatedDate,'localtime')) FROM tickets")
	#lastStart = datetime.now()
#	c.execute("SELECT MAX(datetime(trackdate,'localtime')) FROM tdbatch")
	c.execute("SELECT MAX(trackdate) FROM tdbatch")
	data = c.fetchone()[0]
#	c.execute('SELECT datetime("2015-07-01 00:00:00", "localtime")')
	c.execute('SELECT "2015-12-01 00:00:00"')
	tdate = parser.parse(c.fetchone()[0])
	
	if data:
		greatestdate = parser.parse(data)
		lastdate = max(greatestdate, tdate)
	else:
		lastdate = tdate
	
	#lastdate = lastdate - timedelta(minutes=2)
	return lastdate


# _     __  __       _                       _            _             _
#/ |   |  \/  | __ _(_)_ __     ___ ___   __| | ___   ___| |_ __ _ _ __| |_ ___
#| |   | |\/| |/ _` | | '_ \   / __/ _ \ / _` |/ _ \ / __| __/ _` | '__| __/ __|
#| |_  | |  | | (_| | | | | | | (_| (_) | (_| |  __/ \__ \ || (_| | |  | |_\__ \
#|_(_) |_|  |_|\__,_|_|_| |_|  \___\___/ \__,_|\___| |___/\__\__,_|_|   \__|___/
#
# _
#| |__   ___ _ __ ___
#| '_ \ / _ \ '__/ _ \
#| | | |  __/ | |  __/
#|_| |_|\___|_|  \___|  
#


bindir = os.getcwd()
#os.chdir("..")
#basedir = os.getcwd()
#confdir = bindir + '/config'
#tdedir = basedir + '/tde_repo'
#conffile = 'config.yaml'
#global config
#config = yaml.safe_load(open(conffile))
os.chdir(bindir)

conn = sqlite3.connect('td2.db')

c = conn.cursor()
c.execute('PRAGMA journal_mode=OFF;') # bump da speed
conn.commit()

# Create table

c.execute('''CREATE TABLE IF NOT EXISTS tdbatch
            (ID INTEGER PRIMARY KEY AUTOINCREMENT,
            trackdate INTEGER
            )''')

c.execute('''CREATE TABLE IF NOT EXISTS tdruns
            (ID INTEGER PRIMARY KEY AUTOINCREMENT,
            proc TEXT,
            runStart INT,
            runEnd INT
            )''')

c.execute('''CREATE TABLE IF NOT EXISTS tickets
            (ID INTEGER PRIMARY KEY,
            AccountName TEXT,
            TypeCategoryName TEXT,
            TypeName TEXT,
            SlaName TEXT,
            IsSlaResolveByViolated INTEGER,
            CreatedDate INT,
            ResponsibleGroupName TEXT,
            ServiceName TEXT,
            ServiceCategoryName TEXT,
            CompletedDate INT)''')

# c.execute('SELECT MAX(runStart) FROM tdruns WHERE runEnd IS NOT NULL')
now = datetime.now()
rec = ['getTickets', now]
c.execute('INSERT INTO tdruns(proc, runStart) VALUES(?,?)', rec)


# Save (commit) the changes
# conn.commit()

# for row in c.execute('SELECT * FROM stocks ORDER BY price'):
#        assert isinstance(row, object)
#        print row

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.


username = ''
password = ''

trackdate = getlast(c)
	
#getData(c, trackdate)

while trackdate < now:
	print "Starting loop with trackdate: " + str(trackdate)
	wegood = getData(c, trackdate)
	if wegood is False:
		break
	else:
		rec = [trackdate]
		c.execute('INSERT INTO tdbatch (trackdate) VALUES(?)', rec)
		trackdate = trackdate + timedelta(weeks=2)
		conn.commit()

rec = [now]
c.execute('INSERT INTO tdbatch (trackdate) VALUES(?)', rec)

then = datetime.now()
rec = [then, now]
c.execute('UPDATE tdruns SET runEnd = ? WHERE runStart = ?', rec)

conn.commit()

plotcreates(c)

conn.close()