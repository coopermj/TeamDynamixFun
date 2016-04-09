#!/usr/bin/python
# This Python file uses the following encoding: utf-8
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

def getToken(username, password):
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
        #print('Bearer HTTP Status Code: {status_code}'.format(
        #    status_code=response.status_code))
        if 200 != response.status_code:
            print('Bearer HTTP Status Code: {status_code}'.format(
            status_code=response.status_code))
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


def getData(token, c, lastStart):
    tdson = send_request(token, lastStart)
    if tdson is None:
        return False
    for ticket in tdson:
        upsert(c, ticket)
        print ticket['ID']
    return True


def getlast(c):
    #c.execute("SELECT MAX(datetime(runStart,'localtime')) FROM tdruns WHERE runEnd IS NOT NULL")
    #c.execute("SELECT MAX(datetime(CreatedDate,'localtime')) FROM tickets")
    #lastStart = datetime.now()
#   c.execute("SELECT MAX(datetime(trackdate,'localtime')) FROM tdbatch")
    c.execute("SELECT MAX(trackdate) FROM tdbatch")
    data = c.fetchone()[0]
#   c.execute('SELECT datetime("2012-07-01 00:00:00", "localtime")')
    c.execute('SELECT "2012-07-01 00:00:00"')
    tdate = parser.parse(c.fetchone()[0])

    if data:
        greatestdate = parser.parse(data)
        lastdate = max(greatestdate, tdate)
    else:
        lastdate = tdate

    #lastdate = lastdate - timedelta(minutes=2)
    return lastdate

def checkconfig(config):
    if config is None:
        return False
    if 'username' not in config:
        return False
    elif 'password' not in config:
        return False
    elif 'school' not in config:
        return False
    else:
        return True

def doconfig(config):
    u = None
    p = None
    s = None
    if config is None:
        config = dict()

    if 'username' not in config:
        print "Enter your TeamDynamix username: ",
        u = raw_input()
        if u is None:
            sys.exit(0)
    else:
        u = config['username']
    if 'password' not in config:
        print "Enter your TeamDynamix password: ",
        p = raw_input()
        if p is None:
            sys.exit(0)
    else:
        p = config['password']
    if 'school' not in config:
        print "Enter your school name: ",
        s = raw_input()
    else:
        s = config['school']

    token = getToken(u,p)
    if token is None:
        doconfig(None)
    else:
        config['username'] = u
        config['password'] = p
        config['school'] = s
        with open ('config.yaml', 'w') as outfile:
            outfile.write(yaml.dump(config, default_flow_style=False))
        return token


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
conffile = 'config.yaml'

try:
    config = yaml.safe_load(open(conffile))
except:
    config = None


token = doconfig(config)

conn = sqlite3.connect('td2.db')

c = conn.cursor()
c.execute('PRAGMA journal_mode=OFF;') # bump da speed
conn.commit()

# Create tables if they do not exist

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

trackdate = getlast(c)
wegood = getData(token, c, trackdate)
if wegood is False:
    sys.exit(1)
else:
    rec = [trackdate]
    c.execute('INSERT INTO tdbatch (trackdate) VALUES(?)', rec)
    conn.commit()
    rec = [now]
    c.execute('INSERT INTO tdbatch (trackdate) VALUES(?)', rec)
    then = datetime.now()
    rec = [then, now]
    c.execute('UPDATE tdruns SET runEnd = ? WHERE runStart = ?', rec)
    conn.commit()

conn.close()

#while trackdate < now:
#    print "Starting loop with trackdate: " + str(trackdate)
#    wegood = getData(token, c, trackdate)
#    if wegood is False:
#        break
#    else:
#        rec = [trackdate]
#        c.execute('INSERT INTO tdbatch (trackdate) VALUES(?)', rec)
#        trackdate = trackdate + timedelta(weeks=2)
#        conn.commit()



#conn.commit()

#conn.close()