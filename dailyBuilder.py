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

earliestdate = '2015-07-01 00:00:00'

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
from tableausdk import *
from tableausdk.Extract import *

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


    print "Query starts at {start}".format(start=querystart)

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

def getData(token, c, lastStart):
    tdson = send_request(token, lastStart)
    if tdson is None:
        return False
    else:
        print "Processing tickets"
        for ticket in tdson:
            upsert(c, ticket)
            #print ticket['ID']
        return True


def getlast(c):
    c.execute("SELECT MAX(trackdate) FROM tdbatch")
    data = c.fetchone()[0]

    earlystring = 'SELECT "' + earliestdate + '"'
    #print earlystring
    c.execute(earlystring)

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

def basicextract(cursor):
    print "Building Tableau Extract"
    basicfile = 'alltickets.tde'

    # Tableau SDK does not all reading an extract, so updating an existing
    # one is moot – so we delete it first
    try:
        os.remove(basicfile)
    except OSError:
        pass

    # then build a new one
    new_extract = Extract(basicfile)

    # build our schema
    table_definition = TableDefinition()
    table_definition.addColumn('ID',         Type.INTEGER)    
    table_definition.addColumn('AccountName', Type.UNICODE_STRING)
    table_definition.addColumn('TypeCategoryName', Type.UNICODE_STRING)
    table_definition.addColumn('TypeName', Type.UNICODE_STRING)
    table_definition.addColumn('LocalCreatedDate', Type.DATETIME)
    table_definition.addColumn('ServiceName', Type.UNICODE_STRING)
    table_definition.addColumn('ServiceCategoryName', Type.UNICODE_STRING)
    table_definition.addColumn('LocalCompletedDate', Type.DATETIME)

    # Table always needs to be named Extract *shrug*
    new_table = new_extract.addTable('Extract', table_definition)

    # Query our db
    cursor.execute("SELECT ID, AccountName, TypeCategoryName, TypeName, datetime(CreatedDate, 'localtime') AS LocalCreatedDate, ServiceName, ServiceCategoryName, datetime(CompletedDate, 'localtime') AS LocalCompletedDate FROM tickets")

    for ID, AccountName, TypeCategoryName, TypeName, LocalCreatedDate, ServiceName, ServiceCategoryName, LocalCompletedDate in cursor.fetchall():
        #print ID, AccountName
        # Create new row
        new_row = Row(table_definition)   # Pass the table definition to the constructor
        # Set column values. The first parameter is the column number (its
        # ordinal position) The second parameter (or second and subsequent paramaters) is 
        # the value to set the column to.
        new_row.setInteger(0, ID)
        new_row.setString(1, AccountName)
        new_row.setString(2, TypeCategoryName)
        new_row.setString(3, TypeName)

        d = parser.parse(LocalCreatedDate)
        #if( CreatedDate.find(".") != -1) :
        #        d = datetime.strptime(CreatedDate, "%Y-%m-%d %H:%M:%S.%f")
        #else :
        #        d = datetime.strptime(CreatedDate, "%Y-%m-%d %H:%M:%S")
        new_row.setDateTime(4, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

        new_row.setString(5, ServiceName)
        new_row.setString(6, ServiceCategoryName)
        
        #if( CompletedDate.find(".") != -1) :
        #        d = datetime.datetime.strptime(CompletedDate, "%Y-%m-%d %H:%M:%S.%f")
        #else :
        #        d = datetime.datetime.strptime(CompletedDate, "%Y-%m-%d %H:%M:%S")
        d = parser.parse(LocalCompletedDate)
        new_row.setDateTime(7, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

        new_table.insert(new_row)

    # Close the extract in order to save the .tde file and clean up resources
    new_extract.close()


def dailyextract(cursor):
    print "Building Tableau Daily Extract"
    dailyfile = 'dailyopen.tde'

    # Tableau SDK does not all reading an extract, so updating an existing
    # one is moot – so we delete it first
    try:
        os.remove(dailyfile)
    except OSError:
        pass

    # then build a new one
    new_extract = Extract(dailyfile)

    # build our schema
    table_definition = TableDefinition()
    table_definition.addColumn('ID',         Type.INTEGER)    
    table_definition.addColumn('AccountName', Type.UNICODE_STRING)
    table_definition.addColumn('TypeCategoryName', Type.UNICODE_STRING)
    table_definition.addColumn('TypeName', Type.UNICODE_STRING)
    table_definition.addColumn('LocalCreatedDate', Type.DATETIME)
    table_definition.addColumn('ServiceName', Type.UNICODE_STRING)
    table_definition.addColumn('ServiceCategoryName', Type.UNICODE_STRING)
    table_definition.addColumn('LocalCompletedDate', Type.DATETIME)
    table_definition.addColumn('DisplayDate', Type.DATETIME)

    # Table always needs to be named Extract *shrug*
    new_table = new_extract.addTable('Extract', table_definition)

    # build a daily loop starting with earliest date in the db
    mindate = parser.parse(c.execute('SELECT date(MIN(CreatedDate), "localtime") FROM tickets').fetchone()[0])
    #print "mindate: ", mindate
    maxdate = parser.parse(c.execute('SELECT date(MAX(CreatedDate), "localtime") FROM tickets').fetchone()[0])
    #print "maxdate: ", maxdate
    loopdate = mindate
    while loopdate < maxdate:

        rec = [loopdate, loopdate]
        cursor.execute("SELECT ID, AccountName, TypeCategoryName, TypeName, datetime(CreatedDate, 'localtime') AS LocalCreatedDate, ServiceName, ServiceCategoryName, datetime(CompletedDate, 'localtime') AS LocalCompletedDate FROM tickets WHERE datetime(CreatedDate, 'localtime') < ? AND (datetime(CompletedDate, 'localtime') > ? OR CompletedDate IS NULL)", rec)
        #print "daily processing date: " + str(loopdate)
        for ID, AccountName, TypeCategoryName, TypeName, LocalCreatedDate, ServiceName, ServiceCategoryName, LocalCompletedDate in cursor.fetchall():
            # print ID, AccountName
            # Create new row
            new_row = Row(table_definition)   # Pass the table definition to the constructor
            # Set column values. The first parameter is the column number (its
            # ordinal position) The second parameter (or second and subsequent paramaters) is 
            # the value to set the column to.
            new_row.setInteger(0, ID)
            new_row.setString(1, AccountName)
            new_row.setString(2, TypeCategoryName)
            new_row.setString(3, TypeName)

            d = parser.parse(LocalCreatedDate)
            #if( CreatedDate.find(".") != -1) :
            #        d = datetime.strptime(CreatedDate, "%Y-%m-%d %H:%M:%S.%f")
            #else :
            #        d = datetime.strptime(CreatedDate, "%Y-%m-%d %H:%M:%S")
            new_row.setDateTime(4, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

            new_row.setString(5, ServiceName)
            new_row.setString(6, ServiceCategoryName)
            
            #if( CompletedDate.find(".") != -1) :
            #        d = datetime.datetime.strptime(CompletedDate, "%Y-%m-%d %H:%M:%S.%f")
            #else :
            #        d = datetime.datetime.strptime(CompletedDate, "%Y-%m-%d %H:%M:%S")
            d = parser.parse(LocalCompletedDate)
            new_row.setDateTime(7, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

            d = loopdate
            new_row.setDateTime(8, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

            new_table.insert(new_row)

        loopdate = loopdate + timedelta(days=1)

    # Close the extract in order to save the .tde file and clean up resources
    new_extract.close()



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

# Due to the vagaries of using update dates instead of created dates,
# we get old tickets in the flow that we need to terminate
delstring = 'DELETE FROM tickets WHERE CreatedDate < "' + earliestdate + '"'
c.execute(delstring)

basicextract(c)
dailyextract(c)

conn.close()

