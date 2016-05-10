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
#         │                     │◀─────────────│  dailyextract()   │───────(int)───────────▶
#         │                     │              └───────────────────┘
#         │       SQLite        │──┐
#         │                     │  │
#         │                     │  │
#         └─────────────────────┘  │
#                    ▲             │
#                    │             │
#                    └─────────────┘

# 4. (optional) Upload subset of summary results into webservice for cross-comparison

earliestdate = '2015-07-01 00:00:00'

# coding: utf-8
import json
import yaml
import os
import shutil
import errno
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
#import msgpack
#from io import BytesIO
import readchar
#import xml.etree.cElementTree as ET
import sys, traceback
import copy
from threading import Thread
import getpass

# if zlib, get fancier compression
import zipfile
try:
    import zlib
    compression = zipfile.ZIP_DEFLATED
except:
    compression = zipfile.ZIP_STORED


# Globals
domain = ""

def updatelabels(domain):
    twbedit('best.edu', domain)
    datestr = datetime.now().strftime("%Y-%m-%d %H:%M")
    twbedit('xx/xx/xxxx', datestr)

def twbedit(textToSearch, textToReplace):
    #print "searching for " + textToSearch + "\n"
    # from http://stackoverflow.com/questions/17140886/how-to-search-and-replace-text-in-a-file-using-python
    filein = 'dist/TDAnalysis.twb'
    fileout = 'dist/TDAnalysis.twb'
    f = open(filein,'r')
    filedata = f.read()
    f.close()

    newdata = filedata.replace(textToSearch,textToReplace)

    f = open(fileout,'w')
    f.write(newdata)
    f.close()


def getToken(username, password):
    # Get Bearer Token
    # POST https://teamdynamix.com/TDWebApi/api/auth
    global domain

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
        domain = username.split("@")[-1]
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


    print "Downloading data from TeamDynamix.\nQuery period starts {start}".format(start=querystart)

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
        print('Connected and beginning work'.format(
            status_code=response.status_code))
        if 200 != response.status_code:
            print('Response HTTP Status Code: {status_code}'.format(
            status_code=response.status_code))
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
    """

    :type row: object
    """
    rec = [row['AccountName'], row['TypeCategoryName'], row['TypeName'], row['SlaName'], row['IsSlaResolveByViolated'],
           row['CreatedDate'], row['ResponsibleGroupName'], row['ServiceName'], row['ServiceCategoryName'],
           row['CompletedDate'], row['ID'], row['DaysOld'], row['ResolveByDate']]
    c.execute("INSERT OR REPLACE INTO tickets (AccountName, TypeCategoryName, TypeName, SlaName, IsSlaResolveByViolated, CreatedDate, ResponsibleGroupName, ServiceName, ServiceCategoryName, CompletedDate, ID, DaysOld, ResolveByDate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rec)


def getData(token, c, lastStart):
    tdson = send_request(token, lastStart)
    if tdson is None:
        return False
    else:
        print "Processing tickets"
        for ticket in tdson:
            #print ticket
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
        print "Enter your TeamDynamix username (not SSO): ",
        u = raw_input()
        if u is None:
            sys.exit(0)
    else:
        u = config['username']
    if 'password' not in config:
        #print "Enter your TeamDynamix password: ",
        p = getpass.getpass('Enter your TeamDynamix password (will not display – not SSO): ')
        #p = raw_input()
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
        with open ('data/config.yml', 'w') as outfile:
            outfile.write(yaml.dump(config, default_flow_style=False))
        return token

def basicextract(cursor):
    print "Building Tableau Extract"
    os.chdir("data")
    basicfile = 'alltickets.tde'

    # Tableau SDK does not allow reading an extract, so updating an existing
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
    table_definition.addColumn('ResolveByDate', Type.DATETIME)
    table_definition.addColumn('DaysOld',         Type.INTEGER)     

    # Table always needs to be named Extract *shrug*
    new_table = new_extract.addTable('Extract', table_definition)

    # Query our db
    cursor.execute("SELECT ID, AccountName, TypeCategoryName, TypeName, datetime(CreatedDate, 'localtime') AS LocalCreatedDate, ServiceName, ServiceCategoryName, datetime(CompletedDate, 'localtime') AS LocalCompletedDate, DaysOld, ResolveByDate FROM tickets")

    for ID, AccountName, TypeCategoryName, TypeName, LocalCreatedDate, ServiceName, ServiceCategoryName, LocalCompletedDate, DaysOld, ResolveByDate in cursor.fetchall():
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
        new_row.setDateTime(4, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )
        new_row.setString(5, ServiceName)
        new_row.setString(6, ServiceCategoryName)
        
        d = parser.parse(LocalCompletedDate)
        new_row.setDateTime(7, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

        d = parser.parse(ResolveByDate)
        new_row.setDateTime(8, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )

        new_row.setInteger(9, DaysOld)

        new_table.insert(new_row)

    # Close the extract in order to save the .tde file and clean up resources
    new_extract.close()
    os.chdir(bindir)


def dailyextract(cursor):
    print "Building Tableau Daily Extract"
    os.chdir('data')
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
    minraw = cursor.execute('SELECT date(MIN(CreatedDate), "localtime") FROM tickets').fetchone()[0]
    if minraw:
        mindate = parser.parse(minraw)
    else:
        mindate = parser.parse(earliestdate)

    maxraw = cursor.execute('SELECT date(MAX(CreatedDate), "localtime") FROM tickets').fetchone()[0]
    if maxraw:
        maxdate = parser.parse(maxraw)
    else:
        maxdate = datetime.now()

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
            

            d = parser.parse(LocalCompletedDate)
            new_row.setDateTime(7, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )
            d = loopdate
            new_row.setDateTime(8, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond/100 )
            new_table.insert(new_row)

        loopdate = loopdate + timedelta(days=1)

    # Close the extract in order to save the .tde file and clean up resources
    new_extract.close()
    os.chdir(bindir)
    
def displaychoices(cats):
	i = 0
	print "\nPress q when done selecting"	
	for cat in cats:
		if cat[1]:
			print u"\u2611" + " " + str(i) + ". " + cat[0]
		else:
			print u"\u2610" + " " + str(i) + ". " + cat[0]
		i += 1

def uploadquiz(cats, unit, desc):
    startstr = "| Which of these Categories refers to "
    fullstr = startstr + unit + " (" + desc + ")? |"
    strlen = len(fullstr)
    secstr = "Press q when done selecting"
    secstrlen = len(secstr) + 4
    buflen = int((strlen - secstrlen) / 2)
    
    flsecstr = "| " + " "*buflen + secstr + " "*(strlen-(secstrlen+buflen)) + " |"
    headfoot = "-" * strlen

    print "\n" + headfoot
    print fullstr
    print flsecstr
    print headfoot
    
    ansarray = []
    ans = ""
    displaychoices(cats)
    numchoices = len(cats)
    while True:
        ans = readchar.readkey()
        if ans == 'q':
        	break
        elif ans.isdigit():
            ansint = int(ans)
            if ansint < numchoices:         
				cats[ansint][1] = not cats[ansint][1]
				displaychoices(cats)
        #elif ans in 'q':
        #	break
#        elif ans == '0x03'
#			sys.exit(0)
        #else:
        #	print ans
        #	break
    
    #print "......"
    #print u"\u2611"
    #print ans
    #return int(ans)

def uploadsubset(conn):
    print "\n"
    print "Hi!\n"
    print "** Would you like to opt in to sharing some aggregate data? (Totally FERPA safe – just basic stats!) **\n"
    
    while True:
        helpful = readchar.readkey()
        if helpful == 'y':
            print "\nYay!\n"
            break
        if helpful == 'n':
            print "\nOkay :'(\n"
            return

    # first we get all the type categories
    catsql = 'SELECT DISTINCT TypeCategoryName FROM tickets'
    c.execute(catsql)
    catret = c.fetchall()
    # build a pristine list with everything set to false
    cats = []
    for cat in catret:
        cats.append([cat[0], False])
    # Then copy the list for each grouping

    # first, incidents
    inccats = copy.deepcopy(cats)
    uploadquiz(inccats, "Incidents", "Worked yesterday, but not today")

    # then, SRs
    srcats = copy.deepcopy(cats)
    uploadquiz(srcats, "Service Requests", "Stuff you want, but it's not our fault")

    # then, Changes
    ccats = copy.deepcopy(cats)
    uploadquiz(ccats, "Changes", "Official Changes")

    #print cats[ans]
    #icat = cats[uploadquiz(cats, "Incidents", "broken things")]
    #print icat


def doextracts(threadname, threadno):
    if threadname:
        None
    tconn = sqlite3.connect('data/td2.db')
    tc = tconn.cursor()
    
    dailyextract(tc)
    tconn.commit()
    tconn.close()

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise    

def zipfolder(foldername, target_dir):            
    zipobj = zipfile.ZipFile(foldername + '.twbx', 'w', zipfile.ZIP_DEFLATED)
    rootlen = len(target_dir) + 1
    for base, dirs, files in os.walk(target_dir):
        for file in files:
            fn = os.path.join(base, file)
            zipobj.write(fn, fn[rootlen:])



def makemagic():
    with zipfile.ZipFile('dist/TDAnalysis.twbx', "r") as z:
        z.extractall("data/pytmp")
        z.close()
    try:
        os.remove("data/pytmp/Data/data/alltickets.tde")
    except OSError:
        pass
    try:
        os.remove("data/pytmp/Data/data/dailyopen.tde")
    except OSError:
        pass
    #try:
    os.rename("dist/TDAnalysis.twb", "data/pytmp/TDAnalysis.twb")
    os.rename("data/dailyopen.tde", "data/pytmp/Data/data/dailyopen.tde")
    os.rename("data/alltickets.tde", "data/pytmp/Data/data/alltickets.tde")
    #except OSError:
    #    print "Serious OS error. Exiting\n"
    #    sys.exit(1)

    try:
        os.remove("TDAnalysis.twbx")
    except OSError:
        pass
    zipfolder('TDAnalysis', 'data/pytmp') #insert your variables here
    sys.exit()  


def prepdist():
    # Grab the latest version of TDAnalysis.twb from GitHub

    make_sure_path_exists('dist')
    twbxtemplate = 'dist/TDAnalysis.twbx'

    try:
        print "Grabbing latest template from GitHub"
        r = requests.get('https://raw.githubusercontent.com/coopermj/TeamDynamixFun/master/dist/TDAnalysis.twbx')
        if 200 != r.status_code:
            print('Could not download template – HTTP Status Code: {status_code}'.format(status_code=response.status_code))
            sys.exit(1)
        f = open(twbxtemplate, 'w')
        f.write(r.content)
        f.close()
        r.close()
    except:
        print('HTTP Request failed')
        sys.exit(1)

    try:
        with zipfile.ZipFile(twbxtemplate, 'r') as z:
            z.extractall('data/template')
    except:
        print ('extraction failed')

    # copy twb for manipulation
    srcfile = 'data/template/TDAnalysis.twb'
    shutil.copy(srcfile,'dist/TDAnalysis.twb')

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

# Initial setup operations
make_sure_path_exists('data')
conffile = 'data/config.yml'
try:
    config = yaml.safe_load(open(conffile))
except:
    config = None
token = doconfig(config)
conn = sqlite3.connect('data/td2.db')

prepdist()

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
            ResolveByDate INT,
            ResponsibleGroupName TEXT,
            ServiceName TEXT,
            ServiceCategoryName TEXT,
            CompletedDate INT,
            DaysOld INT)''')

# c.execute('SELECT MAX(runStart) FROM tdruns WHERE runEnd IS NOT NULL')
now = datetime.now()
rec = ['getTickets', now]
c.execute('INSERT INTO tdruns(proc, runStart) VALUES(?,?)', rec)
conn.commit()

trackdate = getlast(c)
wegood = getData(token, c, trackdate)
#wegood = True # for debugging
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
conn.commit()

updatelabels(domain)

#thread.start_new_thread(doextracts, ("doextract",1))
#threads = []
basicextract(c)
conn.commit()

#t = Thread(target=doextracts, args=("doextract",0))
#t.start()
#t.join()
dailyextract(c)
conn.commit()
c.close()

#uploadsubset(c)

# do the file switcharoo
makemagic()

conn.close()

