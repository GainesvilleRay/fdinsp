# coding: utf-8

# This script gathers data on restaurant inspections by the state of Florida
# from the state's website and updates a local database. The information
# is used by another script to build a report for publication.

# User will need to select the district to update or and which county to report.
# At this point, it's all in code below. This is not a working copy.

# Script by Douglas Ray, doug.ray@gainesville.com, updated 10/02/2018

# Requires access to rinspect.sqlite

#installed with pip
from pandas import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from email.message import EmailMessage

# built-in libraries
import datetime
import re
import sqlite3
import urllib
import csv
import smtplib
import os

# append to log that goes out by email with each run
with open('db_update_log.txt','a') as outFile:
    outFile.write('\n' + 'Scrape run ' + str(datetime.datetime.now()))

# DATAFRAME built from summary of recent inspections
#county_sought = 'county' # replace with your county and uncomment
# Read state summary report for district into Pandas df;
# filter for needed fields and assign headersself.
# URL below needs to be changed to reflect district.
# Example: 1fdinsp.csv is District 1
insp1 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/1fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
insp2 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/2fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
insp3 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/3fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
insp4 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/4fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
insp5 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/5fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
insp6 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/6fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
insp7 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/7fdinspi.csv",
                   usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81], encoding="ISO-8859-1")
frames = [insp1, insp2, insp3, insp4, insp5, insp6, insp7]
insp = pd.concat(frames)
insp.columns = ["county", "licnum", "sitename", "streetaddy", "cityaddy", "zip",
                "inspnum", "insptype", "inspdispos", "inspdate", "totalvio", "highvio", "licid", "visitid"]

#insp = insp[(insp.county == county_sought)] # uncomment if particular county sought
insp.sitename = insp.sitename.str.title()
insp.sitename = insp.sitename.str.replace('Mcdonald\'s', 'McDonald\'s')
insp.sitename = insp.sitename.str.replace('Mcdonalds', 'McDonald\'s')
insp.sitename = insp.sitename.str.replace('Bbq', 'BBQ')
insp.sitename = insp.sitename.str.replace(r'\'S ', '\'s ')
insp.streetaddy = insp.streetaddy.str.title()
insp.streetaddy = insp.streetaddy.str.replace(' Sw ', ' SW ')
insp.streetaddy = insp.streetaddy.str.replace(' Se ', ' SE ')
insp.streetaddy = insp.streetaddy.str.replace(' Nw ', ' NW ')
insp.streetaddy = insp.streetaddy.str.replace(' Ne ', ' NE ')
insp.streetaddy = insp.streetaddy.str.replace(' Rd', ' Road')
insp.streetaddy = insp.streetaddy.str.replace(' Sr ', ' State Road ')
insp.streetaddy = insp.streetaddy.str.replace(' Ste ', ', Suite ')
insp.streetaddy = insp.streetaddy.str.replace(r'(?<=[4-9])Th ', 'th ')
insp.streetaddy = insp.streetaddy.str.replace(r'2Nd ', '2nd ')
insp.streetaddy = insp.streetaddy.str.replace(r'3Rd ', '3rd ')
insp.streetaddy = insp.streetaddy.str.replace(r' Us ', ' US ')
insp.cityaddy = insp.cityaddy.str.title()
insp = insp.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)
insp['visitid'] = insp['visitid'].apply(int) # so it can be filtered against df below
insp.inspdate = pd.to_datetime(insp.inspdate)
insp.inspdate = insp.inspdate.dt.strftime('%Y, %m, %d')

# READ IN records from database of earlier reports, create df to filter against new reports in df above.
db_file = "rinspect.sqlite"
conn = sqlite3.connect(db_file)
df = pd.read_sql_query("select * from fdinsp;", conn)
df
conn.close()
unique_vals = insp[~insp.visitid.isin(df.visitid)] #filter
new_vals = len(unique_vals)

# BUILD A LIST of URLs to the inspectors detailed reports that will get scraped
result = []
result_for_urls = result # for url list
result_for_list = result.append("NULL") # field for potential later user input

# takes LicenseID and VisitID, passes it into the urls for detailed reports later
for index, rows in unique_vals.iterrows():
    visitid = rows['visitid']
    licid = rows['licid']
    urls = "https://www.myfloridalicense.com/inspectionDetail.asp?InspVisitID={:d}&id={:s}".format(visitid, licid)
    urls = urls.replace(' ', '')
    result.append(urls)
urlList = result
urlList.pop(0) # get rid of first "Null" from append above

# PLACE DATA from state summary report df into database
# first, interate through the df to return tuples
var = list(unique_vals.itertuples(index='visitid', name=None))
# populate database table for inspection summmary reports
sqlite_file = 'rinspect.sqlite'
fdinsp_table = 'fdinsp' # table for summary data
id_field = 'visitid' # ID column
time_now = 'now' # column for user-input timestamp; until then = NULL

# connect to database and insert new summary report data
conn = sqlite3.connect(sqlite_file,detect_types=sqlite3.PARSE_DECLTYPES)
c = conn.cursor()
c.executemany('''INSERT OR IGNORE INTO fdinsp (librow, county, licnum, sitename,
              streetaddy, cityaddy, zip, inspnum, insptype, inspdispos,
              inspdate, totalvio, highvio, licid, visitid)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', var)
conn.commit()

# VIOLATIONS from scrape of indiviual reports go into another table
def make_obs():
    visitid = url.split("VisitID=")[1].split("&")[0]
    licid = url.split("&id=")[1]
    html = urlopen(url)
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all( "table" )
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    url_error = "http://www.myfloridalicense.com/InspectionDetail.asp?VisitID={:d}&licid={:s}".format(visitid, licid)
    try:
        rows = tables[16].find_all( "tr" )
    except IndexError:
        with open('db_update_log.txt','a') as outFile:
            outFile.write("***Problem gathering from {:s}".format(url_error))
            c.execute("""DELETE FROM fdinsp WHERE visit = '{}' """.format(visitid))
            conn.commit()

    # get cells we want from each row - note, middle cell is empty
    else:
        for row in rows:
            cells = row.find_all( "td" )
            violation = cells[0].get_text().strip()
            if violation != "Violation":
                obs = cells[2].get_text().strip().strip("\n")
                # from link, get numeric code to open description page for this violation
                popup = cells[0].find( "a" )
                p = re.compile("(\(')((.)*)('\))")
                m = p.search( str(popup) )
                details_id = m.group(2)
                values = (visitid, violation, details_id, obs)
                c.execute('''INSERT OR IGNORE INTO violations
                (visitid, violation, details_id, obs) VALUES (?,?,?,?)''', values)
                conn.commit()

for url in urlList:
    make_obs()

conn.close()

# LOG each run
with open('db_update_log.txt','a') as outFile:
    val_text = ' -- with {} new records added\n'.format(new_vals)
    outFile.write('\n' + 'Run complete: ' + str(datetime.datetime.now()))

# SEND Log
receiver = 'you@domain.com' # single address or list of more
path = 'db_update_log.txt'
with open(path) as fp:
    # Create a text/plain message
    msg = EmailMessage()
    msg.set_content(fp.read())

sender = 'data@sunwriters.com'
gmail_password = '%WatchingTheDetectives'
msg['Subject'] = 'Latest scrape'
msg['from'] = sender
msg['To'] = receiver

# Send the message via our own SMTP server.
try:
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    server.login(sender, gmail_password)
    server.send_message(msg)
    server.quit()

    print('Email sent!')
    print('There are {} inspections in the new report.'.format(new_vals))
except:
    print('Something went wrong...')
