# coding: utf-8

# This script gathers data on restaurant inspections by the state of Florida
# from the state's website and updates a local database. The information
# is used by another script to build a report for publication.

# Script by Douglas Ray, doug.ray@gainesville.com, updated 10/05/2018

# Requires access to rinspect.sqlite

#installed with pip
from pandas import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from email.message import EmailMessage

# built-in libraries
import csv
import datetime
import os.path
import re
import smtplib
import sqlite3

# DATAFRAME built from summary of recent inspections
# Read state summary report for District into Pandas df;
# filter for needed fields and assign headers
cols = ["county", "licnum", "sitename", "streetaddy", "cityaddy", "zip",
    "inspnum", "insptype", "inspdispos", "inspdate", "totalvio", "highvio",
    "licid", "visitid"]

insp1 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/1fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")
insp2 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/2fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")
insp3 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/3fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")

insp4 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/4fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")

insp5 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/5fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")

insp6 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/6fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")

insp7 = pd.read_csv("ftp://dbprftp.state.fl.us/pub/llweb/7fdinspi.csv",
                    usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                    names=cols,
                    dtype=object,
                    encoding="ISO-8859-1")

insp_list = [insp1, insp2, insp3, insp4, insp5, insp6, insp7]
insp = pd.concat(insp_list, axis=0)

#Clean up some of the data before storing it in the db
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
insp['visitid'] = insp['visitid'].apply(int) # so it can be filtered against df
insp.inspdate = pd.to_datetime(insp.inspdate)
insp.inspdate = insp.inspdate.dt.strftime('%Y, %m, %d')

# READ IN records from database of earlier reports, create df to filter against
# new reports in df above.
#db_directory = os.path.dirname(os.path.abspath(__file__))
#db_file = os.path.join(db_directory, "rinspect.sqlite")
db_file = "rinspect.sqlite"
conn = sqlite3.connect(db_file)
df = pd.read_sql_query("select * from fdinsp;", conn)
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
fdinsp_table = 'fdinsp' # table for summary data
id_field = 'visitid' # ID column
time_now = 'now' # column for user-input timestamp; until then = NULL

# connect to database and insert new summary report data
conn = sqlite3.connect(db_file,detect_types=sqlite3.PARSE_DECLTYPES)
c = conn.cursor()
c.executemany('''INSERT OR IGNORE INTO fdinsp (librow, county, licnum, sitename,
              streetaddy, cityaddy, zip, inspnum, insptype, inspdispos,
              inspdate, totalvio, highvio, licid, visitid)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', var)
conn.commit()

# VIOLATIONS from scrape of indiviual reports go into another table

def make_obs():
    visitid = url.split("VisitID=")[1].split("&")[0]
    visitid = str(visitid)
    licid = url.split("&id=")[1]
    licid = str(licid)
    html = urlopen(url)
    url_error = "https://www.myfloridalicense.com/inspectionDetail.asp?InspVisitID={:s}&id={:s}".format(visitid, licid)
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all( "table" )
    #log_directory = os.path.dirname(os.path.abspath(__file__))
    #log_file = os.path.join(log_directory, "db_update_log.txt")
    #db_directory = os.path.dirname(os.path.abspath(__file__))
    #db_file = os.path.join(db_directory, "rinspect.sqlite")
    log_file = "db_update_log.txt"
    db_file = "rinspect.sqlite"

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    try:
        rows = tables[16].find_all( "tr" )
    except IndexError:
        with open(log_file,'a') as outFile:
            outFile.write("""\n***Problem gathering from {:s}\n""".format(url_error))
            c.execute("""DELETE FROM fdinsp WHERE visitid = '{}' """.format(visitid))
            conn.commit()
    # get cells we want from each row - note, middle cell is empty
    else:
        for row in rows:
            cells = row.find_all( "td" )
            violation = cells[0].get_text().strip()
            if violation != "Violation":
                obs = cells[2].get_text().strip().strip("\n")
                # from link, get numeric code to open description page
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

# append to log that goes out by email with each run
log_directory = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_directory, "db_update_log.txt")
# log_file = "db_update_log.text"

with open(log_file,'a') as outFile:
    outFile.write('\n' + 'Scrape: ' + str(datetime.datetime.now()))

# LOG each run
with open(log_file,'a') as outFile:
    val_text = ' -- with {} new records added\n'.format(new_vals)
    outFile.write('\n' + 'Run complete: ' + str(datetime.datetime.now()) + '\n' + val_text + '\n')

# SEND Log
receiver = 'doug.ray@starbanner.com'
with open(log_file) as fp:
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
