
# coding: utf-8

# This script gathers data on restaurant inspections by the state of Florida
# that have been collected into a database by a related script
# and builds a txt report for a particular county within a date range.

# Script by Douglas Ray, doug.ray@gainesville.com, updated 09/17/2018
# with help from Mike Stucka, Palm Beach Post, and Mindy McAdams, Univ. of Florida

# Requires access to rinspect.sqlite and insptypes.csv

from datetime import datetime
from datetime import timedelta
from datetime import date
import sqlite3
import csv
import re
import smtplib
import os
from email.message import EmailMessage

# CHOOSE date range and county, used to build a list of visitid numbers included in report
#start_date = input("What is our start date? ") #'2018, 09, 04' format
#end_date = input("What is your end date? ") #'2018, 09, 14' format
today = datetime.today()
start_delta = timedelta(days=9)
priorweek = today - start_delta
end_date = today.strftime('%Y, %m, %d')
start_date = priorweek.strftime('%Y, %m, %d')

countywanted = 'Marion'
# replace with the county you want
# Who gets the report:
receiver = 'doug.ray@starbanner.com' # add single address or create list of more

# The new report and, later, its path:
path = 'bigreport.txt'

# Access database
sqlite_file = 'rinspect.sqlite'
conn = sqlite3.connect(sqlite_file)
conn.row_factory = lambda cursor, row: row[0]
c = conn.cursor()

# Ordered by severity, most high-priority violations first; choose later
ids_vio = c.execute("""SELECT visitid
    FROM fdinsp WHERE county = '{}'
    AND inspdate BETWEEN '{}' AND '{}'
    ORDER BY highvio DESC, totalvio DESC
    """.format(countywanted, start_date, end_date)).fetchall()

# Ordered by date, most recent first; choose later
ids_date = c.execute("""SELECT visitid
    FROM fdinsp WHERE county = '{}'
    AND inspdate BETWEEN '{}' AND '{}'
    ORDER BY inspdate DESC, highvio DESC, totalvio DESC
    """.format(countywanted, start_date, end_date)).fetchall()

reportnum = len(ids_date)

# CSV FILE as sort of a mini module to streamline narrative; courtesy of Mike Stucka, Palm Beach Post
insptypedict = {}
with open("insptypes.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        insptypedict[row["inspdisposition"]] = row["text"]

# CONVERTS datetime into AP style text; from Stucka
def get_big_timestamp(date_object=None):
    if not date_object:
        date_object = datetime.datetime.now()
    stamp = ""
    # comment out below if you don't want "Wednesday" or similar in your string
    #stamp += datetime.datetime.strftime(date_object, "%A, ")
    if date_object.month == 9:
        stamp += "Sept. " +  datetime.strftime(date_object, "%d, %Y").lstrip("0")
    elif date_object.month < 3 or date_object.month > 7:
        stamp += datetime.strftime(date_object, "%b. ") + datetime.strftime(date_object, "%d").lstrip("0")
    else:
        stamp += datetime.strftime(date_object, "%B ") + datetime.strftime(date_object, "%d").lstrip("0")
    # uncomment out below if you want the year
    #stamp += datetime.strftime(date_object, ", %Y")
    # uncomment below if you want the time
    # stamp += ", at "
    # stamp += datetime.datetime.strftime(date_object, "%I:%M %p").lstrip("0").replace("AM", "a.m.").replace("PM", "p.m.")
    return(stamp)

# MAIN FUNCTION to pull inspection report data from db and build narrative
def clean_report(id):
    sqlite_file = 'rinspect.sqlite'
    table_name1 = 'fdinsp'
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    visitid = id
    c.execute("SELECT * FROM '{}' WHERE visitid = '{}'".              format(table_name1, visitid))
    data = c.fetchall()
    sitename = str([x[3] for x in data]).strip("['']")
    streetaddy = str([x[4] for x in data]).strip("['']")
    cityaddy = str([x[5] for x in data]).strip("['']")
    insptype = str([x[8] for x in data]).strip("['']")
    inspdispos = str([x[9] for x in data]).strip("['']")
    inspdate = str([x[10] for x in data]).strip("['']")
    inspdate = str(datetime.strptime(inspdate, ('%Y, %m, %d')).date())
    #inspdate = inspdate.strftime('%B %d, %Y') #comment out if using big_timestamp
    inspdate = (datetime.strptime(inspdate, ('%Y-%m-%d')).date())
    totalvio = str([x[11] for x in data]).strip("['']")
    highvio = str([x[12] for x in data]).strip("['']")

    # Start building narrative; based on concept and foundation from Stuckya
    addy = str(streetaddy) + ", " + str(cityaddy)
    if "Routine" in str(insptype):
        insptype = "routine"
    elif "Licensing" in str(insptype):
        insptype = "licensing"
    elif "Complaint" in insptype:
        insptype = "complaint"
    else:
        insptype = "unknown"
    global pn
    pn = "\n"
    pn += str(sitename).strip('"') + ", "
    pn += addy + ", had a " + insptype + " inspection "
    #pn += inspdate + ". " #comment out if using big_timestamp
    pn += get_big_timestamp(inspdate) + ". "
    pn += insptypedict[inspdispos]
    if str(totalvio) == "0":
        pn += " No violations were found.\n"
    elif str(totalvio) == "1":
        pn += " One violation, with "
        if str(highvio) == "0":
            pn += "no high-priority violations.\n"
        else:
            pn += "one high-priority violation:\n"
    elif str(totalvio) == "2":
        pn += " Two total violations, with "
        if str(highvio) == "0":
            pn += "no high-priority violations.\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "both as high-priority violations:\n"
    elif str(totalvio) == "3":
        pn += " Three total violations, with "
        if str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    elif str(totalvio) == "4":
        pn += " Four total violations, with "
        if str(highvio) == "4":
            pn += "four high-priority violations:\n"
        elif str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    elif str(totalvio) == "5":
        pn += " Five total violations, with "
        if str(highvio) == "5":
            pn += "five high-priority violations:\n"
        elif str(highvio) == "4":
            pn += "four high-priority violations:\n"
        elif str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    elif str(totalvio) == "6":
        pn += " Six total violations, with "
        if str(highvio) == "6":
            pn += "six high-priority violations:\n"
        elif str(highvio) == "5":
            pn += "five high-priority violations:\n"
        elif str(highvio) == "4":
            pn += "four high-priority violations:\n"
        elif str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    elif str(totalvio) == "7":
        pn += " Seven total violations, with "
        if str(highvio) == "7":
            pn += "seven high-priority violations:\n"
        elif str(highvio) == "6":
            pn += "six high-priority violations:\n"
        elif str(highvio) == "5":
            pn += "five high-priority violations:\n"
        elif str(highvio) == "4":
            pn += "four high-priority violations:\n"
        elif str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    elif str(totalvio) == "8":
        pn += " Eight total violations, with "
        if str(highvio) == "8":
            pn += "eight high-priority violations:\n"
        elif str(highvio) == "7":
            pn += "seven high-priority violations:\n"
        elif str(highvio) == "6":
            pn += "six high-priority violations:\n"
        elif str(highvio) == "5":
            pn += "five high-priority violations:\n"
        elif str(highvio) == "4":
            pn += "four high-priority violations:\n"
        elif str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    elif str(totalvio) == "9":
        pn += " Nine total violations, with "
        if str(highvio) == "9":
            pn += "nine high-priority violations:\n"
        elif str(highvio) == "8":
            pn += "eight high-priority violations:\n"
        elif str(highvio) == "7":
            pn += "seven high-priority violations:\n"
        elif str(highvio) == "6":
            pn += "six high-priority violations:\n"
        elif str(highvio) == "5":
            pn += "five high-priority violations:\n"
        elif str(highvio) == "4":
            pn += "four high-priority violations:\n"
        elif str(highvio) == "3":
            pn += "three high-priority violations:\n"
        elif str(highvio) == "2":
            pn += "two high-priority violations:\n"
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += "no high-priority violations.\n"
    else:
        pn += " " + str(totalvio) + " total violations, with "
        if str(highvio) == "0":
            pn += "no high-priority violations."
        elif str(highvio) == "1":
            pn += "one high-priority violation:\n"
        else:
            pn += str(highvio) + " high-priority violations:\n"

    # GET VIOLATIONS from db, rank by severity, cleanup & build narrative
    sqlite_file = 'rinspect.sqlite'
    table_name2 = 'violations'
    vn = "" # violations narrative
    bn = "" # basic violations narrative
    cn = "" # intermediate violatins narrative
    hn = "" # high-priority violations narrative
    kn = "" # unknown violations narrative

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    obs = c.execute("""SELECT obs FROM '{tn}'
        WHERE visitid = '{vi}' """.format(tn=table_name2, vi=visitid))
    vios = c.fetchall()
    for vio in vios:
        if "'Basic" in str(vio):
            basicvio = str(vio)
            bn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'").replace('\\n', ' ')
            " ".join(bn.split())
            bn += "\n"
        elif "'Intermediate" in str(vio):
            intervio = str(vio)
            cn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'").replace('\\n', ' ')
            " ".join(cn.split())
            cn += "\n"
        elif "'High Priority" in str(vio):
            highvio = str(vio)
            hn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'").replace('\\n', ' ')
            " ".join(hn.split())
            hn += "\n"
        else:
            unkvio = str(vio)
            vn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'").replace('\\n', ' ')
            " ".join(kn.split())
            kn += "\n"
    vn = hn + cn + bn + kn
    pn += vn

    return(pn)

# Delete old report file since we'll be building a new one here.
if os.path.exists(path):
    os.remove(path)
else:
    print("The old file for {} isn't there.".format(path))

# Add intro graph to bigreport, with date of last inspection included
intro_date = "Oct. 4"
end_date = "Oct. 13"


intro = """These are recent restaurant inspection reports for {} County — from {} to {} — filed by state safety and sanitation inspectors.\nThe Florida Department of Business & Professional Regulation describes an inspection report as “a ‘snapshot’ of conditions present at the time of the inspection. On any given day, an establishment may have fewer or more violations than noted in their most recent inspection. An inspection conducted on any given day may not be representative of the overall, long-term conditions at the establishment.\nPlease note that some more recent, follow-up inspections may not be included here.\n""".format(countywanted, intro_date, end_date)

f= open(path,'a+')
f.write(intro)

# CALL MAIN function, create big report
ids = ids_vio # to order reports by severity of violations
#ids = ids_date # to order reports by date, most recent in the range first
for id in ids:
    pn = ""
    clean_report(id)
    # append pn to text file
    f= open(path,"a")
    f.write(pn)
    f.close()
conn.close()

# SEND REPORT to recipient(s).
with open(path) as fp:
    # Create a text/plain message
    msg = EmailMessage()
    msg.set_content(fp.read())

sender = 'data@sunwriters.com'
gmail_password = '%WatchingTheDetectives'
msg['Subject'] = 'Latest restaurant inspection report for {}'.format(countywanted)
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
    print('There are {} inspections in the new report.'.format(reportnum))
except:
    print('Something went wrong...')
