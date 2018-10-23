""" This script gathers data on restaurant inspections by the state of Florida
that have been collected into a database by a related script
and builds a txt report for a particular county within a date range.

Script by Douglas Ray, doug.ray@gainesville.com, updated 10/22/2018
with help from Mike Stucka, Palm Beach Post, and Mindy McAdams, Univ. of Florida

Requires access to rinspect.sqlite and insptypes.csv """

#Python standard libraries
import csv
import datetime
from email.message import EmailMessage
import os
import smtplib
import sqlite3

# COUNTY for inspection reports; unselect the one you want
#countywanted = 'Marion'
#countywanted = 'Alachua'
countywanted = 'Polk'
#countywanted = 'Sarasota'
#countywanted = 'Manatee'

# DATE RANGE for report, week prior
today = datetime.date.today()
idx = (today.weekday() + 1) % 7
start_day = today - datetime.timedelta(6+idx) # Monday prior week; date object
start_date = str(start_day.strftime("%Y, %m, %d")) # Monday; date as string
end_day = today - datetime.timedelta(1+idx) # following Saturday; date object
end_date = str(end_day.strftime("%Y, %m, %d")) # Saturday; date as string
pn_startdate = start_day.strftime("%b. %d") # date as string for email narrative
pn_enddate = end_day.strftime("%b. %d") # date as string for email narrative

# Who gets the report:
if countywanted == 'Marion':
    receiver = ['doug.ray@starbanner.com', 'joe.byrnes@gvillesun.com', \
    'alan.youngblood@starbanner.com']
elif countywanted == 'Alachua':
    receiver = ['doug.ray@starbanner.com']
elif countywanted == 'Polk':
    receiver = ['doug.ray@starbanner.com']#, 'laura.davis@theledger.com']
elif countywanted == 'Sarasota':
    receiver = ['doug.ray@starbanner.com', 'brian.ries@heraldtribune.com']
elif countywanted == 'Manatee':
    receiver = ['doug.ray@starbanner.com', 'brian.ries@heraldtribune.com']

# The new report and, later, its path:
path_directory = os.path.dirname(os.path.abspath(__file__))
bigreport = os.path.join(path_directory, 'bigreport.txt')

# Access database
sqlite_file = os.path.join(path_directory, 'rinspect.sqlite')
conn = sqlite3.connect('rinspect.sqlite')
conn.row_factory = lambda cursor, row: row[0]
c = conn.cursor()

# Ordered by severity, most high-priority violations first; choose later
ids_vio = c.execute("""SELECT visitid
    FROM fdinsp WHERE county = '{}'
    AND inspdate BETWEEN '{}' AND '{}'
    ORDER BY highvio DESC, totalvio DESC
    """.format(countywanted, start_date, end_date)).fetchall()

# Ordered by date, most recent first; choose later
ids_date_desc = c.execute("""SELECT visitid
    FROM fdinsp WHERE county = '{}'
    AND inspdate BETWEEN '{}' AND '{}'
    ORDER BY inspdate DESC, highvio DESC, totalvio DESC
    """.format(countywanted, start_date, end_date)).fetchall()

# Ordered by date, Monday to Saturday; choose later
ids_date_asc = c.execute("""SELECT visitid
    FROM fdinsp WHERE county = '{}'
    AND inspdate BETWEEN '{}' AND '{}'
    ORDER BY inspdate ASC, highvio DESC, totalvio DESC
    """.format(countywanted, start_date, end_date)).fetchall()

reportnum = len(ids_vio)

# CSV FILE as sort of a mini module to streamline narrative
insptypedict = {}
insptypes = os.path.join(path_directory, 'insptypes.csv')
with open(insptypes, "r") as csvfile:
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
        stamp += "Sept. " +  datetimedatetime.strftime(date_object, "%d, %Y").lstrip("0")
    elif date_object.month < 3 or date_object.month > 7:
        stamp += datetime.datetime.strftime(date_object, "%b. ") \
        + datetime.datetime.strftime(date_object, "%d").lstrip("0")
    else:
        stamp += datetime.datetime.strftime(date_object, "%B ") \
        + datetime.datetime.strftime(date_object, "%d").lstrip("0")
    # uncomment out below if you want the year
    #stamp += datetime.datetime.strftime(date_object, ", %Y")
    # uncomment below if you want the time
    # stamp += ", at "
    # stamp += datetime.datetime.strftime(date_object, "%I:%M %p").lstrip("0").replace("AM", "a.m.").replace("PM", "p.m.")
    return(stamp)

# MAIN FUNCTION to pull inspection report data from db and build narrative
def clean_report(id):
    db_directory = os.path.dirname(os.path.abspath(__file__))
    sqlite_file = os.path.join(db_directory, 'rinspect.sqlite')
    table_name1 = 'fdinsp'
    table_name2 = 'violations'

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table_name1} WHERE visitid = {id}")
    data = c.fetchall()
    sitename = str([x[3] for x in data]).strip("['']")
    streetaddy = str([x[4] for x in data]).strip("['']")
    cityaddy = str([x[5] for x in data]).strip("['']")
    insptype = str([x[8] for x in data]).strip("['']")
    inspdispos = str([x[9] for x in data]).strip("['']")
    inspdate = str([x[10] for x in data]).strip("['']")
    inspdate = str(datetime.datetime.strptime(inspdate, ('%Y, %m, %d')).date())
    inspdate = (datetime.datetime.strptime(inspdate, ('%Y-%m-%d')).date())
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
    vn = "" # violations narrative
    bn = "" # basic violations narrative
    cn = "" # intermediate violatins narrative
    hn = "" # high-priority violations narrative
    kn = "" # unknown violations narrative

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    obs = c.execute("SELECT obs FROM '{}' WHERE visitid = '{}'"\
    .format(table_name2, id))
    vios = c.fetchall()
    for vio in vios:
        if "'Basic" in str(vio):
            basicvio = str(vio)
            bn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'")\
            .replace('\\n', ' ')
            " ".join(bn.split())
            bn += "\n"
        elif "'Intermediate" in str(vio):
            intervio = str(vio)
            cn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'")\
            .replace('\\n', ' ')
            " ".join(cn.split())
            cn += "\n"
        elif "'High Priority" in str(vio):
            highvio = str(vio)
            hn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'")\
            .replace('\\n', ' ')
            " ".join(hn.split())
            hn += "\n"
        else:
            unkvio = str(vio)
            vn += "-- " + str(vio).strip("('')").rstrip(",").rstrip("'")\
            .replace('\\n', ' ')
            " ".join(kn.split())
            kn += "\n"
    vn = hn + cn + bn + kn
    pn += vn

    return(pn)

# Delete old report file since we'll be building a new one here.
if os.path.exists(bigreport):
    os.remove(bigreport)
else:
    print(f"The old file for {bigreport} isn't there.")

intro = f"""These are recent restaurant inspection reports for {countywanted} County — from {pn_startdate} to {pn_enddate} — filed by state safety and sanitation inspectors.\nThe Florida Department of Business & Professional Regulation describes an inspection report as “a ‘snapshot’ of conditions present at the time of the inspection. On any given day, an establishment may have fewer or more violations than noted in their most recent inspection. An inspection conducted on any given day may not be representative of the overall, long-term conditions at the establishment.\nPlease note that some more recent, follow-up inspections may not be included here.\n"""
f= open(bigreport,'a+')
f.write(intro)

# CALL MAIN function, create big report
ids = ids_vio # to order reports by severity of violations
#ids = ids_date_desc # to order reports by date, most recent first
#ids = ids_date_asc # to order reports by date, Monday to Saturday
for id in ids:
    pn = ""
    clean_report(id)
    # append pn to text file
    f= open(bigreport,"a")
    f.write(pn)
    f.close()
conn.close()

# SEND REPORT to recipient(s).
with open(bigreport) as fp:
    # Create a text/plain message
    msg = EmailMessage()
    msg.set_content(fp.read())

sender = 'data@sunwriters.com'
gmail_password = '%WatchingTheDetectives'
msg['Subject'] = f'Latest restaurant inspection report for {countywanted}'
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
    print(f'There are {reportnum} inspections in the new report.')
except:
    print('Something went wrong...')
