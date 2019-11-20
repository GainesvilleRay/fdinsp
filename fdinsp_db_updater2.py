# Requires access to rinspect.sqlite and creds.py

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

# local library
import creds

def read_summaries(district):
    """
    Read in summaries of inspection reports into a Pandas dataframe.
    """
    # Just use the columns we need
    cols = ["county", "licnum", "sitename", "streetaddy", "cityaddy", "zip",
    "inspnum", "insptype", "inspdispos", "inspdate", "totalvio", "highvio",
    "licid", "visitid"]

    try:
        insp = pd.read_csv(district,
                            usecols=[2, 4, 5, 6, 7, 8, 9, 12, 13, 14, 17, 18, 80, 81],
                            names=cols,
                            dtype=object,
                            encoding="ISO-8859-1"
                          )
    except FileNotFoundError:
        msg = "Sorry, the csv file for" + district + "was not found."
        print(msg)
    else:
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

    return insp

def joined_df():
    """
    Loop through all districts in the state to build the summaries
    dataframe.
    """

    district1 = f"ftp://dbprftp.state.fl.us/pub/llweb/1fdinspi.csv"
    district2 = f"ftp://dbprftp.state.fl.us/pub/llweb/2fdinspi.csv"
    district3 = f"ftp://dbprftp.state.fl.us/pub/llweb/3fdinspi.csv"
    district4 = f"ftp://dbprftp.state.fl.us/pub/llweb/4fdinspi.csv"
    district5 = f"ftp://dbprftp.state.fl.us/pub/llweb/5fdinspi.csv"
    district6 = f"ftp://dbprftp.state.fl.us/pub/llweb/6fdinspi.csv"
    district7 = f"ftp://dbprftp.state.fl.us/pub/llweb/7fdinspi.csv"

    all_districts = [
        district1,
        district2,
        district3,
        district4,
        district5,
        district6,
        district7
    ]

    for district in all_districts:
        insp_list = []
        insp = read_summaries(district)
        insp_list.append(insp)
        df_insp = pd.concat(insp_list, axis=0)

    return df_insp

def create_filter():
    """
    Build a list urls to the inspectors' detailed reports that will get scraped.
    Read in records from database of earlier reports, create df to filter against
    new reports in df above.
    """

    #db_directory = os.path.dirname(os.path.abspath(__file__))
    #db_file = os.path.join(db_directory, "rinspect.sqlite")
    db_file = "rinspect.sqlite"
    conn = sqlite3.connect(db_file)
    df_insp = joined_df() # Access result from function above
    df = pd.read_sql_query("select * from fdinsp;", conn) # Get old info
    unique_vals = df_insp[~df_insp.visitid.isin(df.visitid)] # Filter
    conn.close()

    # Build list of urls for detailed reports
    # Takes LicenseID and VisitID, passes it into the urls for detailed reports later
    result = []
    for index, rows in unique_vals.iterrows():
        visitid = rows['visitid']
        licid = rows['licid']
        urls = f"https://www.myfloridalicense.com/inspectionDetail.asp?InspVisitID={visitid}&id={licid}"
        urls = urls.replace(' ', '')
        result.append(urls)
    urlList = result
    urlList.pop(0) # get rid of first "Null" from append above

    """
    Put new summary report info into the database.

    Would like to make this a separate     function, but unique_vals
    gets returned by function as tuple rather than a dataframe, for some reason.
    I saw something on Stack Overflow that makes me think
    it may be a bug related to timestamps in dataframes.
    """

    unique_vals = create_filter() # Access result from function above
    var = list(unique_vals.itertuples(index='visitid', name=None))

    db_file = "rinspect.sqlite"
    conn = sqlite3.connect(db_file,detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    c.executemany('''INSERT OR IGNORE INTO fdinsp (librow, county, licnum, sitename,
                  streetaddy, cityaddy, zip, inspnum, insptype, inspdispos,
                  inspdate, totalvio, highvio, licid, visitid)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', var)
    conn.commit()
    conn.close()
    c.close()

    return unique_vals, urlList

def insert_summaries():
    """
    Put new summary report info into the database.
    """

    unique_vals = create_filter() # Access result from function above
    var = list(unique_vals.itertuples(index='visitid', name=None))

    db_file = "rinspect.sqlite"
    conn = sqlite3.connect(db_file,detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    c.executemany('''INSERT OR IGNORE INTO fdinsp (librow, county, licnum, sitename,
                  streetaddy, cityaddy, zip, inspnum, insptype, inspdispos,
                  inspdate, totalvio, highvio, licid, visitid)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', var)
    conn.commit()
    conn.close()
    c.close()

def make_obs(url):
    """
    Function runs for each url in urlList to scrape detailed info
    from individual reports that goes into another table.
    """

    visitid = url.split("VisitID=")[1].split("&")[0]
    visitid = str(visitid)
    licid = url.split("&id=")[1]
    licid = str(licid)
    html = urlopen(url)
    url_error = f"https://www.myfloridalicense.com/inspectionDetail.asp?InspVisitID={visitid}&id={licid}"
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
            outFile.write(f"\n***Problem gathering from {url_error}\n")
            c.execute(f"DELETE FROM fdinsp WHERE visitid = '{visitid}' ")
            conn.commit()
    except requests.Timeout as e:
        print("OOPS! Timeout Error")
        print(str(e))
    except KeyboardInterrupt:
        print("Someone closed the program")
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
                conn.close()
                c.close()

def write_email():
    """
    Write an email confirming the run went smoothly, or logging errors
    """

    #log_directory = os.path.dirname(os.path.abspath(__file__))
    #log_file = os.path.join(log_directory, "db_update_log.txt")
    log_file = "db_update_log.text"

    with open(log_file,'a') as outFile:
        outFile.write('\n' + 'Scrape: ' + str(datetime.datetime.now()))

    # LOG each run
    with open(log_file,'a') as outFile:
        val_text = ' -- with {} new records added\n'.format(new_vals)
        outFile.write('\n' + 'Run complete: ' + str(datetime.datetime.now()) + '\n' + val_text + '\n')

    # SEND Log
    receivers = ['doug.ray@starbanner.com']
    with open(log_file) as fp:
        # Create a text/plain message
        msg = EmailMessage()
        msg.set_content(fp.read())

    sender = 'data@sunwriters.com'
    gmail_password = creds.gmail_password
    msg['Subject'] = 'Latest scrape'
    msg['from'] = sender
    msg['To'] = receivers

    # Send the message via our own SMTP server.
    unique_vals = create_filter() # Access result from function above
    new_vals = len(unique_vals) # How many new reports did we add?
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(sender, gmail_password)
        server.send_message(msg)
        server.quit()

        print('Email sent!')
        print(f'There are {new_vals} inspections in the new report.')
    except:
        print('Something went wrong...')

if __name__ == "__main__":
    urlList = create_filter()
    insert_summaries()
    for url in urlList:
        make_obs()
    write_email()
