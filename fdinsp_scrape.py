#installed with pip
from pandas import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from email.message import EmailMessage

# built-in libraries
import datetime
import re
import sqlite3
import smtplib
import os

#from datetime import datetime
today = datetime.today()
weekday = today.weekday()

if (weekday == 1):
    import fdinsp_db_updater