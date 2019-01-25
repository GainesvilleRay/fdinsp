#!/usr/bin/python3.6

import datetime

today = datetime.date.today()
weekday = today.weekday()

if (weekday == 0): #Monday
    import fdinsp_db_updater
