#!/usr/bin/python3.6

import datetime

today = datetime.date.today()
weekday = today.weekday()

if (weekday == 7): #Sunday
    import all_reports_builder.py #send out all reports
