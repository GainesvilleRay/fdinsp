import datetime

today = datetime.date.today()
weekday = today.weekday()

if (weekday == 1):
    import fdinsp_db_updater
