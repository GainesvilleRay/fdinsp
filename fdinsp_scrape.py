from datetime import datetime
today = datetime.date.today()
weekday = today.weekday()

if (weekday == 6):
    import fdinsp_db_updater