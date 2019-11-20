# fdinsp

This repo containes scripts and assets related to gathering state restaurant inspection in Florida and formatting it for publication.
It was developed by Doug Ray at The Gainesville Sun in Gainesville, Florida. doug.ray@gainesville.com.

Files included:

* all_reports_builder.py -- updated script to produce all reports in the list of counties **

* all_reports_task.py -- script to automate the task of running all_reports_builder.py. Not currently in use.

* bigreport.txt -- the report, which gets overwritten on each run of the report_builder, and emailed to recipients

* db_records_checker.py -- a script to check whether the database tables are consistent, and delete records as needed.

* db_records_deleter -- a Jupyter version of db_records_checker, but not the current version of above.

* dbbuilder.py -- a script to build the database file

* db_update_log.txt -- info that goes into the email to record how many reports gathered or what was missed

* fdinsp_db_ updater.py -- scrapes data and deposits it into a database. This is the version now in use. **

* fdinsp_db_updater2.py -- a newer version of above that I think is more pythonic but not yet in use.

* insptypes.csv -- a module of sorts that has some text which goes into the narrative

* report_builder.py -- formats a weekly narrative from the database (for individual county)

* requirements.txt -- more libraries than really needed. It's just my base install for Python at present.

/** denotes files that are the primary scripts to do this job.
