# fdinsp

This repo containes scripts and assets related to gathering state restaurant inspection in Florida and formatting it for publication.
It was developed by Doug Ray at The Gainesville Sun in Gainesville, Florida. doug.ray@gainesville.com.

Files included:

all_reports_builder.py -- updated script to produce all reports in the list of counties

bigreport.txt -- the report, which gets overwritten on each run of the report_builder, and emailed to recipients

dbbuilder.py -- a script to build the database file

db_update_log.txt -- info that goes into the email to record how many reports gathered or what was missed

fdinsp_updater.py -- scrapes data and deposits it into a database

insptypes.csv -- a module of sorts that has some text which goes into the narrative

report_builder.py -- formats a weekly narrative from the database (for individual county)

rinspect.sqlite -- the database
