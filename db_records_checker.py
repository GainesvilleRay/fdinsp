"""
Script to see if tables in rinspect.sqlite are valid,
with all of the reports in the summaries table, 'fdinsp',
represented in the table of detailed reports, 'violations'.

Also, code to delete records from the database after the last
good run date.

Written by Douglas Ray, doug.ray@starbanner.com
"""

import sqlite3

def create_connection(db_file):
    """
    Create a database connection
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def visitid_by_date(conn, good_date):
    """
    Create a list of visitid by date
    :param conn: Connection to the database
    :param good_date: last good date in database
    :param return: list of visitids since good_date
    """
    myquery = f"SELECT visitid FROM fdinsp WHERE time_posted >=?"
    cur = conn.cursor()
    cur.execute(myquery, (good_date,))
    idlist=list(cur.fetchall())
    list_count = str(len(idlist))

    return idlist, list_count

def delete_violations(conn, id):
    """
    Delete detailed violations by visitid
    :param conn:  Connection to the SQLite database
    :param id: visitid of the violation
    :return:
    """
    sql = "DELETE FROM violations WHERE id=?"
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def delete_fdinsp(conn, good_date):
    """
    Delete summary reports since good date
    :param: Connection to database
    :param: Last good date in database
    :return:
    """

    sql = "DELETE FROM fdinsp WHERE time_posted >=?"
    conn = sqlite3.connect('rinspect.sqlite')
    cur = conn.cursor()
    cur.execute(sql, (good_date,))
    conn.commit()

def find_orphans(conn):
    """
    Find records from violations table where the associated
    inspection is not in the fdinsp table.
    """

    sql = "SELECT visitid FROM violations EXCEPT SELECT visitid FROM fdinsp;"
    cur = conn.cursor()
    cur.execute(sql,)
    idlist = list(cur.fetchall())
    #list_count = str(len(idlist))
    #print(list_count)

    return idlist

def main():
    """
    Set to run function that checks for orphaned records in 'violations'
    table. To run functions that delete records from both tables based
    on the last 'good date', uncomment function calls.
    """

    good_date = "2019-11-12" # Update as needed
    database = "rinspect.sqlite"
    conn = create_connection(database)

    # create the list of visitid values in table 'fdinsp'
    visitids = visitid_by_date(conn, good_date)
    list_count = visitid_by_date(conn, good_date)
    visitids = [i[0] for i in visitids]

    # create list of orphans in table 'violations'
    idlist = find_orphans(conn)
    idlist = [i[0] for i in idlist]

    # use list to delete records in violations table; uncomment to run
    #for id in visitids:
    #    delete_violations(conn, id)

    # delete summary reports since good date; uncomment to run
    #delete_fdinsp(conn, good_date)
    #list_count = visitid_by_date(conn, good_date)
    #print("Now there are" + list_count + "records since good date.")

    conn.close()

    #print(visitids)
    print("We found " + str(len(idlist)) + " orphans in 'violations' table.")

if __name__ == '__main__':
    main()
