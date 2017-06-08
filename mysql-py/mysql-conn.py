#!/usr/bin/python

hostname = '127.0.0.1'
username = 'root'
password =  'redhat'
database = 'mysql'
#socket ='/tmp/mysql-master5520.sock'
#Simple script to connect to mysql and execute the queries
# Simple routine to run a query on a database and print the results:
def Query( conn ) :
    print "Printing user and hosts from the box\n"
    cur = conn.cursor()
    cur.execute( "SELECT user,host FROM user" )
#   for row in cur :
    row = cur.fetchone()
    while row is not None:
        print row[0], row[1]
        row = cur.fetchone()
#       print (row)

def index ( conn1 ) :
    cur = conn1.cursor()
    print "\n\nFind total number of tables, rows, total data in index size for given MySQL Instance\n"
    cur.execute( "SELECT count(*) tables,concat(round(sum(table_rows)/1000000,2),'M') rows,concat(round(sum(data_length)/(1024*1024*1024),2),'G') data,concat(round(sum(index_length)/(1024*1024*1024),2),'G') idx,concat(round(sum(data_length+index_length)/(1024*1024*1024),2),'G') total_size,round(sum(index_length)/sum(data_length),2) idxfrac FROM information_schema.TABLES;")
    for row in cur :
        print (row)


def data_big ( conn3 ) :
    cur = conn3.cursor()
    print "\n\nFind biggest db"
    cur.execute ( "SELECT count(*) tables,table_schema,concat(round(sum(table_rows)/1000000,2),'M') rows,        concat(round(sum(data_length)/(1024*1024*1024),2),'G') data,        concat(round(sum(index_length)/(1024*1024*1024),2),'G') idx,        concat(round(sum(data_length+index_length)/(1024*1024*1024),2),'G') total_size,        round(sum(index_length)/sum(data_length),2) idxfrac  FROM information_schema.TABLES        GROUP BY table_schema        ORDER BY sum(data_length+index_length) DESC LIMIT 10;" )
    for row in cur :
        print (row)

def data_dist ( conn4 ) :
   cur = conn4.cursor()
   print "\n\nData Distribution by Storage Engines\n"
   cur.execute ( "SELECT engine,count(*) tables,concat(round(sum(table_rows)/1000000,2),'M') rows,concat(round(sum(data_length)/(1024*1024*1024),2),'G') data,concat(round(sum(index_length)/(1024*1024*1024),2),'G') idx,concat(round(sum(data_length+index_length)/(1024*1024*1024),2),'G') total_size,round(sum(index_length)/sum(data_length),2) idxfrac FROM information_schema.TABLES GROUP BY engine ORDER BY sum(data_length+index_length) DESC LIMIT 10;" )
   for row in cur :
       print (row)

print "======Using pymysql========"
print "     "
import pymysql as my
connect = my.connect( host=hostname, user=username, passwd=password, db=database )
Query( connect )
index( connect )
data_big ( connect )
data_dist ( connect )
connect.close()
