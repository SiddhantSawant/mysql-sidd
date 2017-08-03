import sys
import logging
import argparse
import MySQLdb as mysql
from warnings import filterwarnings


def parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", dest="host", help='Enter Hostname/ip to connect')
    parser.add_argument("-u", "--user", dest="user", help='Enter username to connect')
    parser.add_argument("-p", "--password", dest="password" , help='Specify Password')
    parser.add_argument("-P", "--port", dest="port_no", default=3306, type=int,help="MySQL port to connect")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()

options = parse_options()

def get_mysql_conn(host, port_no):
    conn = mysql.connect(host=host, port=port_no,read_default_group='client', read_default_file='~/.my.cnf')
    filterwarnings('ignore', category=mysql.Warning)
    return conn

def my_user():
        cursor = get_mysql_conn(options.host, options.port_no).cursor(mysql.cursors.DictCursor)
        sql = "SELECT user,host FROM mysql.user"
        cursor.execute(sql)
        print "User available in server are \n"
        print(cursor.fetchall())
        cursor.close()

def index():
	cursor = get_mysql_conn(options.host, options.port_no).cursor(mysql.cursors.DictCursor)
	sql = """SELECT count(*) tables,concat(round(sum(table_rows)/1000000,2),'M') rows,concat(round(sum(data_length)/(1024*1024*1024),2),'G') data,
concat(round(sum(index_length)/(1024*1024*1024),2),'G') idx,
concat(round(sum(data_length+index_length)/(1024*1024*1024),2),'G') total_size,round(sum(index_length)/sum(data_length),2) idxfrac 
FROM information_schema.TABLES;"""
	print "\nPrinting Dup indexes\n"
	cursor.execute(sql)
	print(cursor.fetchall())


def data_big():
	cursor = get_mysql_conn(options.host, options.port_no).cursor(mysql.cursors.DictCursor)
	print "\nFind biggest db\n"
	sql= """SELECT count(*) tables,table_schema,concat(round(sum(table_rows)/1000000,2),'M') rows,
concat(round(sum(data_length)/(1024*1024*1024),2),'G') data,concat(round(sum(index_length)/(1024*1024*1024),2),'G') idx, 
concat(round(sum(data_length+index_length)/(1024*1024*1024),2),'G') total_size,round(sum(index_length)/sum(data_length),2) idxfrac  
FROM information_schema.TABLES GROUP BY table_schema ORDER BY sum(data_length+index_length) DESC LIMIT 10;"""
	cursor.execute(sql)
	print(cursor.fetchall())
	cursor.close()

def data_dist():
	cursor = get_mysql_conn(options.host, options.port_no).cursor(mysql.cursors.DictCursor)
	print "\n\nData Distribution by Storage Engines\n"
	sql= """SELECT engine,count(*) tables,concat(round(sum(table_rows)/1000000,2),'M') rows,
concat(round(sum(data_length)/(1024*1024*1024),2),'G') data,concat(round(sum(index_length)/(1024*1024*1024),2),'G') idx,
concat(round(sum(data_length+index_length)/(1024*1024*1024),2),'G') total_size,round(sum(index_length)/sum(data_length),2) idxfrac
FROM information_schema.TABLES GROUP BY engine ORDER BY sum(data_length+index_length) DESC LIMIT 10;"""
	cursor.execute(sql)
	print(cursor.fetchall())
	cursor.close()


print 20*'-'
print " OPTIONS MENU"
print 20 * '-'
print " 1 . Know available set of users "
print " 2 . Largest Database "
print " 3 . Duplicate indexes "
print " 4 . Data distribution by Storage engine "
print 20 * '-'

while True:
	try:
		opt = { 1: my_user, 2 : data_big , 3 : index, 4 : data_dist}
		value = int(raw_input("Enter your choice 1-4: "))
		if value<=4:
			opt.get(value)()
			break
		else :
			print "Please enter valid option from 1-4"
			continue
	except :
		print "Please enter integer value from 1-4"
#		break

