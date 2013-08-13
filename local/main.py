import inspect, samplers, datasets
import db_info
import MySQLdb

db = MySQLdb.connect(host=db_info.host, # your host, usually localhost
                     user=db_info.user, # your username
                      passwd=db_info.passwd, # your password
                      db=db_info.db) # name of the data base

cur = db.cursor() 			#cursor object for mysql query	

def main():
	print 'Starbird Datasets: V 1.0'
	print '[0] datasets\n[1] samplers'
	option = raw_input('>> ')
	if option == '0':
		for (i,x) in enumerate(inspect.getmembers(datasets, inspect.isfunction)):
			print '[%s] %s' % (i,x[0])
		option = int(raw_input('>> '))
		inspect.getmembers(datasets, inspect.isfunction)[option][1](cur)
	elif option == '1':
		for (i,x) in enumerate(inspect.getmembers(samplers, inspect.isfunction)):
			print '[%s] %s' % (i,x[0])
		option = int(raw_input('>> '))
		inspect.getmembers(samplers, inspect.isfunction)[option][1](cur)
	else:
		print 'invalid input'

if __name__ == "__main__":
    main()