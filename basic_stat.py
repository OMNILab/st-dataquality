# -* - coding: UTF-8 -* -  
import ConfigParser
from pyspark import SparkContext
import datetime


'''定义函数'''
def date_difference(d1, d2):#d1早于d2
	d1 = datetime.datetime.strptime(d1 + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
	d2 = datetime.datetime.strptime(d2 + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
	delta = d2 - d1
	return delta.days


'''读取配置信息'''
config = ConfigParser.ConfigParser()
config.read('para.conf')
#spark配置参数[spark_conf]
spark_host = config.get('spark_conf', 'spark_host')
spark_mode = config.get('spark_conf', 'spark_mode')
#程序中其他参数[basic_stat]
app_name = config.get('basic_stat', 'app_name')
read_data = config.get('basic_stat', 'read_data')
write_data = config.get('basic_stat', 'write_data')


'''数据处理'''
'''
时间维度:	min_date,max_date
空间维度:	min_lng,max_lng,min_lat,max_lat,base_station_num
数量:	total_records,avr_records,total_users,avr_users
'''
sc = SparkContext(spark_mode, app_name)
st_data = sc.textFile(spark_host+read_data)
#时间维度
st_date = st_data.map(lambda x: x.split(',')[1].split(' ')[0])
min_date = st_date.min()
max_date = st_date.max()
#空间维度
st_lng = st_data.map(lambda x: float(x.split(',')[3]))
min_lng = st_lng.min()
max_lng = st_lng.max()
st_lat = st_data.map(lambda x: float(x.split(',')[4]))
min_lat = st_lat.min()
max_lat = st_lat.max()
st_base_station = st_data.map(lambda x: x.split(',')[2])
base_station_num = st_base_station.distinct().count()
#数量
total_records = st_date.count()
total_users = st_data.map(lambda x: float(x.split(',')[0])).distinct().count()
det = date_difference(min_date, max_date)
avr_records = total_records/det
avr_users = total_users/det

basic_stat = sc.parallelize([min_date,max_date,min_lng,max_lng,min_lat,max_lat,base_station_num,total_records,avr_records,total_users,avr_users])
basic_stat.saveAsTextFile(spark_host+write_data)