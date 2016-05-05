# -* - coding: UTF-8 -* -  
import ConfigParser
from pyspark import SparkContext
import time,datetime


'''定义函数'''
def to_timestamp(time_point):
	d = time.strptime(time_point,"%Y-%m-%d %H:%M:%S")
	return time.mktime(d)


def to_datetime(time_interval):
	x = time.localtime(float(time_interval))
	return time.strftime('%Y-%m-%d %H:%M:%S',x)


def etl(line, idx):
	idx = idx.split(',')
	idx = [int(i) for i in idx]
	line = line.split('\t')
	rst = []
	for i in xrange(0, len(idx)):
		if i==1:
			line[idx[i]] = to_datetime(line[idx[i]])
		rst.append(line[idx[i]])
	return ','.join(rst)


'''读取配置信息'''
config = ConfigParser.ConfigParser()
config.read('para.conf')
#spark配置参数[spark_conf]
spark_host = config.get('spark_conf', 'spark_host')
spark_read_data = config.get('spark_conf', 'spark_read_data')
spark_write_data = config.get('spark_conf', 'spark_write_data')
spark_app_name = config.get('spark_conf', 'spark_app_name')
spark_mode = config.get('spark_conf', 'spark_mode')
#程序中其他参数[etl]
extract_fields_names = config.get('etl', 'extract_fields_names')
extract_fields_indexes = config.get('etl', 'extract_fields_indexes')


'''数据处理'''
sc = SparkContext(spark_mode, spark_app_name)
netlogs = sc.textFile(spark_host+spark_read_data)
st_data  = netlogs.map(lambda line: etl(line, extract_fields_indexes)) \
				  .sortBy(lambda x: (x.split(',')[0], x.split(',')[1]))
st_data.saveAsTextFile(spark_host+spark_write_data)