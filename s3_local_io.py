"""
Support module 
	COPY FILES FROM s3 TO LOCAL
"""

import os
import configparser
import boto
import botocore
import boto3
from boto.s3.key import Key

#  https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3


def copy_files_s3_to_local(bucket_name, local_path):

	"""
	It copies a number of files from s3 to local

	Parameters:
	bucket_name: s3 bucket name where the files are
	local_path: local path to store the files 
	"""

	config=configparser.ConfigParser()
	config.read('dl.cfg')


	# SETUP FOR USING S3 BUCKETS WHERE THE DATAFILES ARE

	os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
	os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

	# connect to the bucket
	try:
		conn = boto.connect_s3(os.environ['AWS_ACCESS_KEY_ID'],os.environ['AWS_SECRET_ACCESS_KEY'])
		print("Connected to the s3 bucket",conn)
	except:
		print("Cannot connect with the bucket")
		return

	# Explanation client and resource
	s3_resource = boto3.resource('s3')
	s3 = boto3.client('s3')

	resp=s3.list_objects_v2(Bucket=bucket_name )

	# List file names
	files = []
	for obj in resp['Contents']:
		files.append(obj['Key'])

	print("List of bucket objects",files)

	# This would clone a file in a s3 bucket
	# s3_resource.Object('raul-udacity','bares2.csv').copy_from(CopySource='raul-udacity/bares.csv')
	# And this would read it df=spark.read.json("s3a://udacity-dend/log_json_path.json")

	for file in files:
		try:
			s3_resource.Bucket(bucket_name).download_file(file, local_path+file)
		except botocore.exceptions.ClientError as e:
			if e.response['Error']['Code'] == "404":
				print("I cannot find object ", file)
			else:
				raise
