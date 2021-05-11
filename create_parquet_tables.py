"""
Support module 
	COPY FILES FROM s3 TO LOCAL
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, lit, when, max

def create_postal_code(sparkdf_garitos):

	"""
	Infers a list of county-city-postal_code from the list of garitos (bars, restaurants, cafe,...)
	
	Parameters:
	sparkdf_garitos: the sparkdataframe with all the garitos

	Returns:
	sparkdf_postal_codes: the sparkdataframe with the inferred postal_codes	
	"""

	return sparkdf_garitos.select(
		col('county'),
		col('city'),
		col('postal_code')
		).distinct().where(col('postal_code').isNotNull())


def toparquet_postal_codes(spark_session, parquet_location_path, sparkdf_postal_codes):

	"""
	Creates a parquet file with postal_codes
	
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	parquet_location_path: The path where the files are (It could be 's3a/bucket/' or a local path) 
	sparkdf_postal_codes: the sparkdataframe with the postal codes

	"""

	sparkdf_postal_codes.write.partitionBy("county").parquet(parquet_location_path + "postal_codes/", mode="overwrite")

def create_social(spark_session, location_path, asociationfilename, sportclubsfilename):

	"""
	Reads the list of different social elements returns a spark dataframe with the needed columns
		
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	location_path: The path where the files are (It could be 's3a://bucket/' or a local path) 
	asociationfilename filename for associations 
	sportclubsfilename filename containing sport clubs

	NOTE We cannot use an array with filenames as with garitos BECAUSE column names
	are not consistent among files (Asociación and Name / C_Postal and C.Postal) 

	Returns:
	sparkdf_social: the sparkdataframe with all the garitos
	"""

	Sparkdf_association = spark_session.read.options(inferSchema='true',\
				delimiter=';',
				header='true',
				encoding='ISO-8859-1').csv(location_path+asociationfilename)

	Sparkdf_sport_clubs = spark_session.read.options(inferSchema='true',\
				delimiter=';',
				header='true',
				encoding='ISO-8859-1').csv(location_path+sportclubsfilename)

	# We just need some columns for garitos: nombre, dirección, provincia, municipio and Código Postal
	# We keep just these and also translate them into English

	sparkdf_social = Sparkdf_association.select(
			col('Asociación').alias('name'),
			col('Domicilio').alias('address'),
			col('Provincia').alias('county'),
			col('Municipio').alias('city'),
			col('`C_Postal`').alias('postal_code')
			).withColumn("social_kind",lit('association')).distinct()\
		.union(Sparkdf_sport_clubs.select(
			col('Nombre').alias('name'),
			col('Domicilio').alias('address'),
			col('Provincia').alias('county'),
			col('Localidad').alias('city'),
			col('`C.Postal`').alias('postal_code')
			).withColumn("social_kind",lit('sports_club')).distinct())

	#We finally apply union to all the sparkDf handles

	print("social Spark Data Frame was created; \n ", sparkdf_social.head(2))

	return sparkdf_social


def create_garitos(spark_session, location_path, filenames):

	"""
	Reads the list of different garitos filenames and compose a parquet file with the 
	needed columns
	
	By default we have 3 filenames containing bars, restaurants and cafes, but some other 
	garitos type (and consequently files) could be added. 
	
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	location_path: The path where the files are (It could be 's3a://bucket/' or a local path) 
	file_names list: a list with the filenames

	Returns:
	sparkdf_garitos: the sparkdataframe with all the garitos
	"""

	sparkdf_list=[]

	for filename in filenames:

		sparkdf_handle = spark_session.read.options(inferSchema='true',\
					delimiter=';',
					header='true',
					encoding='ISO-8859-1').csv(location_path+filename)

	# We just need some columns for garitos: nombre, dirección, provincia, municipio and Código Postal
	# We keep just these and also translate them into English

		sparkdf_handle=sparkdf_handle.select(
			col('Nombre').alias('name'),
			col('Dirección').alias('address'),
			col('Provincia').alias('county'),
			col('Municipio').alias('city'),
			col('`C.Postal`').alias('postal_code')
			).withColumn("garito_kind",lit(filename[:filename.find('.')])).distinct()

	#We finally apply union to all the sparkDf handles

		sparkdf_list.append(sparkdf_handle)
	
		try:
			sparkdf_garitos
		except NameError:
			#is Sparkdf_garitos doesn't exist, we create it.
			sparkdf_garitos=sparkdf_handle
		else:
			sparkdf_garitos = (
				sparkdf_garitos.union(sparkdf_handle))

	return sparkdf_garitos

def toparquet_by_county_and_postcode(spark_session, parquet_location_path, sparkdf_garitos):

	"""
	Creates a parquet file with postal_codes
	
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	parquet_location_path: The complete path where to copy the parquet file (It could be 's3a/bucket/' or a local path) 
	sparkdf_garitos: the sparkdataframe with the garitos

	"""
	sparkdf_garitos.write.partitionBy("county","postal_code").parquet(parquet_location_path, mode="overwrite")