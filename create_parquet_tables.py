"""
Support module 
	COPY FILES FROM s3 TO LOCAL
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, lit, when, max, lower, explode, explode_outer,coalesce, monotonically_increasing_id, collect_set


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
	we are also going to use 'Deportes' column from one of them. 

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
			lower(col('Provincia')).alias('county'),
			col('Municipio').alias('city'),
			col('`C_Postal`').alias('postal_code')
			).withColumn("sports",lit('N/A'))\
			.withColumn("social_kind",lit('association')).distinct()\
		.union(Sparkdf_sport_clubs.select(
			col('Nombre').alias('name'),
			col('Domicilio').alias('address'),
			lower(col('Provincia')).alias('county'),
			col('Localidad').alias('city'),
			col('`C.Postal`').alias('postal_code'),
			col('Deportes').alias('sports')
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
			lower(col('Provincia')).alias('county'),
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

def toparquet_by_county_and_postcode(spark_session, parquet_location_path, sparkdf):

	"""
	Creates a parquet file with postal_codes
	
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	parquet_location_path: The complete path where to copy the parquet file (It could be 's3a/bucket/' or a local path) 
	sparkdf: the input sparkdataframe for the parquet table

	"""
	sparkdf.write.partitionBy("county","postal_code").parquet(parquet_location_path, mode="overwrite")


def create_cultural_from_json(spark_session, location_path, filenames):

	"""
	Reads the list of different cultural institution filenames a
	
	By default we have 2 filenames containing libraries and museums, but some other 
	institution type (and consequently files) could be added. 
	
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	location_path: The path where the files are (It could be 's3a://bucket/' or a local path) 
	file_names list: a list with the filenames

	Returns:
	sparkdf_cultural: the sparkdataframe with all the garitos
	"""

	sparkdf_list=[]

	for filename in filenames:

		sparkdf_handle = spark_session.read.options(multiLine=True).json(location_path+filename)

		# THE JSON FILE STRUCTURE IS COMPLEX TO THE MAX!!!
		# Dividing the process in several steps to make it easier to follow

		exp_df = sparkdf_handle.select('document.list.element.attribute')
		exp_df2 = exp_df.select(monotonically_increasing_id().alias('library_id'), explode_outer('attribute').alias('next_evolution'))
		exp_df3 = exp_df2.select(col('library_id'),explode('next_evolution').alias('next_evolution'))

		# THese are the fields that we'll use
		fields=['Identificador','NombreOrganismo','Calle','CodigoPostal','Localidad_NombreLocalidad','Directorio Superior']

		exp_df4=exp_df3.select(
			col('library_id'),
			col('next_evolution.name').alias('name'), 
			coalesce(col('next_evolution.valor'),
					col('next_evolution.LocalidadPadre'),
					col('next_evolution.string'),
					col('next_evolution.text')).alias('value'), 
			).filter(col('name').isin(fields) == True)


		exp_df5=exp_df4.select('*').groupBy("library_id").pivot('name').agg(collect_set('value')[0])

		exp_df6=exp_df5.select(
			col('NombreOrganismo').alias('name'),
			col('Calle').alias('address'),
			col('Directorio Superior').alias('county'),
			col('Localidad_NombreLocalidad').alias('city'),
			col('CodigoPostal').alias('postal_code')
			).withColumn("cultural_kind",lit(filename[:filename.find('.')])).distinct()
		#We finally apply union to all the sparkDf handles
	
		try:
			sparkdf_cultural
		except NameError:
			#is Sparkdf_garitos doesn't exist, we create it.
			sparkdf_cultural=exp_df6
		else:
			sparkdf_cultural = (
				sparkdf_cultural.union(exp_df6))

	return sparkdf_cultural