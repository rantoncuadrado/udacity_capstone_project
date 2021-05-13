"""
Support module 
	COPY FILES FROM s3 TO LOCAL
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, lit, when, max, lower, explode, explode_outer,coalesce, monotonically_increasing_id, collect_set , regexp_replace, regexp_extract, expr
from pyspark.sql.types import IntegerType

def create_population(spark_session, location_path, population_filename):

	"""
	Reads the list of different social elements returns a spark dataframe with the needed columns
		
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	location_path: The path where the files are (It could be 's3a://bucket/' or a local path) 
	population_filename filename for population 

	Returns:
	sparkdf_population: the sparkdataframe with all the garitos
	"""

	sparkdf_handle = spark_session.read.options(inferSchema='true',\
				delimiter=';',
				header='true',
				encoding='ISO-8859-1').csv(location_path+population_filename)

	# first we filter out municipalities that are not in Castilla y León region
	# We can use the Municipios code (starts by the postal code of the cities)
	# We change some column names 
	# We use both genders population together
	# We remove . from numbers in population, and also convert .. into 0 values
	# We infer county from postal code and remove the postal code part (as postal code is
	# just one of the possible postal codes for each city, when there are some)
	# We finally fix all the weird chars (coming from the file that has these faults)
	# and convert age and population to Integer

	print("Population dataframe rows: ",sparkdf_handle.count())

	sparkdf_handle=sparkdf_handle \
	.select(col('Municipios').alias('city'),
			col('Edad (aÃ±o a aÃ±o)').alias('age'),
			col('Total').alias('population'))\
	.where("Municipios like '05%' or Municipios like '09%' or Municipios like '24%' or \
			Municipios like '34%' or Municipios like '37%' or Municipios like '40%' or \
			Municipios like '42%' or Municipios like '47%' or Municipios like '49%' ") \
	.where("Sexo ='Ambos sexos'")\
	.where("age <> 'Total'")\
	.withColumn('population', regexp_replace('population','\.\.','0')) \
	.withColumn('population', regexp_replace('population','\.','')) \
	.withColumn('age', regexp_extract('age', '([0-9]*)(\s+)', 1)) \
	.withColumn("county",expr("case when city like '05%' then 'ávila' " +
							"when city like '09%' then 'burgos' " +
							"when city like '24%' then 'león' " +
							"when city like '34%' then 'palencia' " +
							"when city like '37%' then 'salamanca' " +
							"when city like '40%' then 'segovia' " +
							"when city like '42%' then 'soria' " +
							"when city like '47%' then 'valladolid' " +
							"when city like '49%' then 'zamora' " +
							"else 'Great' end")) \
	.withColumn('city', regexp_extract('city', '([0-9]*)(\s+)(.+)', 3)) \
	.withColumn('city', regexp_replace('city', 'Ã¡', 'á')) \
	.withColumn('city', regexp_replace('city', 'Ã\x81', 'Á')) \
	.withColumn('city', regexp_replace('city', 'Ã©', 'é')) \
	.withColumn('city', regexp_replace('city', 'Ã\xad', 'í')) \
	.withColumn('city', regexp_replace('city', 'Ã\x8d', 'Í')) \
	.withColumn('city', regexp_replace('city', 'Ã³', 'ó')) \
	.withColumn('city', regexp_replace('city', 'Ã\x93', 'Ó')) \
	.withColumn('city', regexp_replace('city', 'Ã\x9a', 'Ú')) \
	.withColumn('city', regexp_replace('city', 'Ãº', 'ú')) \
	.withColumn('city', regexp_replace('city', 'Ã¼', 'ü')) \
	.withColumn('city', regexp_replace('city', 'Ã±', 'ñ')) \
	.withColumn('age', col('age').cast(IntegerType())) \
	.withColumn('population', col('population').cast(IntegerType()))

	print("Population dataframe rows: ", sparkdf_handle.count())

	sparkdf_population = sparkdf_handle.select('county','city','age','population')\
	.withColumn('under16',expr('case when age <16 then population else 0 end'))\
	.withColumn('16to30',expr('case when age >= 16 and age <31 then population else 0 end'))\
	.withColumn('31to45',expr('case when age >= 31 and age <46 then population else 0 end'))\
	.withColumn('46to65',expr('case when age >= 46 and age <66 then population else 0 end'))\
	.withColumn('66to80',expr('case when age >= 66 and age <81 then population else 0 end'))\
	.withColumn('over80',expr('case when age >= 81 then population else 0 end'))\
	.groupBy('county','city') \
	.agg({'under16': 'sum' , '16to30': 'sum' , '31to45': 'sum', '46to65': 'sum' , '66to80': 'sum' , 'over80': 'sum'} )\
	.withColumn("total Population",col('sum(under16)')+col('sum(16to30)')+col('sum(31to45)')+col('sum(46to65)')+col('sum(66to80)')+col('sum(over80)'))

	#We need to rename columns if we want them to be usable for the parquet files

	sparkdf_population=sparkdf_population.select(
		col('county'),
		col('city'),
		col('sum(under16)').alias('pop_under15'),
		col('sum(16to30)').alias('pop_16to30'),
		col('sum(31to45)').alias('pop_31to45'),
		col('sum(46to65)').alias('pop_46to65'),
		col('sum(66to80)').alias('pop_66to80'),
		col('sum(over80)').alias('pop_over80'),
		col('total Population').alias('total_population'))

	return sparkdf_population


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

def toparquet_by_county(spark_session, parquet_location_path, sparkdf):

	"""
	Creates a parquet file with postal_codes
	
	Parameters:
	spark_session: the session we'll use (It could be local or to read from s3)
	parquet_location_path: The complete path where to copy the parquet file (It could be 's3a/bucket/' or a local path) 
	sparkdf: the input sparkdataframe for the parquet table

	"""
	sparkdf.write.partitionBy("county").parquet(parquet_location_path, mode="overwrite")


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