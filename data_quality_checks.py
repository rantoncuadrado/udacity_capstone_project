"""
Support module 
	COPY FILES FROM s3 TO LOCAL
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, lit, when, max, regexp_replace, lower

def check_sparkdf_not_nulls(sparkdf,columns):

	"""
	Check a sparkdataframe to find existence of null values in given columns
	Parameters:
	sparkdf: the sparkdataframe to be checked


	Returns:
	true if there are no null values in these columns
	false if there are null values (at least 1 null value)
	"""

	for column in columns:

		empties = sparkdf.select(col(column)).where(col(column).isNull())
		if len(empties.head(1)) > 0:
			print("Checking DataFrame. I found null values in column", column)
			return False
		else:
			print("Checking DataFrame. No null values found in column", column)

	return True

def check_sparkdf_find_dupes(sparkdf,columns):

	"""
	Check a sparkdataframe to find existence of duplicate values for a set of given columns
	Parameters:
	sparkdf: the sparkdataframe to be checked


	Returns:
	the ordered list of duplicate values

	"""

	return sparkdf.groupBy(columns).count().where('count>1').sort('count', ascending=False)


def clean_wrong_counties(sparkdf):

	"""
	Check a sparkdataframe to find existence of rows with county != one of the valid 
	and remove them
	Parameters:
	sparkdf: the sparkdataframe to be checked
	counties: 

	Returns:
	The list without rows with wrong counties

	"""
	counties=['burgos','ávila','león','segovia','palencia','zamora','soria','salamanca','valladolid']

	# First we replace common errors (avila -> ávila, leon -> león)
	sparkdf=sparkdf.withColumn('county',lower(col('county'))).withColumn('county', regexp_replace('county', 'avila', 'ávila')).withColumn('county', regexp_replace('county', 'leon', 'león'))

	return sparkdf.filter(col('county').isin(counties) == True)

