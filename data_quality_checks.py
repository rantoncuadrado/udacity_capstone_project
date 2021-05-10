"""
Support module 
	COPY FILES FROM s3 TO LOCAL
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, lit, when, max

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

