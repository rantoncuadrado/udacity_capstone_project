# Step 1: Scope the Project and Gather Data

## Scope 

**We plan to prepare data for further studies on population vs social associations, cultural institutions and garitos (restaurants, cafeterias, bars...) in the Autonomous Community of Castilla y León https://en.wikipedia.org/wiki/Castile_and_Le%C3%B3n**

To do this we will need a bucketed population / city table and tables for each kind of organization: garitos, cultural, social. We'll also need a postal_code table/list.

We expect being able to join on city / county (as these 2 dimensions will be present and clean on all the tables)



## Describe and Gather Data 
Describe the data sets you're using. Where did it come from? What type of information is included? 
https://github.com/rantoncuadrado/udacity_capstone_project/blob/main/Datasources%20Description.md
Datasources Description.md file

We'll use 8 datasources (6 csv, 2 json) with a total of more than 2.5 million rows.

Directorio de Bibliotecas de Castilla y León.json JSON 482Kb // 506 LINES
Directorio de Museos de Castilla y León.json JSON 596Kb // 784 LINES
Clubes Deportivos.csv  CSV 5.4Mb // 8406 LINES
Restaurantes.csv CSV 1.2Mb // 5961 LINES
Cafeterias.csv CSV  268Kb // 1448 LINES
Bares.csv CSV 2.2 MB // 15082 LINES
AsociacionesJCyL.csv CSV 18.3 MB // 35345 LINES
Cities population per gender age.csv CSV 100.4Mb // 2483803 LINES

Source: https://datosabiertos.jcyl.es/web/es/datos-abiertos-castilla-leon.html
These files are copied in a S3 bucket and in local. (I include a copy in input-files folder)



# Step 2: Explore and Assess the Data

## Explore the Data

## Identify data quality issues, like missing values, duplicate data, etc.

## Cleaning Steps

Clubes Deportivos, Restaurantes, Cafeterias, Bares, AsociacionesJCyL are generaly well built files with overall good data quality, with some inconsistencies like misspelled counties/cities, cities that appear as counties, missing postal_codes...
I checked for null values and duplicates and tried to fix the data where possible (see the jupyter notebook and the helper files)

The JSON files Directorio de Bibliotecas de Castilla y León.json and Directorio de Museos de Castilla y León.json have an -unnecesarily- convoluted schema, so it took time to find the way to extract the data. Once extracted the data quality is generally good. I checked for null values and duplicates and tried to fix the data where possible (see the jupyter notebook and the helper files)

I anticipated the Cities population per gender age was going to be a challengue, because of the schema that is pretty different to the final expected result:

Example Rows:
```
Municipios;Sexo;Edad (año a año);Total
TOTAL NACIONAL;Ambos sexos;Total;46.815.916
...
TOTAL NACIONAL;Ambos sexos;97 años;13.241
...
TOTAL NACIONAL;Hombres;7 años;246.271
TOTAL NACIONAL;Hombres;13 años;220.346
...
TOTAL NACIONAL;Mujeres;75 años;209.412
...
44001  Ababuj;Ambos sexos;4 años;..
```

The file contains a row containing the population per city (city contains the postal code), gender (male, female or both), age and it contains information for the whole country. This is quite divergent from the expected outcome with 1 row / city with the recap values. We found a list of added difficulties having to do with the data quality that I needed to wrangle before using hte data. Due to the nature of these issues, they were dealt with in create_parquet_tables.py file in function  ```create_population(spark_session, location_path, population_filename)```. To mention some of these issues:

- The postal code is part of the name and we needed to infer the county from the postal code, suing regular expressions.
- The file was badly encoded and I needed to 'translate' the codes for Ñ,ñ,á,é,í,ó,ú,ü,Á,É,Í,Ó,Ú, at least in column names and cities (as cities will be used to join this table with others).
- 0 population for an age is encoded sometimes with no value, sometimes like 0, and sometimes with '..', so I needed to convert this too.
- numbers are encoded with thousends dots (like 1.000 for a thousend) and we needed to remove these '.' 
- the numeric fields contained other characters so I removed them and then I casted the result to int.
- I also found problems due to the above issue in the aggregation / sum.
- ...


All the cleaning and wrangling steps are documented in:
- the *data_quality_checks.py* file (containing functions)
- the *create_parquet_tables.py* file (containing functions)
- the jupyter notebook, *Capstone Project.ipynb*

For more information on the data sources schema, etc: https://github.com/rantoncuadrado/udacity_capstone_project/blob/main/Datasources%20Description.md

# Step 3: Define the Data Model

## 3.1 Conceptual Data Model

We will prepare several parquet files out of the 8 input datasources:

- a **garitos** parquet file containing the useful columns (those that we consider can be used to join with other tables or to infer results) from these 3 datasources: restaurant, bars and cafeterias. We'll add a new column called 'garito_kind'
- a **social** parquet file containing the useful columns (those that we consider can be used to join with other tables or to infer results) from the 2 datasources containing the list of sport clubs and associations. We'll add a new column called 'social_kind'
- a **cultural** parquet file containing the useful columns (those that we consider can be used to join with other tables or to infer results) from the 2 datasources containing the list of museums and libraries. We'll add a new column called 'cultural_kind'.

These 3 files schema contains these fields:
 name|             address|    county|                city|postal_code|social_kind / cultural_kind / garito_kind
And just in the case of social, sports (a list of the sports that are the object of the sport club)

### Garitos

So, for garitos:

<img width="593" alt="image" src="https://user-images.githubusercontent.com/6404071/118194145-36cdc780-b449-11eb-88c5-774a6307707b.png">

```
root
 |-- name: string (nullable = true) *Comes from field 'Nombre' in the bares/restaurants/cafeterias file*
 |-- address: string (nullable = true) *Comes from field 'Dirección' in the 3 garitos files*
 |-- county: string (nullable = true) *Comes from field 'Provincia' in the 3 garitos files*
 |-- city: string (nullable = true) *Comes from field 'Municipio' in the 3 garitos files. We could have used Municipio, Localidad or Nucleo, but Municipio was more consistently filed and contained fewer mistakes, nulls, unexpected values to fix*
 |-- postal_code: string (nullable = true) *Comes from C.Postal in the 3 garitos files. Also, we would try to guess the postal_code when it is not present in the datafile*
 |-- garito_kind: string (nullable = false) *As we made a UNION of data coming from the 3 datafiles, this column identifies the kind of garito (depending on the file where the element was stored: bar/restaurant/cafeteria)*
```

### Social

For social:

<img width="591" alt="image" src="https://user-images.githubusercontent.com/6404071/118194127-2d445f80-b449-11eb-84ad-12886d5eeca8.png">

```
root 
 |-- name: string (nullable = true) *Comes from field 'Nombre' in the sportclubs file or 'Asociación' in the associations file*
 |-- address: string (nullable = true) *Comes from field 'Domicilio' in the asociaciones/sportclubs files*
 |-- county: string (nullable = true) *Comes from field 'Domicilio' in the asociaciones/sportclubs files*
 |-- city: string (nullable = true) *Comes from field 'Municipio' in the sportclubs file or 'Localidad' in the associations file*
 |-- postal_code: string (nullable = true) *Comes from field 'C.Postal' in the sportclubs file or 'C_Postal' in the associations file*
 |-- sports: string (nullable = true) *Contains a | sepparated list with the sports that are the object of the sportclub or null if the row came from a cultural association*
 |-- social_kind: string (nullable = false) *As we made a UNION of data coming from the 3 datafiles, this column identifies the kind of social (depending on the file where the element was stored: association/sportclub)*
```

### Cultural

For cultural:


<img width="181" alt="image" src="https://user-images.githubusercontent.com/6404071/118194104-2289ca80-b449-11eb-9601-7323a71de337.png">

```
root
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- county: string (nullable = true)
 |-- city: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- cultural_kind: string (nullable = false)
```

The issue for cultural was that the input json files were really convoluted. This is the schema:

<img width="288" alt="image" src="https://user-images.githubusercontent.com/6404071/118194079-11d95480-b449-11eb-9075-a385e56225c9.png">

```
root
 |-- document: struct (nullable = true)
 |    |-- date: string (nullable = true)
 |    |-- list: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- element: struct (nullable = true)
 |    |    |    |    |-- attribute: array (nullable = true)
 |    |    |    |    |    |-- **element**: struct (containsNull = true)
 |    |    |    |    |    |    |-- LocalidadPadre: string (nullable = true)
 |    |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |    |-- string: string (nullable = true)
 |    |    |    |    |    |    |-- text: string (nullable = true)
 |    |    |    |    |    |    |-- valor: string (nullable = true)
 ```
 
 Once we get to the inner element struct, we found pairs attribute-value, where the attribute name is 'name' but the assigned value could be 'LocalidadPadre', 'string', 'text' or 'valor'.
 
 So the information for one of these elements (say a library) has this aspect:
 
 <img width="362" alt="image" src="https://user-images.githubusercontent.com/6404071/118194017-f706e000-b448-11eb-8107-a49ca1a990b2.png">

### Postal Codes

- a **postal codes** parquet files containing triplets county-city-postal_code. This parquet file is extracted from the information in garitos file (it is not a complete postal_code list, as the aim is just to work/play with data).

<img width="414" alt="image" src="https://user-images.githubusercontent.com/6404071/118194186-45b47a00-b449-11eb-9531-56d650f85f51.png">

```
root
 |-- county: string (nullable = true) *Extracted from field 'county' in garitos final table*
 |-- city: string (nullable = true) *Extracted from field 'city' in garitos final table*
 |-- postal_code: string (nullable = true) *Extracted from field 'postal_code' in garitos final table*
```

### Population

- a **population** parquet file containing the county, city and population (total and in buckets). 

<img width="583" alt="image" src="https://user-images.githubusercontent.com/6404071/118194216-4fd67880-b449-11eb-9c55-f3313369dbc4.png">

```
 |-- county: string (nullable = false) *The county is guessed from the postal_code included in field 'Municipios' in Cities population per gender age.csv*
 |-- city: string (nullable = true) *The city is extracted with a regexp from field 'Municipios' in Cities population per gender age.csv*
 |-- pop_under15: long (nullable = true) *Population in bucket 0..15 years calculated from the Cities population per gender age.csv*
 |-- pop_16to30: long (nullable = true) *Population in bucket. Same*
 |-- pop_31to45: long (nullable = true) *Population in bucket. Same*
 |-- pop_46to65: long (nullable = true) *Population in bucket. Same*
 |-- pop_66to80: long (nullable = true) *Population in bucket. Same*
 |-- pop_over80: long (nullable = true) *Population in bucket. Same*
 |-- total_population: long (nullable = true) *Total population is the sum of all the buckets for a given city*
```

## 3.2 Mapping Out Data Pipelines
The pipelines are explained step by step in the jupyter notebook file *Capstone Project.ipynb*. 


# Step 4: Run Pipelines to Model the Data 

## 4.1 Create the data model
These steps are detailed in the jupyter notebook file *Capstone Project.ipynb*. 

## 4.2 Data Quality Checks
Same. Data Quality checks are included in 3 files:
- The Jupyter notebook *Capstone Project.ipynb* contains and explains in comments the data quality checks and steps to fix the problems we found.
- File *data_quality_checks.py* contains functions used to check for data quality
- File *create_parquet_tables.py* contains functions to read and prepare the data (and in some cases, especially in *create_population* function we should consider the data wrangling started there.

## 4.3 Data dictionary 

I included field definitions in the Conceptual Data Model section, up above.


# Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
We used Spark for speed, ease of use, scalability and because it allows with just some parameter adjustments to work on a bucket or in a local filesystem. Spark will allow to paralellize the calculations in case of need (if the data sources are bigger or we extend the analysis for the whole country with 20x more population and therefore bars, museums...)

We stored the output tables in parquet files because it is self-describing, it is very optimized in space, is language-independent, so could be used from different apps. Also it is columnar (vs row oriented)

* Propose how often the data should be updated and why.

According to the Junta de Castilla y León https://datosabiertos.jcyl.es/web/es/datos-abiertos-castilla-leon.html the datasets are updated daily. However it's unlikely that a great difference can be observed in one day in the number of museums, restaurants, etc... so I'd suggest a monthly update.

* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 
 I would default to s3 for the storage and operations and try to tap into Spark features to work in parallel. 
 
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 
 I'd convert the jupyter notebook operations / step into DAGs to be scheduled in Airflow for a daily launch / update at 7am.
 
 * The database needed to be accessed by 100+ people.

 We could COPY the parquet files into Amazon Redshift tables (this is pretty straighforward with COPY feature and could be automatised as a part of the DAG in Airflow). Parquet has a great support for alternative DBMS. 
 
