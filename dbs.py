#Optionen, dbname,user,password entsprechend eurer Datenbank ändern
dbname="dbs"
user="postgres"
password="Benedikt"
path_to_file="C:\\Users\\caval\\downloads\\gdp.csv" #für windows \\ sonst /
#in gdp.csv alle ("") löschen
path_to_file2="C:\\Users\\caval\\downloads\\population_growth.csv"
path_to_file3="C:\\Users\\caval\\downloads\\meat_consumption_worldwide.csv"
path_to_file4="C:\\Users\\caval\\downloads\\co2_emission.csv"
import psycopg2
# Connect to an existing database
conn = psycopg2.connect(dbname=dbname, user=user, password=password)
# Open a cursor to perform database operations
cur = conn.cursor()

#gdp

#drop
cur.execute("DROP TABLE gdp; DROP TABLE gdp_3cols; DROP TABLE beef; DROP TABLE sheep; DROP TABLE poultry; DROP TABLE pig; DROP TABLE co2_meat;")
#str: list attr types
x = ""
for i in range (1960,2021): x += ", _"+str(i)+" double precision"
print("x= "+x)
#CREATE TABLE 
cur.execute("CREATE TABLE gdp ( Country_Name text, Country_Code text, \
Indicator_Name text, Indicator_Code text"+x+", error text);")
#file
f = open(path_to_file)
f.readline()
#copy from
#for more information on the python function:
#https://www.psycopg.org/docs/cursor.html#cursor.copy_expert
#postgre statement:
#https://www.postgresql.org/docs/current/static/sql-copy.html
cur.copy_expert("COPY gdp FROM STDIN \
WITH (FORMAT csv)", f)
#SELECT * 
cur.execute("SELECT * FROM gdp;")
print("\t gdp ="+str(cur.fetchmany(3)))
#CREATE TABLE gdp_3cols
cur.execute("CREATE TABLE gdp_3cols \
( Country_Name text, Country_Code text, Year integer, Value double precision );")
#INSERT loop
y = ""
for i in range (1960,2021):
    cur.execute("INSERT INTO gdp_3cols (Country_Name, Country_Code, Year, Value) \
SELECT Country_Name, Country_Code, "+str(i)+", _"+str(i)+" FROM gdp;")
#SELECT *
cur.execute("SELECT * FROM gdp_3cols;")
print("\t gdp_3cols="+str(cur.fetchmany(20)))

#pop growth

#drop
cur.execute("DROP TABLE pop_growth; DROP TABLE pop_growth_4cols;")
#str: list attr types
x = ""
for i in range (1960,2021): x += ", _"+str(i)+" double precision"
print("x= "+x)
#CREATE TABLE
cur.execute("CREATE TABLE pop_growth ( Country_Name text, Country_Code text, \
Indicator_Name text, Indicator_Code text"+x+");")
#file
f2 = open(path_to_file2)
f2.readline()
#copy from
#for more information on the python function:
#https://www.psycopg.org/docs/cursor.html#cursor.copy_expert
#postgre statement:
#https://www.postgresql.org/docs/current/static/sql-copy.html
cur.copy_expert("COPY pop_growth FROM STDIN \
WITH (FORMAT csv)", f2)
#SELECT *
cur.execute("SELECT * FROM pop_growth;")
print("\t pop_growth ="+str(cur.fetchmany(3)))
#CREATE TABLE gdp_3cols
cur.execute("CREATE TABLE pop_growth_4cols \
( Country_Name text, Country_Code text, Year integer, Value double precision );")
#INSERT loop
y2 = ""
for i in range(1960, 2021):
    cur.execute("INSERT INTO pop_growth_4cols (Country_Name, Country_Code, Year, Value) \
SELECT Country_Name, Country_Code, "+str(i)+", _"+str(i)+" FROM pop_growth;")
#SELECT *
cur.execute("SELECT * FROM pop_growth_4cols;")
print("\t pop_growth_4cols="+str(cur.fetchmany(20)))



#drop
cur.execute("DROP TABLE meat_consumption; DROP TABLE meat_consumption_4cols;")
#str: list attr types

#CREATE TABLE
cur.execute("CREATE TABLE meat_consumption (Country_Code text, Subject text,\
Measure text, Year integer, Value double precision);")
#file
f3 = open(path_to_file3)
f3.readline()
#copy from
#for more information on the python function:
#https://www.psycopg.org/docs/cursor.html#cursor.copy_expert
#postgre statement:
#https://www.postgresql.org/docs/current/static/sql-copy.html
cur.copy_expert("COPY meat_consumption FROM STDIN \
WITH (FORMAT csv)", f3)
#SELECT *
cur.execute("SELECT * FROM meat_consumption;")
print("\t meat_consumption ="+str(cur.fetchmany(3)))
#CREATE TABLE gdp_3cols
cur.execute("CREATE TABLE meat_consumption_4cols \
( Country_Code text, Subject text, Year integer, Value double precision );")
#INSERT loop
y3 = ""
#for i in range (1960,2021):
cur.execute("INSERT INTO meat_consumption_4cols (Country_Code, Subject, Year, Value) \
SELECT Country_Code, Subject, Year, Value FROM meat_consumption WHERE Measure = 'THND_TONNE';")
#SELECT *
cur.execute("SELECT * FROM meat_consumption_4cols;")
print("\t meat_consumption_4cols="+str(cur.fetchall()))



#drop
cur.execute("DROP TABLE co2_emissions;")
#str: list attr types

#CREATE TABLE
cur.execute("CREATE TABLE co2_emissions (Country_Name text, Country_Code text,\
Year integer, Value double precision);")
#file
f4 = open(path_to_file4)
f4.readline()
#copy from
#for more information on the python function:
#https://www.psycopg.org/docs/cursor.html#cursor.copy_expert
#postgre statement:
#https://www.postgresql.org/docs/current/static/sql-copy.html
cur.copy_expert("COPY co2_emissions FROM STDIN \
WITH (FORMAT csv)", f4)

#line chart Query

cur.execute("CREATE TABLE co2_meat (Year integer, Country_Code text,\
Beef double precision, Sheep double precision, Poultry double precision, Pig double precision, co2 double precision);")

cur.execute("CREATE TABLE beef (Year integer, Country_Code text,\
Beef double precision);")

cur.execute("INSERT INTO beef (Year, Country_Code, Beef) \
SELECT Year, Country_Code, Value FROM meat_consumption_4cols WHERE Subject = 'BEEF';")

cur.execute("CREATE TABLE sheep (Year integer, Country_Code text,\
Sheep double precision);")

cur.execute("INSERT INTO sheep (Year, Country_Code, Sheep) \
SELECT Year, Country_Code, Value FROM meat_consumption_4cols WHERE Subject = 'SHEEP';")

cur.execute("CREATE TABLE poultry (Year integer, Country_Code text,\
Poultry double precision);")

cur.execute("INSERT INTO poultry (Year, Country_Code, Poultry) \
SELECT Year, Country_Code, Value FROM meat_consumption_4cols WHERE Subject = 'POULTRY';")

cur.execute("CREATE TABLE pig (Year integer, Country_Code text,\
Pig double precision);")

cur.execute("INSERT INTO pig (Year, Country_Code, PIG) \
SELECT Year, Country_Code, Value FROM meat_consumption_4cols WHERE Subject = 'PIG';")

cur.execute("INSERT INTO co2_meat (Year, Country_Code, Beef, Sheep, Poultry, Pig, co2) \
SELECT B.Year, B.Country_Code, B.BEEF, S.SHEEP, Po.POULTRY, Pi.PIG, C.Value         \
FROM beef B LEFT JOIN sheep S ON (B.Country_Code = S.Country_Code AND B.Year = S.Year) LEFT JOIN poultry Po ON \
(Po.Country_Code = S.Country_Code AND Po.Year = S.Year) LEFT JOIN pig Pi ON \
(Pi.Country_Code = Po.Country_Code AND Pi.Year = Po.Year) LEFT JOIN co2_emissions C ON \
(Pi.Country_Code = C.Country_Code AND Pi.Year = C.Year) WHERE (B.Country_Code = 'USA' OR B.Country_Code = 'CHN' \
OR B.Country_Code = 'AUS' OR B.Country_Code = 'RUS' OR B.Country_Code = 'BRA' OR B.Country_Code = 'CAN' \
OR B.Country_Code = 'IND' OR B.Country_Code = 'EGY' OR B.Country_Code = 'CHE' OR B.Country_Code = 'MEX' \
OR B.Country_Code = 'JPN' OR B.Country_Code = 'TUR') AND (B.Year < 2018) ORDER BY B.Country_Code, Year;")

cur.execute("SELECT * FROM co2_meat")

print("\t Gejoined="+str(cur.fetchall()))



# Make the changes to the database persistent
conn.commit()
# Close communication with the database
cur.close()
conn.close()
