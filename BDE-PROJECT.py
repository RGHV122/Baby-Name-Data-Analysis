import pandas as pd
import os


#import data
current_dir = os.getcwd()
filename  = 'nat1900-2017.tsv'
path = os.path.join(current_dir,filename)
dataset = pd.read_csv(path,delimiter = '\t')

#preprocessing
column_name = ['sex','name','year','frequency']
dataset.columns = column_name
dataset = dataset[dataset.year != 'XXXX']
dataset = dataset.reset_index(drop=True)

#create new file
newFileName = 'finalData.csv'
dataset.to_csv(newFileName)
newpath = os.path.join(current_dir,newFileName)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Analyse').config("spark.some.config.option", "some-value").getOrCreate()

#read data
df = spark.read.csv(newpath,header=True,sep=',')


from pyspark.sql.types import DecimalType
import pyspark.sql.functions as F


#Q1 max used male  name
newdf = df.filter("sex==1")
newdf  = newdf['name','frequency']
newdf = newdf.groupBy('name').agg(F.sum('frequency'))

newdf = newdf.withColumn("sum(frequency)",newdf["sum(frequency)"].cast(DecimalType()))

max_frequency = newdf.select([F.max("sum(frequency)")]).collect()[0]['max(sum(frequency))']
most_used_name = newdf.where(newdf['sum(frequency)'] == max_frequency).collect()[0]['name']
print('Most used male name is ',most_used_name,'and it is used',max_frequency,'times')


#Q2 max used female  name
newdf = df.filter("sex==2")
newdf  = newdf['name','frequency']
newdf = newdf.groupBy('name').agg(F.sum('frequency'))
newdf = newdf.withColumn("sum(frequency)",newdf["sum(frequency)"].cast(DecimalType()))
max_frequency = newdf.select([F.max("sum(frequency)")]).collect()[0]['max(sum(frequency))']
most_used_name = newdf.where(newdf['sum(frequency)'] == max_frequency).collect()[0]['name']
print('Most used female name is ',most_used_name,'and it is used',max_frequency,'times')


#Q3 max used name in of a particular year for male
newdff = df.filter("sex==1")
male_name_by_year = {}
for year in range(1900,2018):
    newdf = newdff.filter("year == "+str(year))
    newdf  = newdf['name','frequency']
    try:
        max_frequency = newdf.select([F.max("frequency")]).collect()[0]['max(frequency)']
        most_used_name = newdf.where(newdf['frequency'] == max_frequency).collect()[0]['name']
        male_name_by_year[year] = [most_used_name,max_frequency]
    except:
        pass

print(male_name_by_year)

#Q4 max used name in of a particular year for fwmale
newdff = df.filter("sex==2")
female_name_by_year={}
for year in range(1900,2018):
    newdf = newdff.filter("year == "+str(year))
    newdf  = newdf['name','frequency']
    try:
        max_frequency = newdf.select([F.max("frequency")]).collect()[0]['max(frequency)']
        most_used_name = newdf.where(newdf['frequency'] == max_frequency).collect()[0]['name']
        female_name_by_year[year] = [most_used_name,max_frequency]
    except:
        pass
print(female_name_by_year)


#Q5 name used in a year by order by their frequency
year = 1900
newdf= df['name','frequency','year']
newdf = newdf.filter("year == "+str(year))
newdf= newdf['name','frequency']
newdf = df.select(newdf.name,df.frequency.cast("int"))
#df.frequency = df.frequency.astype(int)
print(newdf.dtypes)
newdf = newdf['name','frequency'].sort('frequency')
newdf.show()


