# Author: Migle Simaviciute
# Contact: simaviciut@gmail.com
 
"""Programuotojų užduotys kandidatams 2023

## Pedagogų registras _(junior)_

1. Sutvarkyti pedagogų kvalifikacijos lentelę:
  - Jei tekstiniuose laukuose reikšmė yra `null`, ją pakeisti tekstu "Nenurodyta".
  - Stulpelius pervadinti į `snake_case` stilių.
  - Nustatyti tinkamus stulpelių duomenų tipus.
  - Papildomai atlikti transformacijas, kurios, jūsų manymu, būtų naudingos (pvz., išrinkti tik tokius stulpelius, kurie bus naudojami).

2. Paruošti lentelę, rodančią mokytojų kiekį kiekvienoje savivaldybėje (naudokite tik tokius įrašus, kurių lauko `pd_pareigu_grupe` reikšmė yra "Mokytojai") ir visoje Lietuvoje.

"""
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession
import requests
import re

url_pedagogai = "https://data.gov.lt/dataset/12/download/4468/Pedagogu%20kvalifikacija-12-lt-lt.csv"
url_gyventoju_registras = "https://www.registrucentras.lt/aduomenys/?byla=01_gr_open_amzius_lytis_pilietybes_sav_r1.csv"

spark = SparkSession.builder.appName("Read CSV from URL").getOrCreate()

def csv_to_df(spark, url, delimiter="\t"):
    data = requests.get(url)
    rdd = spark.sparkContext.parallelize(data.iter_lines())
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)
    df = spark.createDataFrame(rdd.map(lambda line: line.decode("utf-8").split(delimiter)), header.decode("utf-8").split(delimiter))
    return df

#updating not only null values, but also falsy values to 'Nenurodyta'
def fix_falsy_values(df):
    for column_name in df.columns:
        df = df.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name)).otherwise('Nenurodyta'))
    return df

def column_name_to_snake_case(df):
    for name in df.columns:
        snake_case_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', re.sub(' ', '_', name)).lower()
        df = df.withColumnRenamed(name, snake_case_name)
    return df

# I only changed those values that I am sure about, like id and sum
def fix_df_pedagogai_types(df):
    return df.withColumn("pd_institucijos_savivaldybės_id", df["pd_institucijos_savivaldybės_id"].cast(IntegerType()))\
        .withColumn("pd_pareigų_grupė_id", df_pedagogai["pd_pareigų_grupė_id"].cast(IntegerType()))\
        .withColumn("pd_pedagogų_skaičius", df_pedagogai["pd_pedagogų_skaičius"].cast(IntegerType()))

#filtering by column and condition, but this function should be implemented more because now it is not protected from mistakes and incorrect arguments
def filter_by_column_and_condition(dataframe, column, filter_condition):
    full_condition = f"`{column}` {filter_condition}"
    filtered_dataframe = dataframe.filter(full_condition)
    return filtered_dataframe

def group_mokytojai_by_savivaldybe(df_pedagogai, df_gr):
    filtered_df = filter_by_column_and_condition(df_pedagogai, "pd_pareigų_grupė", "= 'Mokytojai'")
    teacher_count_df = filtered_df.groupBy("pd_institucijos_savivaldybės_id").agg({"pd_pedagogų_skaičius": "sum"})
    teacher_count_df = teacher_count_df.withColumnRenamed("pd_institucijos_savivaldybės_id", "savivaldybės_id")
    teacher_count_df = teacher_count_df.withColumnRenamed("sum(pd_pedagogų_skaičius)", "mokytojų_skaičius")
    return teacher_count_df

df_pedagogai = csv_to_df(spark, url_pedagogai, "\t")
df_gr = csv_to_df(spark, url_gyventoju_registras, "|")

#1.1
df_pedagogai = fix_falsy_values(df_pedagogai)
#1.2
df_pedagogai = column_name_to_snake_case(df_pedagogai)
#1.3
df_pedagogai = fix_df_pedagogai_types(df_pedagogai)
#1.4
filtered_df = filter_by_column_and_condition(df_pedagogai, "pd_pedagogų_skaičius", "> 1")
filtered_df.show(5)

#2 for this task I have reused function from 1.4
#also there is a mix of languages in function names, but I thought that for this task it would be more clearly this way
sum_of_mokytojai_by_savivaldybe = group_mokytojai_by_savivaldybe(df_pedagogai, df_gr)
sum_of_mokytojai_by_savivaldybe.show()

# this block is from the original code
# Naudojimo pavyzdys:
df_pedagogai.show(3)
df_gr.show(3)
spark.stop()
