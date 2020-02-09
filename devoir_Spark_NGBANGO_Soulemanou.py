# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
import pyspark.sql.functions as f
from pyspark.sql import Window

def main():
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-s', "--school_file", help='School input file', required=True)
    parser.add_argument('-v', "--valeur_fonciere_file", help='Valeur finciere input file', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.school_file,args.valeur_fonciere_file,args.output)


def process(spark, school_file, valeur_fonciere_file, output):
    #Chargement des datasets
    schools = spark.read.option('header', 'true').option('inferSchema', 'true').option('delimiter', ';').csv(school_file)
    vf = spark.read.option('header', 'true').option('inferSchema', 'true').csv(valeur_fonciere_file)
    #renommage de la colonne Code état etablissment
    schools = schools.withColumnRenamed('Code état établissement', 'code_etat_etablissment')
    #suppression des établissement qui ne sont pas "ouverts"
    schools = schools.filter(schools.code_etat_etablissment == 1)
    #Création d'un datastet liant le code postal et le nombre d'établissement par code postal
    w = Window.partitionBy('Code postal')
    schools1 = schools.select('Code postal', f.count('Code postal').over(w).alias('nbre établissments par code postal'))
    schools1 = schools1.select('Code postal', 'nbre établissments par code postal').distinct()

    #filtre des valeurs foncières pour n'avoir des données concernants les habitations de particulier
    vf = vf.filter((vf.type_local == 'Dépendance') | (vf.type_local == 'Appartement') | (vf.type_local == 'Maison'))
    #renommage de la colonne "Code Postal"
    schools1 = schools1.withColumnRenamed('Code postal', 'Code_postal')
    #jointure des deux tables sur la base des codes postaux
    df = vf.join(schools1.hint('broadcast'), vf.code_postal == schools1.Code_postal, 'inner')
    #renommage de la colonne "Code Postal"
    df = df.withColumnRenamed('nbre établissments par code postal', 'nbre_établissments_par_code_postal')
    df = df.drop('Code_postal_schools')
    df.write.parquet(output) 


if __name__ == '__main__':
    main()