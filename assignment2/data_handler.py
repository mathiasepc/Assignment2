import os
import requests
import pandas as pd
from pyspark.sql import SparkSession
#Importere indbygget funktioner.
from pyspark.sql.functions import sum, year
import plotly.graph_objects as go

# Opret en variabel til PySpark-session.
spark = None

def start_spark_session():
    global spark

    # Igangsætter spark.
    spark = SparkSession.builder.getOrCreate()

def extract_data():
    response = requests.get(
        url='https://api.energidataservice.dk/dataset/ElectricitySuppliersPerGridarea?limit=0')
    
    if response.status_code == 200:
        result = response.json()
        records = result.get('records', [])
        return records
    else:
        print(f"An error occurred with status code: {response.status_code}")
        return []

def transform_data(data):
    # Opret en DataFrame fra det eksisterende data
    df = spark.createDataFrame(data)

    # Grupper data efter år
    grouped_df = df.groupBy(year('Month').alias('Year'))

    # Beregn summen af ActiveSupplierPerGridArea for hver gruppe
    aggregated_df = grouped_df.agg(sum('ActiveSupplierPerGridArea').alias('TotalActiveSuppliers'))

    # Konverter DataFrame tilbage til en liste med dictionaries
    transformed_data = [row.asDict() for row in aggregated_df.collect()]

    # Returner det transformerede data som en liste med dictionaries
    return transformed_data

def load_data(data):
    data_dir = 'data'

    #Tjekker om folderen eksisterer.
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Find eksisterende filer der indeholder ActiveSupplierPerGridArea
    existing_files = [f for f in os.listdir(data_dir) if f.startswith('ActiveSupplierPerGridArea')]

    # Henter nummer til fil
    file_number = check_file(existing_files)

    # Opret filnavnet med det genererede nummer
    new_file_name = f'ActiveSupplierPerGridArea{file_number}.csv'
    
    # Opret en DataFrame fra transformerede data
    data_df = spark.createDataFrame(data)

    # Gem data i den nye CSV-fil
    data_df.coalesce(1).write.csv(os.path.join(data_dir, new_file_name), header=True, mode='overwrite')

def print_graph(data):
    img_dir = 'img'

    #Tjekker om folderen eksisterer.
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)

    # Pakker data ud og indsætter i variablerne.
    year, suppliers = zip(*[(row['Year'], row['TotalActiveSuppliers']) for row in data])

    # Find det mindste og største år fra data'en
    start_year = min(year)
    end_year = max(year)
    
    #laver intervallet af mit index til graf. Skal have + 1 ellers crash
    years = list(range(start_year, end_year + 1))

    # Opret en Pandas Series med indeks baseret på år (fra start_year til end_year) 
    pser = pd.Series(suppliers, index=years)

    # Opret et Plotly-linjediagram
    fig = go.Figure(data=go.Scatter(x=years, y=pser.values, mode='lines'))

    # Tilføj aksetitler
    fig.update_layout(
        title='Total Active Suppliers Over Time',
        xaxis_title='Year',
        yaxis_title='TotalActiveSuppliers'
    )

    # Find eksisterende filer der indeholder ActiveSupplierPerGridArea
    existing_files = [f for f in os.listdir(img_dir) if f.startswith('fig')]

    # Henter nummer til fil
    file_number = check_file(existing_files)

    # Gem
    fig.write_image(f"img/fig{file_number}.png")

def check_file(existing_files):
    # Hvis der er eksisterende filer, skal vi generere et nyt filnavn med det næste ledige nummer
    if existing_files:
        #Henter sidste i array
        latest_file = max(existing_files)

        file_number = find_digit(latest_file) + 1

        #finder tallet og plusser med 1. Hver fil får nyt navn
        return file_number
    else:
        file_number = 1
        return file_number 
    
def find_digit(string):
    file_number = 0

    # gennemgår tegnene i string
    for char in string:
        if char.isdigit():
            file_number += int(char)
    
    return file_number
    
def stop_spark_session():
    global spark

    # Stop SparkSession.builder
    spark.stop()