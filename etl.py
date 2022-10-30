import pandas as pd
import requests, json, csv
from sqlalchemy import create_engine
from io import StringIO

### DOCUMENTATION ###
### This file will first fetch data from the api when ran, then prompt the user to indicate what form they would like the data to be
### transformed to(CSV or SQL), then it will create a new column, then print out a summary of the data.


def jsonToCSV(data):
    df = pd.read_json(data.text, orient='records')

    return df.to_csv()

def jsonToSQL(data):

    df = pd.read_json(data.text, orient='records')

    engine = create_engine('sqlite://', echo=False)
    df.to_sql('BeerStyles', con=engine)
    return engine.execute("SELECT * FROM BeerStyles").fetchall()


def get_api_response(url):
    try:
        response = requests.get(url)
    except requests.exceptions.HTTPError as errh:
        return "An Http Error occurred: " + repr(errh)
    except requests.exceptions.ConnectionError as errc:
        return "An Error Connecting to the API occurred: " + repr(errc)
    except requests.exceptions.Timeout as errt:
        return "A Timeout Error occurred: " + repr(errt)
    except requests.exceptions.RequestException as err:
        return "An Unknown Error occurred: " + repr(err)
        
    return response
    

print("Benchmark i.1: Beer Data is fetched from api")
#Source: https://rustybeer.herokuapp.com/docs#/default/styles
beerStylesJSON = get_api_response('https://rustybeer.herokuapp.com/styles')

    
print("Initial json data: ")
print(beerStylesJSON.json())
print("\n")


print("Benchmark i.2 change output form based on user input")

changedForm = input("The beer data is currently in json form. Enter the format(CSV or SQL) you would like to convert it to: ")

while changedForm != "CSV" and changedForm != "SQL":
    changedForm = input("Please enter either CSV or SQL: ")

if changedForm == "CSV":
    beerStylesCSV = jsonToCSV(beerStylesJSON)
    print("New data: ")
    print(beerStylesCSV)

elif changedForm == "SQL":
    beerStylesSQL = jsonToSQL(beerStylesJSON)
    print("New data: ")
    print(beerStylesSQL)


print("\n")

print("Benchmark i.3 Modify columns, add column for range of abv")


df = pd.read_json(beerStylesJSON.text, orient='record')
df['abv_range'] = df.apply(lambda row: row.abv_max - row.abv_min, axis = 1)
print(df['abv_range'])

print("\n")

print("Benchmark i.5 Brief Summary: ")

print("Number of records: " + str(len(df)))

print("Number of columns: " + str(len(df.columns)))













