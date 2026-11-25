import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm


def haeData(url):
    vastaus = requests.get(url, timeout = 240, headers = {
            "apikey": os.environ.get("APIKEY")
        }
    )
        
    return vastaus.json()
    

def dataParsing(jsonData):
    data = {
        "id": jsonData["IBUId"],
        "etunimi": jsonData["GivenName"],
        "sukunimi": jsonData["FamilyName"],
        "nimi": jsonData["FullName"],
        "syntymaaika": jsonData['Birthdate'],
        "maa": jsonData['NAT'],
        "ampuminen": jsonData["StatShooting"][1],
        "makuu": jsonData["StatShootingProne"][1],
        "pysty": jsonData["StatShootingStanding"][1],
        "hiihtokm": jsonData["StatSkiKMB"][1]
    }
    return data
    

def convertingFormats(data):
    data['ampuminen'] = data['ampuminen'].str.replace(
                "%", "")
    data['pysty'] = data['pysty'].str.replace("%", "")
    data['makuu'] = data['makuu'].str.replace("%", "")
    data['syntymaaika'] = pd.to_datetime(data['syntymaaika']).dt.strftime("%Y-%m-%d")
    
    return data


def tallenna_csv(data, polku):
    data.to_csv(f"{Path(__file__).parent.parent}/data/{polku}.csv", index = False)
   

def athleteSearch(athleteID):
    url = f"https://www.biathlonresults.com/modules/sportapi/api/CISBios?IBUId={athleteID}"
    data = haeData(url)
    jsonData = dataParsing(data)
    return jsonData


def main():
    load_dotenv()
    
    df = haeData("https://www.biathlonresults.com/modules/sportapi/api/Stats?StatisticId=RUNKMB_W&StatId=RUNKMB&byWhat=ATH&SeasonId=2526&OrganizerId=&GenderId=M&IBUId=&Nat=")
    
    data = []

    for i in tqdm(df['Rows'], total = len(df['Rows'])):
        athleteID = str(i["IBUId"])
        
        data.append(athleteSearch(athleteID))

    data = pd.json_normalize(data)
    
    finalData = convertingFormats(data)

    tallenna_csv(finalData, "ibumen")


if __name__ == '__main__':
    main()
