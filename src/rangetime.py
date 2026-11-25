import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm


def haeData(url):
    vastaus = requests.get(url, timeout=240, headers={
        "apikey": os.environ.get("APIKEY")
    }
    )

    return vastaus.json()


def raceResults(raceId):
    url = f"https://www.biathlonresults.com/modules/sportapi/api/AnalyticResults?RaceId={raceId}&TypeId=RNGT"

    data = haeData(url)
    
    resultsData = data['Results']
    
    for i in resultsData:
        i['RaceId'] = raceId

    return resultsData


def saveCSV(data, polku):
    data.to_csv(
        f"{Path(__file__).parent.parent}/data/{polku}.csv", index=False)


def main():
    load_dotenv()

    races = pd.read_csv(f"{Path(__file__).parent.parent}/data/kilpaliut2526.csv")
    
    # (races['DisciplineId'] != 'RL')
    
    womenraces = races[(races['DisciplineId'] != 'RL') & 
        (races['DisciplineId'] != 'SR') & (races['catId'] == 'SM')
    ]
    
    raceList = []
    
    for i, rows in tqdm(womenraces.iterrows(), total=len(womenraces)):
        data = raceResults(rows['RaceId'])

        raceList.extend(data)

    normSki = pd.json_normalize(raceList)
    
    normSki = pd.merge(normSki, womenraces[['RaceId', 'km', 'StartTime', 'Description', 'Location', 'altitude']], left_on='RaceId', right_on='RaceId')

    saveCSV(normSki, "skitimemen")
    
    
if __name__ == '__main__':
    main()
