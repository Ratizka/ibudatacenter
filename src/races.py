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


def saveCSV(data, polku):
    data.to_csv(
        f"{Path(__file__).parent.parent}/data/{polku}.csv", index=False)


def athleteSearch(eventID, altitude):
    url = f"https://www.biathlonresults.com/modules/sportapi/api/Competitions?EventId={eventID}"
    
    data = haeData(url)
    
    for i in data:
        i['altitude'] = altitude
    
    return data


def main():
    load_dotenv()

    jsonData = haeData(
        "https://www.biathlonresults.com/modules/sportapi/api/Events?SeasonId=2526&Level=1")

    normData = pd.json_normalize(jsonData)
    
    raceList = []
    
    for i, rows in tqdm(normData.iterrows(), total = len(normData)):
        data = athleteSearch(rows['EventId'], rows['Altitude'])
        
        raceList.extend(data)

    saveCSV(pd.json_normalize(raceList), "kilpaliut2526")


if __name__ == '__main__':
    main()
