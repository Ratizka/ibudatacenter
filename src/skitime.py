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
    url = f"https://www.biathlonresults.com/modules/sportapi/api/AnalyticResults?RaceId={raceId}&TypeId=CRST"

    data = haeData(url)
    
    resultsData = data['Results']
    
    for i in resultsData:
        i['RaceId'] = raceId

    return resultsData


def converttoSeconds(df):
    df['km'] = df['km'].str.replace("4x", "").astype(float)
    
    df['Behind'] = df['Behind'].str.replace("+", "")   
    df["Behind"] = df["Behind"].apply(lambda x: x if ":" in x else "0:" + x) 
    df["seconds"] = pd.to_timedelta("0:" + df["Behind"]).dt.total_seconds()
    
    
    df['secondsperkm'] = df['seconds']/df['km']
    
    return df


def groupby(df):
    df = df.groupby(['IBUId', 'Name']).agg(
        races = ('secondsperkm', 'count'), secondsperkmmean = ('secondsperkm', 'mean'), secondsperkmmedian = ('secondsperkm', 'median'), 
        secondsperkmstd = ('secondsperkm', 'std')).reset_index()
    
    return df

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

    finalData = converttoSeconds(normSki)
    
    saveCSV(finalData, "skitimemen")
    
    advancedSkiTime = groupby(finalData)
    
    saveCSV(advancedSkiTime, "skitimeanalysismen")

    
if __name__ == '__main__':
    main()
