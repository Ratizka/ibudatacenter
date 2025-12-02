import requests
import os
import pandas as pd
from tqdm import tqdm


def hae_data(url):
    try:
        vastaus = requests.get(url, timeout=240)
        vastaus.raise_for_status()
        return vastaus.json()
    except requests.RequestException as e:
        print(f"Virhe datan haussa: {e}")


def datan_kasittely(data_json):
    try:
        data = {"id": data_json["IBUId"],
                "etunimi": data_json["GivenName"],
                "sukunimi": data_json["FamilyName"],
                "nimi": data_json["FullName"],
                "syntymaaika": data_json['Birthdate'],
                "maa": data_json['NAT'],
                "ampuminen": data_json["StatShooting"][0],
                "makuu": data_json["StatShootingProne"][0],
                "pysty": data_json["StatShootingStanding"][0],
                "hiihtokm": data_json["StatSkiKMB"][0]}
        return data
    except (UnboundLocalError):
        print("Muuttujaan viitataan ennen asettamista")


def tallenna_csv(data, polku):
    try:
        data['ampuminen'] = data['ampuminen'].str.replace(
            "%", "")
        data['pysty'] = data['pysty'].str.replace("%", "")
        data['makuu'] = data['makuu'].str.replace("%", "")
        data['syntymaaika'] = pd.to_datetime(data['syntymaaika']).dt.strftime(
            "%Y-%m-%d")
        taulukko = pd.DataFrame(data)
        taulukko.to_csv(f"{os.getcwd()}/data/{polku}.csv", index=False)
        print('CSV-tiedosto luotu onnistuneesti')
    except (FileNotFoundError, PermissionError, ValueError) as e:
        print(f"Virhe CSV:n tallentamisessa: {e}")


def pelaaja_prosessi(pelaaja_id):
    url = f"https://www.biathlonresults.com/modules/sportapi/api/CISBios?IBUId={pelaaja_id}"
    data = hae_data(url)
    json_data = datan_kasittely(data)
    return json_data


def main():
    df = pd.read_csv(f"{os.getcwd()}/data/ibuid.csv")

    all_data = []

    pituus = len(df)

    for i, rivi in tqdm(df.iterrows(), total=pituus):
        player_id = str(rivi["IBUId"])
        player_data = pelaaja_prosessi(player_id)
        all_data.append(player_data)

    data = pd.json_normalize(all_data)

    tallenna_csv(data, "ibumiehettilastot")


if __name__ == '__main__':
    main()
