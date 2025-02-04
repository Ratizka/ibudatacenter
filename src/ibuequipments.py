import sys
import requests
import pandas as pd
from tqdm import tqdm


def hae_data(url):
    try:
        vastaus = requests.get(url, timeout=240)
        vastaus.raise_for_status()
        return vastaus.json()
    except requests.RequestException as e:
        print(f"Virhe datan haussa: {e}")


def datan_kasittely(data):
    try:
        normalisointi = pd.json_normalize(data["Equipment"])
        normalisointi["IBUId"] = data["IBUId"]
        normalisointi["FullName"] = data["FullName"]
        return (normalisointi)
    except (UnboundLocalError):
        print("Muuttujaan viitataan ennen asettamista")


def tallenna_csv(data, polku):
    try:
        taulukko = pd.DataFrame(data)
        taulukko.to_csv("c:/Users/Lenovo/Documents/" +
                        polku + ".csv", index=False)
        print('CSV-tiedosto luotu onnistuneesti')
    except (FileNotFoundError, PermissionError, ValueError) as e:
        print(f"Virhe CSV:n tallentamisessa: {e}")
        sys.exit(1)


def pelaaja_prosessi(pelaaja_id):
    url = f"https://www.biathlonresults.com/modules/sportapi/api/CISBios?IBUId={pelaaja_id}"
    data = hae_data(url)
    json_data = datan_kasittely(data)
    return json_data


def main():
    df = pd.read_csv(
        "c:/Users/Lenovo/Documents/ibumiehet.csv")

    all_data = []

    pituus = len(df)

    for i, rivi in tqdm(df.iterrows(), total=pituus):
        player_id = str(rivi["IBUId"])
        player_data = pelaaja_prosessi(player_id)
        all_data.append(player_data)

    combined_df = pd.concat(all_data, ignore_index=True)

    tallenna_csv(combined_df, "ibumiehetvalineet")


if __name__ == '__main__':
    main()
    print("Tietojen kerääminen valmistui.")
