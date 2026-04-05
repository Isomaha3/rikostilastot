"""
Tilastokeskus PxWeb -päivitysskripti
Hakee uusimmat rikostilastot ja päivittää index.html-tiedoston.

Lähde: Tilastokeskus, rikos- ja pakkokeinotilasto
API: https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/rpk/
"""

import requests
import json
import re
import sys
from datetime import datetime

BASE_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/rpk/"

def hae_pxweb(taulukko, kysely):
    """Hakee dataa Tilastokeskuksen PxWeb-rajapinnasta."""
    url = BASE_URL + taulukko
    try:
        r = requests.post(url, json=kysely, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  Virhe haussa {taulukko}: {e}")
        return None

def hae_pahoinpitelyt():
    """Hakee pahoinpitelyrikokset vuosittain."""
    kysely = {
        "query": [
            {"code": "Rikostyyppi", "selection": {"filter": "item", "values": ["10"]}},
            {"code": "Vuosi", "selection": {"filter": "item", "values": [
                "2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"
            ]}}
        ],
        "response": {"format": "json-stat2"}
    }
    return hae_pxweb("statfin_rpk_pxt_11by.px", kysely)

def hae_henkirikokset():
    """Hakee henkirikokset vuosittain."""
    kysely = {
        "query": [
            {"code": "Rikostyyppi", "selection": {"filter": "item", "values": ["01"]}},
            {"code": "Vuosi", "selection": {"filter": "item", "values": [
                "2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"
            ]}}
        ],
        "response": {"format": "json-stat2"}
    }
    return hae_pxweb("statfin_rpk_pxt_11by.px", kysely)

def hae_ryostot():
    """Hakee ryöstörikokset vuosittain."""
    kysely = {
        "query": [
            {"code": "Rikostyyppi", "selection": {"filter": "item", "values": ["13"]}},
            {"code": "Vuosi", "selection": {"filter": "item", "values": [
                "2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"
            ]}}
        ],
        "response": {"format": "json-stat2"}
    }
    return hae_pxweb("statfin_rpk_pxt_11by.px", kysely)

def hae_seksuaalirikokset():
    """Hakee seksuaalirikokset vuosittain."""
    kysely = {
        "query": [
            {"code": "Rikostyyppi", "selection": {"filter": "item", "values": ["12"]}},
            {"code": "Vuosi", "selection": {"filter": "item", "values": [
                "2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"
            ]}}
        ],
        "response": {"format": "json-stat2"}
    }
    return hae_pxweb("statfin_rpk_pxt_11by.px", kysely)

def poimi_arvot(data):
    """Poimii lukuarvot PxWeb-vastauksesta."""
    if not data or "value" not in data:
        return None
    return [int(v) if v is not None else None for v in data["value"]]

def paivita_html(tiedosto, muutokset):
    """Päivittää JavaScript-taulukot HTML-tiedostossa."""
    with open(tiedosto, "r", encoding="utf-8") as f:
        sisalto = f.read()

    paivityksia = 0
    for avain, uudet_arvot in muutokset.items():
        if uudet_arvot is None:
            print(f"  Ohitetaan {avain} (ei dataa)")
            continue

        arvot_str = ",".join(str(v) for v in uudet_arvot)

        # Etsii esim: counts:[43200,42600,...] tai p100k:[784,773,...]
        pattern = rf'({re.escape(avain)}:\[)[^\]]*(\])'
        uusi = rf'\g<1>{arvot_str}\g<2>'

        uusi_sisalto, n = re.subn(pattern, uusi, sisalto)
        if n > 0:
            sisalto = uusi_sisalto
            paivityksia += n
            print(f"  Päivitetty: {avain} ({n} kpl)")
        else:
            print(f"  Ei löydetty: {avain}")

    with open(tiedosto, "w", encoding="utf-8") as f:
        f.write(sisalto)

    return paivityksia

def main():
    print(f"Tilastopäivitys käynnistetty: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*50)

    # Haetaan data
    print("\nHaetaan Tilastokeskukselta...")

    muutokset = {}

    print("  Pahoinpitelyt...")
    data = hae_pahoinpitelyt()
    arvot = poimi_arvot(data)
    if arvot:
        muutokset["counts"] = arvot  # vakivalta-paneeli
        print(f"    -> {arvot}")

    print("  Henkirikokset...")
    data = hae_henkirikokset()
    arvot = poimi_arvot(data)
    if arvot:
        # Henkirikoksilla oma avain — päivitetään erikseen
        muutokset["henki_counts"] = arvot
        print(f"    -> {arvot}")

    print("  Ryöstöt...")
    data = hae_ryostot()
    arvot = poimi_arvot(data)
    if arvot:
        muutokset["ryostot_counts"] = arvot
        print(f"    -> {arvot}")

    print("  Seksuaalirikokset...")
    data = hae_seksuaalirikokset()
    arvot = poimi_arvot(data)
    if arvot:
        muutokset["seksuaali_total"] = arvot
        print(f"    -> {arvot}")

    if not muutokset:
        print("\nEi dataa haettu — tarkista API-yhteys")
        sys.exit(1)

    # Päivitetään HTML
    print("\nPäivitetään index.html...")
    n = paivita_html("index.html", muutokset)

    print(f"\nValmis! {n} päivitystä tehty.")
    print(f"Päivitetty: {datetime.now().strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    main()
