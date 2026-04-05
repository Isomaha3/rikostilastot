#!/usr/bin/env python3
"""
update_stats.py — Persutilastot automaattinen päivitys
======================================================
Hakee rikostilastot Tilastokeskuksen PxWeb API:sta ja päivittää
index.html:n JavaScript-arrayt regexillä.

Käyttö:
    python update_stats.py              # Päivittää index.html
    python update_stats.py --dry-run    # Näyttää mitä muuttaisi, ei tallenna

Päivitettävät tilastot (PxWeb API):
  ✓ Väkivaltarikokset (pahoinpitelyt)
  ✓ Henkirikokset (tappo + murha)
  ✓ Ryöstöt
  ✓ Seksuaalirikokset (raiskaukset, lapsiin kohdistuvat, ahdistelu, ym.)
  ✓ Väestö per 100 000 -laskentaan

EI päivitettävissä automaattisesti (manuaalinen lähde):
  ✗ Vangit (Rise — ei API:a)
  ✗ Kansalaisuuskohtaiset tilastot
  ✗ Syntyperä vs. vangit
  ✗ Maahanmuuttajat vs. kantaväestö
  ✗ Perheväkivalta (erillinen julkaisu)
  ✗ Nuorisorikollisuus (ikäryhmädata eri taulussa, monimutkainen)

Lähde: https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/rpk/
"""

import requests
import json
import re
import sys
from datetime import datetime

# ── ASETUKSET ────────────────────────────────────────────

HTML_FILE = 'index.html'
DRY_RUN = '--dry-run' in sys.argv

# Vuodet joita haetaan (10 vuoden aikasarja)
START_YEAR = 2015
# Viimeisin vuosi haetaan dynaamisesti API:sta

# PxWeb API base
PXWEB_BASE = 'https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin'

# Rikostilastotaulu — poliisin tietoon tulleet rikokset
CRIME_TABLE = f'{PXWEB_BASE}/rpk/statfin_rpk_pxt_11cg.px'

# Väestötaulu — vuoden lopun väkiluku
POP_TABLE = f'{PXWEB_BASE}/vaerak/statfin_vaerak_pxt_11ra.px'

# ── TILASTOKESKUKSEN RIKOSNIMIKEKOODIT ───────────────────
# Nämä ovat Tilastokeskuksen PxWeb-taulukon koodeja.
# Jos koodit muuttuvat, aja ensin discover_codes() tarkistaaksesi.

CRIME_CODES = {
    # Pahoinpitelyt yhteensä (RL 21:5-7 + 21:11)
    'pahoinpitely': [
        '0520',  # Pahoinpitely (RL 21:5)
        '0530',  # Törkeä pahoinpitely (RL 21:6)
        '0540',  # Lievä pahoinpitely (RL 21:7)
    ],
    # Henkirikokset (tappo + murha + surma)
    'henkirikos': [
        '0110',  # Tappo (RL 21:1)
        '0120',  # Murha (RL 21:2)
        '0130',  # Surma (RL 21:3)
    ],
    # Ryöstöt
    'ryosto': [
        '1000',  # Ryöstö (RL 31:1)
        '1010',  # Törkeä ryöstö (RL 31:2)
    ],
    # Seksuaalirikokset — raiskaukset
    'raiskaus': [
        '0300',  # Raiskaus (RL 20:1) — vanha laki
        '0310',  # Törkeä raiskaus (RL 20:2) — vanha laki
        '2000',  # Raiskaus (RL 20:1) — uusi laki 2023+
    ],
    # Seksuaalirikokset — lapsiin kohdistuvat
    'lapsi_seksuaali': [
        '0330',  # Lapsen seksuaalinen hyväksikäyttö — vanha
        '0340',  # Törkeä lapsen seks. hyväksikäyttö — vanha
        '2010',  # Seksuaalinen kajoaminen lapseen — uusi 2023+
        '2020',  # Törkeä seksuaalinen kajoaminen lapseen — uusi
        '2030',  # Lapsenraiskaus — uusi 2023+
        '2040',  # Törkeä lapsenraiskaus — uusi 2023+
    ],
    # Seksuaalinen ahdistelu
    'ahdistelu': [
        '0360',  # Seksuaalinen ahdistelu
    ],
    # Koko seksuaalirikosluku (RL 20)
    'seksuaali_yht': [
        '0299',  # Seksuaalirikokset yhteensä (koko 20 luku)
    ],
}

# ── APUFUNKTIOT ──────────────────────────────────────────

def pxweb_query(table_url, variable_code, values, year_start, year_end=None):
    """
    Hae dataa PxWeb API:sta.
    
    Palauttaa dict: {vuosi: arvo} tai {vuosi: {koodi: arvo}}
    """
    years = [str(y) for y in range(year_start, (year_end or datetime.now().year) + 1)]
    
    query = {
        "query": [
            {
                "code": variable_code,
                "selection": {
                    "filter": "item",
                    "values": values
                }
            },
            {
                "code": "Vuosi",
                "selection": {
                    "filter": "item",
                    "values": years
                }
            }
        ],
        "response": {
            "format": "json-stat2"
        }
    }
    
    # Jos taulussa on Tiedot-muuttuja, valitaan "Ilmoitettuja" (tietoon tulleet)
    query["query"].append({
        "code": "Tiedot",
        "selection": {
            "filter": "item",
            "values": ["ilm_rikoksia"]
        }
    })
    
    try:
        r = requests.post(table_url, json=query, timeout=30)
        r.raise_for_status()
        data = r.json()
        return parse_jsonstat2(data, years, values)
    except Exception as e:
        print(f"  API-virhe: {e}")
        # Yritä ilman Tiedot-valintaa
        query["query"] = query["query"][:2]
        try:
            r = requests.post(table_url, json=query, timeout=30)
            r.raise_for_status()
            data = r.json()
            return parse_jsonstat2(data, years, values)
        except Exception as e2:
            print(f"  Toinenkin yritys epäonnistui: {e2}")
            return None


def parse_jsonstat2(data, years, codes):
    """
    Parsii JSON-stat2 -vastauksen.
    Palauttaa: {vuosi: summa} kun koodit summataan yhteen.
    """
    values = data.get('value', [])
    dims = data.get('dimension', {})
    sizes = data.get('size', [])
    
    if not values:
        return None
    
    result = {}
    
    # Yksinkertainen tapaus: yksi koodi per vuosi
    if len(codes) == 1 and len(sizes) <= 3:
        for i, year in enumerate(years):
            if i < len(values) and values[i] is not None:
                result[year] = int(values[i])
        return result
    
    # Monimutkaisempi: useita koodeja, summataan per vuosi
    n_codes = len(codes)
    n_years = len(years)
    
    for yi, year in enumerate(years):
        total = 0
        all_none = True
        for ci in range(n_codes):
            idx = ci * n_years + yi
            if idx < len(values) and values[idx] is not None:
                total += int(values[idx])
                all_none = False
        if not all_none:
            result[year] = total
    
    return result


def get_population(year_start, year_end=None):
    """Hae Suomen väkiluku vuosittain."""
    years = [str(y) for y in range(year_start, (year_end or datetime.now().year) + 1)]
    
    query = {
        "query": [
            {
                "code": "Alue",
                "selection": {
                    "filter": "item",
                    "values": ["SSS"]  # Koko maa
                }
            },
            {
                "code": "Vuosi",
                "selection": {
                    "filter": "item",
                    "values": years
                }
            },
            {
                "code": "Sukupuoli",
                "selection": {
                    "filter": "item",
                    "values": ["SSS"]  # Yhteensä
                }
            }
        ],
        "response": {
            "format": "json-stat2"
        }
    }
    
    try:
        r = requests.post(POP_TABLE, json=query, timeout=30)
        r.raise_for_status()
        data = r.json()
        values = data.get('value', [])
        result = {}
        for i, year in enumerate(years):
            if i < len(values) and values[i] is not None:
                result[year] = int(values[i])
        return result
    except Exception as e:
        print(f"  Väestödata-virhe: {e}")
        return None


def discover_codes(table_url):
    """Listaa taulun muuttujat ja koodit (debug-apufunktio)."""
    try:
        r = requests.get(table_url, timeout=15)
        r.raise_for_status()
        meta = r.json()
        for var in meta.get('variables', []):
            print(f"\n=== {var['code']}: {var['text']} ===")
            for code, text in zip(var['values'][:30], var['valueTexts'][:30]):
                print(f"  {code} = {text}")
            if len(var['values']) > 30:
                print(f"  ... ja {len(var['values']) - 30} muuta")
    except Exception as e:
        print(f"Metadata-virhe: {e}")


def fetch_crime_data(category, year_start, year_end=None):
    """Hae rikosdata tietylle kategorialle."""
    codes = CRIME_CODES.get(category)
    if not codes:
        print(f"  Tuntematon kategoria: {category}")
        return None
    return pxweb_query(CRIME_TABLE, 'Rikosnimike', codes, year_start, year_end)


# ── HTML-PÄIVITYS ────────────────────────────────────────

def format_js_array(values):
    """Muotoile Python-lista JS-arrayksi."""
    return '[' + ','.join(str(v) for v in values) + ']'


def format_js_float_array(values):
    """Muotoile float-lista JS-arrayksi."""
    return '[' + ','.join(f'{v:.2f}' for v in values) + ']'


def update_array_in_html(content, pattern, new_array_str, label):
    """Korvaa JS-array HTML:ssä regexillä."""
    new_content = re.sub(pattern, new_array_str, content)
    if new_content != content:
        print(f"  ✓ Päivitetty: {label}")
        return new_content
    else:
        print(f"  ✗ Ei löytynyt: {label}")
        return content


def update_data_object(content, obj_key, field, new_values, is_float=False):
    """
    Päivitä DATA-objektin kenttä.
    Esim: DATA.vakivalta.counts = [...]
    """
    arr_str = format_js_float_array(new_values) if is_float else format_js_array(new_values)
    # Etsi pattern: field:[...]  DATA-objektin sisältä
    pattern = rf'({obj_key}:\{{[^}}]*{field}:)\[[^\]]+\]'
    new_content = re.sub(pattern, rf'\g<1>{arr_str}', content)
    if new_content != content:
        print(f"  ✓ Päivitetty: DATA.{obj_key}.{field}")
        return new_content
    else:
        print(f"  ✗ Ei löytynyt: DATA.{obj_key}.{field}")
        return content


def update_seksuaali_array(content, var_name, new_values):
    """Päivitä seksuaalirikokset-funktion muuttuja."""
    arr_str = format_js_array(new_values)
    pattern = rf'(const {var_name}=)\[[^\]]+\]'
    new_content = re.sub(pattern, rf'\g<1>{arr_str}', content)
    if new_content != content:
        print(f"  ✓ Päivitetty: seksuaali.{var_name}")
        return new_content
    else:
        print(f"  ✗ Ei löytynyt: seksuaali.{var_name}")
        return content


def update_year_array(content, new_years):
    """Päivitä Y-vuosiarray."""
    arr_str = '[' + ','.join(f"'{y}'" for y in new_years) + ']'
    pattern = r"(const Y=)\[[^\]]+\]"
    new_content = re.sub(pattern, rf"\g<1>{arr_str}", content)
    if new_content != content:
        print(f"  ✓ Päivitetty: Y (vuodet)")
        return new_content
    else:
        print(f"  ✗ Ei löytynyt: Y")
        return content


def update_trend_pct(content, obj_key, first_val, last_val):
    """Päivitä trendPct-arvo."""
    pct = round((last_val - first_val) / first_val * 100, 1)
    pattern = rf'({obj_key}:\{{[^}}]*trendPct:)[^,]+'
    new_content = re.sub(pattern, rf'\g<1>{pct}', content)
    if new_content != content:
        print(f"  ✓ Päivitetty: DATA.{obj_key}.trendPct = {pct}%")
        return new_content
    return content


# ── PÄÄOHJELMA ───────────────────────────────────────────

def main():
    print("=" * 60)
    print("PERSUTILASTOT — Automaattinen päivitys")
    print(f"Aika: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)
    
    if DRY_RUN:
        print("⚠  DRY RUN — ei tallenneta muutoksia\n")
    
    # ── 1. Selvitä viimeisin saatavilla oleva vuosi ──
    print("\n[1/6] Tarkistetaan viimeisin vuosi...")
    test = fetch_crime_data('henkirikos', 2020)
    if not test:
        print("VIRHE: API ei vastaa. Tarkista yhteys ja rikosnimikekoodit.")
        print("Aja: python update_stats.py --discover  koodien tarkistamiseksi")
        sys.exit(1)
    
    latest_year = max(int(y) for y in test.keys())
    years = list(range(START_YEAR, latest_year + 1))
    print(f"  Viimeisin vuosi: {latest_year}")
    print(f"  Aikasarja: {START_YEAR}–{latest_year} ({len(years)} vuotta)")
    
    if len(years) != 10:
        print(f"  ⚠ Dashboard odottaa 10 vuotta, saatavilla {len(years)}")
        print(f"    Säädetään aloitusvuotta: {latest_year - 9}–{latest_year}")
        years = list(range(latest_year - 9, latest_year + 1))
    
    # ── 2. Hae väestödata ──
    print("\n[2/6] Haetaan väestödata...")
    pop = get_population(years[0], latest_year)
    if pop:
        for y in years:
            p = pop.get(str(y), 0)
            if p:
                print(f"  {y}: {p:,}")
    else:
        print("  ⚠ Väestödata ei saatavilla, per 100k ei päivity")
    
    # ── 3. Hae rikosdata ──
    print("\n[3/6] Haetaan rikostilastot...")
    
    print("  Pahoinpitelyt...")
    pahoinpitely = fetch_crime_data('pahoinpitely', years[0], latest_year)
    
    print("  Henkirikokset...")
    henkirikos = fetch_crime_data('henkirikos', years[0], latest_year)
    
    print("  Ryöstöt...")
    ryosto = fetch_crime_data('ryosto', years[0], latest_year)
    
    print("  Raiskaukset...")
    raiskaus = fetch_crime_data('raiskaus', years[0], latest_year)
    
    print("  Lapsiin kohdistuvat seksuaalirikokset...")
    lapsi = fetch_crime_data('lapsi_seksuaali', years[0], latest_year)
    
    print("  Seksuaalinen ahdistelu...")
    ahdistelu = fetch_crime_data('ahdistelu', years[0], latest_year)
    
    print("  Seksuaalirikokset yhteensä...")
    seks_yht = fetch_crime_data('seksuaali_yht', years[0], latest_year)
    
    # ── 4. Koosta arrayt ──
    print("\n[4/6] Koostetaan data-arrayt...")
    
    year_strs = [str(y) for y in years]
    
    def to_array(data_dict):
        """Muunna {vuosi: arvo} dict listaksi."""
        if not data_dict:
            return None
        return [data_dict.get(y, 0) for y in year_strs]
    
    def calc_per100k(counts, pop_dict):
        """Laske per 100 000 asukasta."""
        if not counts or not pop_dict:
            return None
        result = []
        for i, y in enumerate(year_strs):
            p = pop_dict.get(y, 0)
            c = counts[i] if i < len(counts) else 0
            if p > 0:
                result.append(round(c / p * 100000, 2))
            else:
                result.append(0)
        return result
    
    pahoinpitely_arr = to_array(pahoinpitely)
    henkirikos_arr = to_array(henkirikos)
    ryosto_arr = to_array(ryosto)
    raiskaus_arr = to_array(raiskaus)
    lapsi_arr = to_array(lapsi)
    ahdistelu_arr = to_array(ahdistelu)
    seks_yht_arr = to_array(seks_yht)
    
    # Per 100k
    pahoinpitely_p100k = calc_per100k(pahoinpitely_arr, pop)
    henkirikos_p100k = calc_per100k(henkirikos_arr, pop)
    ryosto_p100k = calc_per100k(ryosto_arr, pop)
    
    # Tulosta yhteenveto
    print("\n  Yhteenveto:")
    if pahoinpitely_arr:
        print(f"  Pahoinpitelyt: {pahoinpitely_arr[0]:,} ({years[0]}) → {pahoinpitely_arr[-1]:,} ({latest_year})")
    if henkirikos_arr:
        print(f"  Henkirikokset: {henkirikos_arr[0]} ({years[0]}) → {henkirikos_arr[-1]} ({latest_year})")
    if ryosto_arr:
        print(f"  Ryöstöt:       {ryosto_arr[0]:,} ({years[0]}) → {ryosto_arr[-1]:,} ({latest_year})")
    if seks_yht_arr:
        print(f"  Seksuaalir.:   {seks_yht_arr[0]:,} ({years[0]}) → {seks_yht_arr[-1]:,} ({latest_year})")
    
    # ── 5. Päivitä HTML ──
    print(f"\n[5/6] Päivitetään {HTML_FILE}...")
    
    with open(HTML_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Vuodet
    content = update_year_array(content, year_strs)
    
    # Väkivalta (pahoinpitelyt)
    if pahoinpitely_arr:
        content = update_data_object(content, 'vakivalta', 'counts', pahoinpitely_arr)
        content = update_trend_pct(content, 'vakivalta', pahoinpitely_arr[0], pahoinpitely_arr[-1])
    if pahoinpitely_p100k:
        content = update_data_object(content, 'vakivalta', 'p100k', pahoinpitely_p100k, is_float=True)
    
    # Henkirikokset
    if henkirikos_arr:
        content = update_data_object(content, 'henki', 'counts', henkirikos_arr)
        content = update_trend_pct(content, 'henki', henkirikos_arr[0], henkirikos_arr[-1])
    if henkirikos_p100k:
        content = update_data_object(content, 'henki', 'p100k', henkirikos_p100k, is_float=True)
    
    # Ryöstöt
    if ryosto_arr:
        content = update_data_object(content, 'ryostot', 'counts', ryosto_arr)
        content = update_trend_pct(content, 'ryostot', ryosto_arr[0], ryosto_arr[-1])
    if ryosto_p100k:
        content = update_data_object(content, 'ryostot', 'p100k', ryosto_p100k, is_float=True)
    
    # Seksuaalirikokset
    if raiskaus_arr:
        content = update_seksuaali_array(content, 'R', raiskaus_arr)
    if lapsi_arr:
        content = update_seksuaali_array(content, 'L', lapsi_arr)
    if ahdistelu_arr:
        content = update_seksuaali_array(content, 'A', ahdistelu_arr)
    if seks_yht_arr:
        content = update_seksuaali_array(content, 'T', seks_yht_arr)
    
    # Päivitä data range topbarissa
    content = re.sub(
        r"(id=\"datarange\">)[^<]+(<)",
        rf"\g<1>{years[0]}–{latest_year}\g<2>",
        content
    )
    
    # Päivitä COVERAGE-vuodet
    for panel in ['vakivalta', 'seksuaali', 'henki', 'ryostot']:
        content = re.sub(
            rf"({panel}:\s*\{{years:')[^']+(')",
            rf"\g<1>{years[0]}–{latest_year}\g<2>",
            content
        )
    
    # ── 6. Tallenna ──
    if content == original:
        print("\n  Ei muutoksia — data on ajan tasalla.")
    elif DRY_RUN:
        print("\n  DRY RUN: muutoksia havaittu mutta ei tallennettu.")
        changes = sum(1 for a, b in zip(content, original) if a != b)
        print(f"  ({changes} merkkiä muuttunut)")
    else:
        print(f"\n[6/6] Tallennetaan {HTML_FILE}...")
        with open(HTML_FILE, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ Tallennettu!")
    
    print("\n" + "=" * 60)
    print("Valmis!")
    print("\nMuista: Seuraavat paneelit vaativat manuaalisen päivityksen:")
    print("  • Vangit (Rise)")
    print("  • Kansalaisuus")
    print("  • Syntyperä")
    print("  • Maahanmuuttajat vs. kantaväestö")
    print("  • Perheväkivalta")
    print("  • Nuorisorikollisuus (ikäryhmädata)")
    print("  • 2025 ennakkotiedot")
    print("=" * 60)


if __name__ == '__main__':
    if '--discover' in sys.argv:
        print("Haetaan taulukon metadata...")
        discover_codes(CRIME_TABLE)
    else:
        main()
