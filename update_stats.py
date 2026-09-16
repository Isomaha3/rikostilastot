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

# Rikostilastotaulu — tietoon tulleet rikokset rikosnimikkeittäin (2006–)
# HUOM: Tilastokeskus vaihtoi taulujen URL-muodon. Vanha muoto
# 'statfin_rpk_pxt_11cg.px' vastaa nykyään HTTP 400, ja taulu 11cg on
# poistettu kokonaan. Nykymuoto on pelkkä taulutunnus.
CRIME_TABLE = f'{PXWEB_BASE}/rpk/13gw.px'

# Väestötaulu — vuoden lopun väkiluku
POP_TABLE = f'{PXWEB_BASE}/vaerak/11ra.px'

# ── TAULUJEN MUUTTUJAKOODIT ──────────────────────────────
# Myös muuttujien nimet vaihtuivat: ennen 'Vuosi' ja 'Rikosnimike'.
CRIME_VAR_CRIME = 'rikokset_15_20190102'   # Rikos
CRIME_VAR_YEAR = 'timeperiod_y'            # Vuosi
CRIME_VAR_AUTH = 'rikokset_34_20180101'    # Viranomainen
CRIME_AUTH_ALL = 'SSS'                     # Viranomaiset yhteensä
CRIME_CONTENT = 'rikokset_lkm'             # Viranomaisten tietoon tulleet rikokset

POP_VAR_AREA = 'alue_23_20260101'          # Alue
POP_AREA_ALL = 'SSS'                       # KOKO MAA
POP_VAR_YEAR = 'timeperiod_y'              # Vuosi
POP_CONTENT = 'vaerak-vaesto'              # Väestö 31.12.

# Nuorisorikollisuus — syylliseksi epäillyt iän mukaan (taulu 13yq, 2010–)
YOUTH_TABLE = f'{PXWEB_BASE}/rpk/13yq.px'
YOUTH_VAR_AREA = 'alue_23_20230101'        # Kunta
YOUTH_VAR_SEX = 'sukupuoli_9_20180101'     # Epäillyn sukupuoli
YOUTH_VAR_AGE = 'ikaryhma_10_20180101'     # Syylliseksi epäillyn ikä
YOUTH_VAR_CRIME = 'rikokset_74_20211209'   # Rikosryhmä
YOUTH_CRIME_RL = '101T504X406'             # 1 RIKOSLAKIRIKOKSET
YOUTH_CONTENT = 'ep_lkm_tork'              # vuoden törkeimmän rikoksen mukaan
YOUTH_AREA_ALL = 'SSS'
YOUTH_SEX_ALL = 'SSS'
YOUTH_AGE_ALL = 'SSS'
YOUTH_AGE_0_17 = '0-17'

# Nuorisotyöttömyys — työvoimatutkimus (taulu tyti/13aj, 2009–)
UNEMP_TABLE = f'{PXWEB_BASE}/tyti/13aj.px'
UNEMP_VAR_SEX = 'sukupuoli_9_20180101'
UNEMP_VAR_AGE = 'ikaryhma_19_20190101'
UNEMP_SEX_ALL = 'SSS'
UNEMP_AGE_YOUTH = '15-24'
UNEMP_AGE_ALL = '15-74'
UNEMP_RATE = 'tyti-Tyottomyysaste'      # Työttömyysaste, %
UNEMP_COUNT = 'tyti-Tyottomat'          # Työttömät, 1000 henkilöä
UNEMP_EMPRATE = 'tyti-Tyollisyysaste'   # Työllisyysaste, %

# Perhe- ja lähisuhdeväkivallan uhrit (13rc) ja uhrien syntyperä (14cf)
PERHE_TABLE = f'{PXWEB_BASE}/rpk/13rc.px'
PERHE_SYNT_TABLE = f'{PXWEB_BASE}/rpk/14cf.px'
PERHE_CONTENT = 'uhri_tork'        # uhrit vakavimman rikoksen mukaan
PERHE_WINDOW = 16                  # dashboardin perhepaneelin pituus

# Epäillyt syntyperän mukaan, väestöön suhteutettuna (13zk)
SYNT_TABLE = f'{PXWEB_BASE}/rpk/13zk.px'
SYNT_VAR = 'syntypera_101_20180101'
SYNT_FOREIGN = '2'                 # Ulkomaalaistaustaiset yhteensä
SYNT_DOMESTIC = '1'                # Suomalaistaustaiset yhteensä
SYNT_CONTENT = 'ep_lkm_vaesto'     # epäillyt väestön 10 000 kohden
SYNT_CRIME_ALL = '101T603'

# Raiskausepäillyt kansalaisuuden mukaan (13je)
RAPE_TABLE = f'{PXWEB_BASE}/rpk/13je.px'
RAPE_VAR_RESID = 'valtio_34_20220101'
RAPE_VAR_CITIZEN = 'valtio_19_20190101'
RAPE_VAR_CRIME = 'rikokset_74_20211209'
RAPE_VAR_AREA = 'alue_23_20230101'
# Vanhan ja uuden lain koodit yhdessä, jotta 2023 lakiuudistus ei katkaise sarjaa
RAPE_CODES = ['234', '239', '232']
RAPE_CONTENT = 'ep_lkm_tork'

# Väestö kansalaisuuden (11rg) ja syntyperän (159s) mukaan
POP_CITIZEN_TABLE = f'{PXWEB_BASE}/vaerak/11rg.px'
POP_ORIGIN_TABLE = f'{PXWEB_BASE}/vaerak/159s.px'
SYNT_WINDOW = 11                   # syntyperäpaneelin pituus

# Kansalaisuuskohtaiset epäillyt väestöön suhteutettuna (13jg)
NAT_TABLE = f'{PXWEB_BASE}/rpk/13jg.px'
NAT_CODES = ['706', '368', '004', '008', '566', '642', '504', '643',
             '233', '752', '246', '764', '276']  # sama järjestys kuin KNAMES
NAT_CONTENT = 'ep_lkm_tork_vaesto'   # kukin epäilty kerran vuodessa
NAT_CRIME = '101T504X406'            # vain rikoslakirikokset

# Rikostyyppiryhmät syntyperävertailuun (13zk)
CRIME_GROUPS = ['201T223', '231T241', '201_202_205', '101T161']
# = Väkivalta, Seksuaali, Henkirikokset, Omaisuus (sama järjestys kuin RTNAMES)

# ── TILASTOKESKUKSEN RIKOSNIMIKEKOODIT ───────────────────
# Koodit ovat muotoa <luku><pykälä><momentti>. Seksuaalirikoslaki uudistui
# 2023, joten aikasarjan jatkuvuuden vuoksi mukana ovat sekä nykyiset koodit
# että vanhan lain '_2022'-päätteiset vastineet.
# Yritykset on jätetty pois — mukana vain täytetyt teot, kuten ennenkin.
# Jos koodit muuttuvat, aja: python update_stats.py --discover

CRIME_CODES = {
    # Pahoinpitelyt yhteensä (RL 21:5-7)
    'pahoinpitely': [
        '210501',  # Pahoinpitely 21:5§1
        '210601',  # Törkeä pahoinpitely 21:6§1
        '210701',  # Lievä pahoinpitely 21:7§
    ],
    # Pahoinpitelyn osat erittelyä varten (summa = pahoinpitely)
    'pahoinpitely_perus': ['210501'],
    'pahoinpitely_torkea': ['210601'],
    'pahoinpitely_lieva': ['210701'],
    # Henkirikokset (tappo + murha + surma)
    'henkirikos': [
        '210101',  # Tappo 21:1§1
        '210201',  # Murha 21:2§1
        '210301',  # Surma 21:3§1
    ],
    # Ryöstöt
    'ryosto': [
        '310101',  # Ryöstö 31:1§1
        '310201',  # Törkeä ryöstö 31:2§1
    ],
    # Raiskaukset — uusi ja vanha laki yhdessä
    'raiskaus': [
        '200101',       # Raiskaus 20:1§1-2 (2023-)
        '200201',       # Törkeä raiskaus 20:2§1 (2023-)
        '200101_2022',  # Raiskaus 20:1§1-2 (-2022)
        '2001A3_2022',  # Raiskaus, 3 mom 20:1§3 (-2022)
        '200201_2022',  # Törkeä raiskaus 20:2§1 (-2022)
    ],
    # Lapsiin kohdistuvat seksuaalirikokset
    'lapsi_seksuaali': [
        '201201',       # Lapsenraiskaus 20:12§1 (2023-)
        '201301',       # Törkeä lapsenraiskaus 20:13§1 (2023-)
        '201401',       # Seksuaalinen kajoaminen lapseen 20:14§1
        '201501',       # Törkeä seksuaalinen kajoaminen lapseen 20:15§1
        '201601',       # Lapsen seksuaalinen hyväksikäyttö 20:16§1
        '200601_2022',  # Lapsen seksuaalinen hyväksikäyttö 20:6§1-2 (-2022)
        '200701_2022',  # Törkeä lapsen seks. hyväksikäyttö 20:7§1 (-2022)
        '2007B1_2022',  # Törkeä lapsenraiskaus 20:7b§1 (-2022)
    ],
    # Seksuaalinen ahdistelu
    'ahdistelu': [
        '200601',       # Seksuaalinen ahdistelu 20:6§1-2 (2023-)
        '2005A1_2022',  # Seksuaalinen ahdistelu 20:5a§1 (-2022)
    ],
    # Koko seksuaalirikosluku (RL 20) — taulun oma summarivi
    'seksuaali_yht': [
        '20LUKU',  # 20 Luku. Seksuaalirikoksista
    ],
}

# ── APUFUNKTIOT ──────────────────────────────────────────

_YEAR_CACHE = {}


def available_years(table_url, year_var):
    """
    Palauttaa taulussa oikeasti olevat vuodet.

    PxWeb vastaa 400, jos kyselyssä pyydetään vuotta jota taulussa ei ole.
    Koska skripti ajetaan vuosittain, pyydetty loppuvuosi on säännöllisesti
    tuoreempi kuin julkaistu data — siksi vuodet suodatetaan metadatan
    perusteella ennen kyselyä.
    """
    key = (table_url, year_var)
    if key in _YEAR_CACHE:
        return _YEAR_CACHE[key]

    found = []
    try:
        r = requests.get(table_url, timeout=30)
        r.raise_for_status()
        for var in r.json().get('variables', []):
            if var.get('code') == year_var:
                found = list(var.get('values', []))
                break
    except Exception as e:
        print(f"  Metadata-virhe: {e}")

    _YEAR_CACHE[key] = found
    return found


def pxweb_query(table_url, variable_code, values, year_start, year_end=None):
    """
    Hae dataa PxWeb API:sta.

    Palauttaa dict: {vuosi: summa}, jossa annetut koodit on laskettu yhteen.
    """
    years = [str(y) for y in range(year_start, (year_end or datetime.now().year) + 1)]

    avail = available_years(table_url, CRIME_VAR_YEAR)
    if avail:
        years = [y for y in years if y in avail]
    if not years:
        print("  Ei pyydettyjä vuosia saatavilla taulussa.")
        return None

    query = {
        "query": [
            {
                "code": variable_code,
                "selection": {"filter": "item", "values": values},
            },
            {
                "code": CRIME_VAR_YEAR,
                "selection": {"filter": "item", "values": years},
            },
            {
                "code": CRIME_VAR_AUTH,
                "selection": {"filter": "item", "values": [CRIME_AUTH_ALL]},
            },
            {
                "code": "contentscode",
                "selection": {"filter": "item", "values": [CRIME_CONTENT]},
            },
        ],
        "response": {"format": "json-stat2"},
    }

    try:
        r = requests.post(table_url, json=query, timeout=30)
        r.raise_for_status()
        return parse_jsonstat2(r.json(), years, values)
    except Exception as e:
        print(f"  API-virhe: {e}")
        return None


def parse_jsonstat2(data, years, codes, as_float=False):
    """
    Parsii JSON-stat2 -vastauksen ja summaa arvot vuosittain.

    Ulottuvuuksien järjestystä ei oleteta: se luetaan vastauksen 'id'- ja
    'size'-kentistä, ja vuosiulottuvuus tunnistetaan 'role.time'-tiedosta.
    Kaikki muut ulottuvuudet (rikosnimikkeet) summataan yhteen.
    """
    values = data.get('value')
    ids = data.get('id') or []
    sizes = data.get('size') or []
    dims = data.get('dimension') or {}

    if values is None or not ids or len(ids) != len(sizes):
        return None

    # Kunkin ulottuvuuden kategoria-avaimet oikeassa järjestyksessä
    keys_per_dim = []
    for did in ids:
        index = dims.get(did, {}).get('category', {}).get('index', {})
        if isinstance(index, dict):
            ordered = sorted(index.items(), key=lambda kv: kv[1])
            keys_per_dim.append([k for k, _ in ordered])
        else:
            keys_per_dim.append(list(index))

    # Vuosiulottuvuus: ensisijaisesti role.time, muuten arvojen perusteella
    year_dim = None
    for did in (data.get('role') or {}).get('time') or []:
        if did in ids:
            year_dim = ids.index(did)
            break
    if year_dim is None:
        for i, keys in enumerate(keys_per_dim):
            if any(k in years for k in keys):
                year_dim = i
                break
    if year_dim is None:
        return None

    # Litteä indeksi -> koordinaatit (row-major)
    strides = [1] * len(sizes)
    for i in range(len(sizes) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    items = values.items() if isinstance(values, dict) else enumerate(values)

    result = {}
    for flat, val in items:
        if val is None:
            continue
        yi = (int(flat) // strides[year_dim]) % sizes[year_dim]
        year = keys_per_dim[year_dim][yi]
        result[year] = result.get(year, 0) + (float(val) if as_float else int(val))

    return result or None


def get_population(year_start, year_end=None):
    """Hae Suomen väkiluku vuosittain."""
    years = [str(y) for y in range(year_start, (year_end or datetime.now().year) + 1)]

    avail = available_years(POP_TABLE, POP_VAR_YEAR)
    if avail:
        years = [y for y in years if y in avail]
    if not years:
        print("  Ei pyydettyjä vuosia saatavilla väestötaulussa.")
        return None

    query = {
        "query": [
            {
                "code": POP_VAR_AREA,
                "selection": {"filter": "item", "values": [POP_AREA_ALL]},
            },
            {
                "code": POP_VAR_YEAR,
                "selection": {"filter": "item", "values": years},
            },
            {
                "code": "contentscode",
                "selection": {"filter": "item", "values": [POP_CONTENT]},
            },
        ],
        "response": {"format": "json-stat2"},
    }

    try:
        r = requests.post(POP_TABLE, json=query, timeout=30)
        r.raise_for_status()
        return parse_jsonstat2(r.json(), years, [POP_AREA_ALL])
    except Exception as e:
        print(f"  Väestödata-virhe: {e}")
        return None


def fetch_youth(age_code, year_start, year_end=None):
    """
    Hae syylliseksi epäillyt ikäryhmittäin (taulu 13yq).

    Rajattu rikoslakirikoksiin ja laskentatapaan "vuoden törkeimmän rikoksen
    mukaan", jolloin kukin epäilty lasketaan vuodessa kerran. Kaikkien
    rikosten ja rikkomusten kokonaisluvussa on vuosien 2020–2021 välillä
    luokitusmuutos, joka tekisi osuusluvusta vertailukelvottoman.
    """
    years = [str(y) for y in range(year_start, (year_end or datetime.now().year) + 1)]

    avail = available_years(YOUTH_TABLE, CRIME_VAR_YEAR)
    if avail:
        years = [y for y in years if y in avail]
    if not years:
        print("  Ei pyydettyjä vuosia saatavilla nuorisotaulussa.")
        return None

    query = {
        "query": [
            {"code": CRIME_VAR_YEAR,
             "selection": {"filter": "item", "values": years}},
            {"code": YOUTH_VAR_AREA,
             "selection": {"filter": "item", "values": [YOUTH_AREA_ALL]}},
            {"code": YOUTH_VAR_SEX,
             "selection": {"filter": "item", "values": [YOUTH_SEX_ALL]}},
            {"code": YOUTH_VAR_AGE,
             "selection": {"filter": "item", "values": [age_code]}},
            {"code": YOUTH_VAR_CRIME,
             "selection": {"filter": "item", "values": [YOUTH_CRIME_RL]}},
            {"code": "contentscode",
             "selection": {"filter": "item", "values": [YOUTH_CONTENT]}},
        ],
        "response": {"format": "json-stat2"},
    }

    try:
        r = requests.post(YOUTH_TABLE, json=query, timeout=30)
        r.raise_for_status()
        return parse_jsonstat2(r.json(), years, [age_code])
    except Exception as e:
        print(f"  Nuorisodata-virhe: {e}")
        return None


def fetch_unemployment(age_code, content_code):
    """
    Hae työvoimatutkimuksen vuositieto (taulu 13aj).

    Työttömyys- ja työllisyysasteet ovat prosenttilukuja, joten ne
    luetaan desimaalilukuina. Kyseessä on otostutkimuksen estimaatti,
    ei työnvälitystilaston rekisteriluku.
    """
    avail = available_years(UNEMP_TABLE, CRIME_VAR_YEAR)
    if not avail:
        return None

    query = {
        "query": [
            {"code": CRIME_VAR_YEAR,
             "selection": {"filter": "item", "values": avail}},
            {"code": UNEMP_VAR_SEX,
             "selection": {"filter": "item", "values": [UNEMP_SEX_ALL]}},
            {"code": UNEMP_VAR_AGE,
             "selection": {"filter": "item", "values": [age_code]}},
            {"code": "contentscode",
             "selection": {"filter": "item", "values": [content_code]}},
        ],
        "response": {"format": "json-stat2"},
    }

    try:
        r = requests.post(UNEMP_TABLE, json=query, timeout=30)
        r.raise_for_status()
        return parse_jsonstat2(r.json(), avail, [age_code], as_float=True)
    except Exception as e:
        print(f"  Työttömyysdata-virhe: {e}")
        return None


def _pxpost(table, query_items, years, as_float=False):
    """Yhteinen PxWeb-POST: vuodet suodatetaan taulun metadatan mukaan."""
    avail = available_years(table, CRIME_VAR_YEAR)
    years = [y for y in years if not avail or y in avail]
    if not years:
        return None
    query = [{"code": CRIME_VAR_YEAR,
              "selection": {"filter": "item", "values": years}}] + query_items
    body = {"query": query, "response": {"format": "json-stat2"}}
    try:
        r = requests.post(table, json=body, timeout=30)
        r.raise_for_status()
        return parse_jsonstat2(r.json(), years, ["x"], as_float=as_float)
    except Exception as e:
        print(f"  Hakuvirhe ({table.rsplit('/', 1)[-1]}): {e}")
        return None


def fetch_perhe_victims(age_code, years):
    """Perhe- ja lähisuhdeväkivallan uhrit (13rc)."""
    return _pxpost(PERHE_TABLE, [
        {"code": "rikokset_77_20220101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "rikokset_114_20190101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": [age_code]}},
        {"code": "rikokset_116_20190101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "rikokset_10_20220101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": [PERHE_CONTENT]}},
    ], years)


def fetch_perhe_origin(origin_code, years):
    """Perheväkivallan uhrit syntyperän mukaan (14cf)."""
    return _pxpost(PERHE_SYNT_TABLE, [
        {"code": "rikokset_77_20220101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "rikokset_116_20190101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "rikokset_10_20220101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": SYNT_VAR, "selection": {"filter": "item", "values": [origin_code]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": [PERHE_CONTENT]}},
    ], years)


def fetch_suspect_rate(origin_code, years):
    """Epäillyt väestön 10 000 kohden syntyperän mukaan (13zk)."""
    return _pxpost(SYNT_TABLE, [
        {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "rikokset_74_20211209", "selection": {"filter": "item", "values": [SYNT_CRIME_ALL]}},
        {"code": SYNT_VAR, "selection": {"filter": "item", "values": [origin_code]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": [SYNT_CONTENT]}},
    ], years, as_float=True)


def fetch_suspect_count(origin_code, years):
    """Epäiltyjen lukumäärä syntyperän mukaan, tapauskerrat (13zk)."""
    return _pxpost(SYNT_TABLE, [
        {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "rikokset_74_20211209", "selection": {"filter": "item", "values": [SYNT_CRIME_ALL]}},
        {"code": SYNT_VAR, "selection": {"filter": "item", "values": [origin_code]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": ["ep_lkm"]}},
    ], years)


def fetch_rape_suspects(citizen_code, years):
    """Raiskausepäillyt kansalaisuuden mukaan (13je)."""
    return _pxpost(RAPE_TABLE, [
        {"code": RAPE_VAR_RESID, "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": RAPE_VAR_CITIZEN, "selection": {"filter": "item", "values": [citizen_code]}},
        {"code": RAPE_VAR_CRIME, "selection": {"filter": "item", "values": RAPE_CODES}},
        {"code": RAPE_VAR_AREA, "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": [RAPE_CONTENT]}},
    ], years)


def fetch_pop_citizen(citizen_code, years):
    """Väestö kansalaisuuden mukaan (11rg)."""
    return _pxpost(POP_CITIZEN_TABLE, [
        {"code": "alue_23_20260101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": RAPE_VAR_CITIZEN, "selection": {"filter": "item", "values": [citizen_code]}},
        {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": ["vaerak-vaesto"]}},
    ], years)


def fetch_pop_origin(origin_code, years):
    """Väestö syntyperän mukaan (159s)."""
    return _pxpost(POP_ORIGIN_TABLE, [
        {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "valtio_19_20190101-vaerak-kansa1", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "valtio_19_20190101-vaerak-svaltio", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": "kieli_15_20180102", "selection": {"filter": "item", "values": ["SSS"]}},
        {"code": SYNT_VAR, "selection": {"filter": "item", "values": [origin_code]}},
        {"code": "contentscode", "selection": {"filter": "item", "values": ["vaerak-vaesto"]}},
    ], years)


def fetch_nationality_rates(year):
    """Epäillyt per 100 000 kansalaisuuden mukaan (13jg)."""
    out = []
    for code in NAT_CODES:
        body = {"query": [
            {"code": CRIME_VAR_YEAR, "selection": {"filter": "item", "values": [year]}},
            {"code": "valtio_19_20190101", "selection": {"filter": "item", "values": [code]}},
            {"code": "rikokset_74_20211209", "selection": {"filter": "item", "values": [NAT_CRIME]}},
            {"code": "alue_23_20230101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "contentscode", "selection": {"filter": "item", "values": [NAT_CONTENT]}},
        ], "response": {"format": "json-stat2"}}
        try:
            r = requests.post(NAT_TABLE, json=body, timeout=30)
            r.raise_for_status()
            v = r.json().get("value", [None])[0]
            out.append(round((v or 0) * 10))
        except Exception as e:
            print(f"  Kansalaisuusdata-virhe ({code}): {e}")
            return None
    return out


def fetch_crime_group_rates(origin_code, year):
    """Epäiltyjen lukumäärä rikostyypeittäin syntyperän mukaan (13zk).

    Lukumäärä eikä valmis suhdeluku, koska taulun suhdeluvun tarkkuus
    (0,1 / 10 000) peittäisi henkirikosten eron kokonaan.
    """
    out = []
    for grp in CRIME_GROUPS:
        body = {"query": [
            {"code": CRIME_VAR_YEAR, "selection": {"filter": "item", "values": [year]}},
            {"code": "sukupuoli_9_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "ikaryhma_10_20180101", "selection": {"filter": "item", "values": ["SSS"]}},
            {"code": "rikokset_74_20211209", "selection": {"filter": "item", "values": [grp]}},
            {"code": SYNT_VAR, "selection": {"filter": "item", "values": [origin_code]}},
            {"code": "contentscode", "selection": {"filter": "item", "values": ["ep_lkm"]}},
        ], "response": {"format": "json-stat2"}}
        try:
            r = requests.post(SYNT_TABLE, json=body, timeout=30)
            r.raise_for_status()
            v = r.json().get("value", [None])[0]
            out.append(int(v or 0))
        except Exception as e:
            print(f"  Rikostyyppidata-virhe ({grp}): {e}")
            return None
    return out

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
    return pxweb_query(CRIME_TABLE, CRIME_VAR_CRIME, codes, year_start, year_end)


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


def format_js_year_array(years):
    """Vuosilaput: ensimmäinen, viimeinen ja tasavuodet kokonaisina."""
    out = []
    for idx, y in enumerate(years):
        full = idx == 0 or idx == len(years) - 1 or int(y) % 5 == 0
        out.append(f"'{y}'" if full else f"'{str(y)[2:]}'")
    return '[' + ','.join(out) + ']'


def update_const_array(content, var_name, values, is_float=False, years=False):
    """Päivitä render-funktion sisäinen const-taulukko."""
    if years:
        arr = format_js_year_array(values)
    elif is_float:
        arr = format_js_float_array(values)
    else:
        arr = format_js_array(values)
    pattern = rf'(const {var_name}=)\[[^\]]+\]'
    new_content = re.sub(pattern, rf'\g<1>{arr}', content)
    if new_content != content:
        print(f"  ✓ Päivitetty: {var_name}")
        return new_content
    print(f"  ✗ Ei löytynyt: {var_name}")
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
    """Päivitä trendPct-arvo ja sitä vastaava trend-nimilappu."""
    pct = round((last_val - first_val) / first_val * 100, 1)
    pattern = rf'({obj_key}:\{{[^}}]*trendPct:)[^,]+'
    new_content = re.sub(pattern, rf'\g<1>{pct}', content)
    if new_content != content:
        print(f"  ✓ Päivitetty: DATA.{obj_key}.trendPct = {pct}%")
        content = new_content

    # Nimilappu pidetään datan mukana, jotta sivulla ei lue 'laskeva'
    # silloin kun luvut nousevat. Aiemmin tämä jäi käsin ylläpidettäväksi
    # ja ehti ajautua ristiriitaan datan kanssa.
    label = 'vakaa' if abs(pct) < 1 else ('nouseva' if pct > 0 else 'laskeva')
    label_pattern = rf"({obj_key}:\{{[^}}]*?trend:')[^']*(')"
    relabeled = re.sub(label_pattern, rf"\g<1>{label}\g<2>", content)
    if relabeled != content:
        print(f"  ✓ Päivitetty: DATA.{obj_key}.trend = {label}")
        content = relabeled

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

    year_strs_pre = [str(y) for y in years]
    print("  Nuoret (alle 18) ja epäillyt yhteensä...")
    nuoriso = fetch_youth(YOUTH_AGE_0_17, years[0], latest_year)
    nuoriso_kaikki = fetch_youth(YOUTH_AGE_ALL, years[0], latest_year)
    pp_perus = fetch_crime_data('pahoinpitely_perus', years[0], latest_year)
    pp_torkea = fetch_crime_data('pahoinpitely_torkea', years[0], latest_year)
    pp_lieva = fetch_crime_data('pahoinpitely_lieva', years[0], latest_year)

    print("  Nuorisotyöttömyys...")
    unemp_rate = fetch_unemployment(UNEMP_AGE_YOUTH, UNEMP_RATE)
    unemp_count = fetch_unemployment(UNEMP_AGE_YOUTH, UNEMP_COUNT)
    unemp_rate_all = fetch_unemployment(UNEMP_AGE_ALL, UNEMP_RATE)
    unemp_emp = fetch_unemployment(UNEMP_AGE_YOUTH, UNEMP_EMPRATE)

    print("  Perheväkivallan uhrit...")
    perhe_years = [str(y) for y in range(latest_year - PERHE_WINDOW + 1, latest_year + 1)]
    perhe_tot = fetch_perhe_victims("SSS", perhe_years)
    perhe_alle18 = fetch_perhe_victims("0-17", perhe_years)
    perhe_ulk = fetch_perhe_origin(SYNT_FOREIGN, perhe_years)
    perhe_kaikki = fetch_perhe_origin("SSS", perhe_years)
    pop_ulk_syntypera = fetch_pop_origin(SYNT_FOREIGN, perhe_years)
    pop_kaikki_syntypera = fetch_pop_origin("SSS", perhe_years)

    print("  Epäillyt syntyperän mukaan...")
    rate_foreign = fetch_suspect_rate(SYNT_FOREIGN, year_strs_pre)
    rate_domestic = fetch_suspect_rate(SYNT_DOMESTIC, year_strs_pre)
    susp_foreign = fetch_suspect_count(SYNT_FOREIGN, year_strs_pre)
    susp_domestic = fetch_suspect_count(SYNT_DOMESTIC, year_strs_pre)

    print("  Raiskausepäillyt kansalaisuuden mukaan...")
    rape_all = fetch_rape_suspects("SSS", year_strs_pre)
    rape_foreign = fetch_rape_suspects("ULK", year_strs_pre)

    print("  Ulkomaan kansalaisten väestöosuus...")
    synt_years = [str(y) for y in range(latest_year - SYNT_WINDOW + 1, latest_year + 1)]
    pop_all_c = fetch_pop_citizen("SSS", synt_years)
    pop_for_c = fetch_pop_citizen("ULK", synt_years)

    print("  Kansalaisuuskohtaiset suhdeluvut...")
    nat_rates = fetch_nationality_rates(str(latest_year))
    rt_foreign_n = fetch_crime_group_rates(SYNT_FOREIGN, str(latest_year))
    rt_domestic_n = fetch_crime_group_rates(SYNT_DOMESTIC, str(latest_year))
    pop_for_o = fetch_pop_origin(SYNT_FOREIGN, [str(latest_year)])
    pop_dom_o = fetch_pop_origin(SYNT_DOMESTIC, [str(latest_year)])
    ly = str(latest_year)
    rt_foreign = ([round(n / pop_for_o[ly] * 100000, 1) for n in rt_foreign_n]
                  if rt_foreign_n and pop_for_o and pop_for_o.get(ly) else None)
    rt_domestic = ([round(n / pop_dom_o[ly] * 100000, 1) for n in rt_domestic_n]
                   if rt_domestic_n and pop_dom_o and pop_dom_o.get(ly) else None)
    
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
    nuoriso_arr = to_array(nuoriso)
    nuoriso_kaikki_arr = to_array(nuoriso_kaikki)
    pp_perus_arr = to_array(pp_perus)
    pp_torkea_arr = to_array(pp_torkea)
    pp_lieva_arr = to_array(pp_lieva)
    unemp_rate_arr = to_array(unemp_rate)
    unemp_count_arr = to_array(unemp_count)
    unemp_rate_all_arr = to_array(unemp_rate_all)
    unemp_emp_arr = to_array(unemp_emp)
    
    # Per 100k
    pahoinpitely_p100k = calc_per100k(pahoinpitely_arr, pop)
    henkirikos_p100k = calc_per100k(henkirikos_arr, pop)
    ryosto_p100k = calc_per100k(ryosto_arr, pop)
    nuoriso_p100k = calc_per100k(nuoriso_arr, pop)

    # Alle 18-vuotiaiden osuus kaikista rikoslakirikoksista epäillyistä
    nuoriso_share = None
    if nuoriso_arr and nuoriso_kaikki_arr:
        nuoriso_share = [
            round(n / k * 100, 1) if k else 0
            for n, k in zip(nuoriso_arr, nuoriso_kaikki_arr)
        ]
    
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
    if pp_perus_arr:
        content = update_data_object(content, 'vakivalta', 'ppPerus', pp_perus_arr)
    if pp_torkea_arr:
        content = update_data_object(content, 'vakivalta', 'ppTorkea', pp_torkea_arr)
    if pp_lieva_arr:
        content = update_data_object(content, 'vakivalta', 'ppLieva', pp_lieva_arr)
    
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

    if nuoriso_arr:
        content = update_data_object(content, 'nuoriso', 'counts', nuoriso_arr)
        content = update_trend_pct(content, 'nuoriso', nuoriso_arr[0], nuoriso_arr[-1])
    if nuoriso_p100k:
        content = update_data_object(content, 'nuoriso', 'p100k', nuoriso_p100k, is_float=True)
    if nuoriso_share:
        content = update_data_object(content, 'nuoriso', 'yShare', nuoriso_share, is_float=True)

    if unemp_rate_arr:
        content = update_data_object(content, 'tyottomyys', 'aste1524', unemp_rate_arr, is_float=True)
        content = update_trend_pct(content, 'tyottomyys', unemp_rate_arr[0], unemp_rate_arr[-1])
    if unemp_count_arr:
        content = update_data_object(content, 'tyottomyys', 'maara1524', unemp_count_arr, is_float=True)
    if unemp_rate_all_arr:
        content = update_data_object(content, 'tyottomyys', 'aste1574', unemp_rate_all_arr, is_float=True)
    if unemp_emp_arr:
        content = update_data_object(content, 'tyottomyys', 'tyollisyys1524', unemp_emp_arr, is_float=True)

    # ── Perheväkivalta (13rc, 14cf, 159s) ──
    if perhe_tot and perhe_alle18:
        pv, pm, py = [], [], []
        for y in perhe_years:
            t, a = perhe_tot.get(y), perhe_alle18.get(y)
            if t is None or a is None:
                continue
            py.append(y)
            pm.append(int(a))
            # PV on uhrien kokonaismäärä; kaavio laskee aikuiset PV - PM
            pv.append(int(t))
        if pv:
            content = update_const_array(content, 'PYR', py, years=True)
            content = update_const_array(content, 'PV', pv)
            content = update_const_array(content, 'PM', pm)
            if perhe_ulk and perhe_kaikki:
                pb = [round(perhe_ulk.get(y, 0) / perhe_kaikki[y] * 100, 1)
                      for y in py if perhe_kaikki.get(y)]
                if len(pb) == len(py):
                    content = update_const_array(content, 'PB', pb, is_float=True)
            if pop_ulk_syntypera and pop_kaikki_syntypera:
                pbv = [round(pop_ulk_syntypera.get(y, 0) / pop_kaikki_syntypera[y] * 100, 1)
                       for y in py if pop_kaikki_syntypera.get(y)]
                if len(pbv) == len(py):
                    content = update_const_array(content, 'PBV', pbv, is_float=True)

    # ── Epäillyt syntyperän mukaan, per 100 000 (13zk) ──
    if rate_foreign and rate_domestic:
        ip = [round(rate_foreign.get(y, 0) * 10) for y in year_strs]
        np_ = [round(rate_domestic.get(y, 0) * 10) for y in year_strs]
        content = update_const_array(content, 'IP', ip)
        content = update_const_array(content, 'NP', np_)

    # Epäiltyjen lukumäärät syntyperän mukaan (13zk, tapauskerrat)
    if susp_foreign and susp_domestic:
        content = update_const_array(content, 'EU', [int(susp_foreign.get(y,0)) for y in year_strs])
        content = update_const_array(content, 'ES', [int(susp_domestic.get(y,0)) for y in year_strs])
    if pop_ulk_syntypera and pop_kaikki_syntypera:
        pbi = [round(pop_ulk_syntypera.get(y,0)/pop_kaikki_syntypera[y]*100,1)
               for y in year_strs if pop_kaikki_syntypera.get(y)]
        if len(pbi) == len(year_strs):
            content = update_const_array(content, 'PBI', pbi, is_float=True)

    # ── Raiskausepäillyt kansalaisuuden mukaan (13je) ──
    if rape_all and rape_foreign:
        ur = [round(rape_foreign.get(y, 0) / rape_all[y] * 100, 1)
              if rape_all.get(y) else 0 for y in year_strs]
        content = update_const_array(content, 'UR', ur, is_float=True)

    # ── Ulkomaan kansalaisten väestöosuus (11rg) ──
    if pop_all_c and pop_for_c:
        uv = [round(pop_for_c.get(y, 0) / pop_all_c[y] * 100, 1)
              if pop_all_c.get(y) else 0 for y in year_strs]
        content = update_const_array(content, 'UV', uv, is_float=True)
        pk = [round(pop_for_c.get(y, 0) / pop_all_c[y] * 100, 1)
              if pop_all_c.get(y) else 0 for y in synt_years]
        content = update_const_array(content, 'PK', pk, is_float=True)
        content = update_const_array(content, 'RY', synt_years, years=True)

    if nat_rates:
        content = update_const_array(content, 'KV', nat_rates)
    if rt_foreign:
        content = update_const_array(content, 'RTU', rt_foreign, is_float=True)
    if rt_domestic:
        content = update_const_array(content, 'RTK', rt_domestic, is_float=True)
    
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
    for panel in ['vakivalta', 'seksuaali', 'henki', 'ryostot', 'nuoriso', 'tyottomyys']:
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
    print("  • 2025 ennakkotiedot")
    print("=" * 60)


if __name__ == '__main__':
    if '--discover' in sys.argv:
        print("Haetaan taulukon metadata...")
        discover_codes(CRIME_TABLE)
    else:
        main()
