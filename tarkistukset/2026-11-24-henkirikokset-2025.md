# Lähdetarkistus 24.11.2026: henkirikokset 2025

Jatkoa 23.11. tarkistukseen, jossa jäi auki, miksi sivulla on vuoden 2025
henkirikoksille 80, kun Tilastokeskuksen ennakkojulkaisussa on 84.
Sivun tietoihin ei tehty muutoksia.

## Mitä tarkistettiin

StatFin-rajapinta ja stat.fi eivät olleet yöajon ympäristön saatavilla
(verkkoyhteys estetty). Siksi vertailukohtana on repon oman
päivitysskriptin ajo GitHub Actionsissa, joka hakee luvut suoraan
StatFin-taulusta 13gw:

- Ajo: [Päivitä rikostilastot #9, 23.11.2026 klo 21.24 UTC](https://github.com/Isomaha3/rikostilastot/actions/runs/45269275396)
- Taulu: `rpk/13gw.px`, nimikkeet 210101 (tappo), 210201 (murha), 210301 (surma), vain täytetyt teot
- Lokin yhteenveto: `Henkirikokset: 76 (2016) → 80 (2025)`
- Lokin mukaan `DATA.henki.counts` ja `DATA.henki.p100k` olivat ennallaan, eli sivun luvut ovat samat kuin taulussa 13gw ajohetkellä.

| Sarja | Sivun arvo 2025 | StatFin 13gw 23.11.2026 | Ennakkojulkaisu | Tulos |
|---|---|---|---|---|
| Henkirikokset | 80 | 80 | 84 | Sivu täsmää lopulliseen tauluun |

## Johtopäätös

Sivun luku 80 on StatFin-taulun 13gw lopullinen luku. Ero ennakkotietoon (84)
ei ole sivun virhe. Ennakkotiedot tarkentuvat, kun poliisin kirjaukset
päivittyvät. Syytä neljän rikoksen eroon ei voitu tarkistaa tässä ajossa,
koska Tilastokeskuksen sivut eivät olleet saatavilla.

## Avoimeksi jäi

- Tarkista halutessasi selaimessa StatFin-taulusta 13gw, että vuoden 2025
  tappo, murha ja surma ovat yhteensä 80.
- Eurostatin EU-maavertailu henkirikoksista (taulu `crim_hom_soff`) jäi
  tekemättä, koska ec.europa.eu ei ollut yöajon saatavilla.
