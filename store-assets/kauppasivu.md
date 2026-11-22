# Microsoft Store -kauppasivun tiedot

Kopioi kentät Partner Centeriin. Suomi on ensisijainen kieli, englanti
toissijainen.

---

## Tunnisteet

Nämä tulevat Partner Centeristä, eivät tästä tiedostosta.

| Kenttä | Mistä |
|---|---|
| Package Name | Nimen varauksen jälkeen Product Identity -sivulta |
| Publisher | `CN=67E294FD-0572-4F4E-A93E-8B3F281BAABF` — sama kuin Mouse Jigglerissä |
| Publisher display name | `Isomaha3` |
| Version | `1.0.0.0` |

Publisher-arvot ovat samat kaikissa julkaisemissasi sovelluksissa, joten
ne voi ottaa suoraan `MouseJiggler/msix/AppxManifest.xml`-tiedostosta.

---

## Suomi

**Nimi**

    Suomen rikostilastot

**Lyhyt kuvaus**

    Suomen viralliset rikostilastot yhdessä näkymässä. Luvut tulevat
    suoraan Tilastokeskuksen rajapinnasta ja päivittyvät itsestään.

**Kuvaus**

    Suomen rikostilastot kokoaa viranomaisten julkiset rikosluvut
    yhteen sovellukseen. Kaikki luvut ovat peräisin Tilastokeskuksen
    StatFin-rajapinnasta, Rikosseuraamuslaitokselta ja Eurostatista, ja
    ne päivittyvät automaattisesti kuukausittain.

    Sovellus näyttää kehityksen 18 näkymässä: pahoinpitelyt, henki-
    rikokset, seksuaalirikokset, ryöstöt, varkaudet, huumausaine-
    rikokset, nuorisorikollisuus, perhe- ja lähisuhdeväkivalta,
    vankiluvut, ikäjakaumat sekä EU-vertailu.

    Mitä sovellus tekee toisin:

    • Kertoo mittarin. Rikostilastoissa sama ilmiö näyttää erilaiselta
      sen mukaan lasketaanko rikosilmoituksia, tapauskertoja vai
      henkilöitä. Sovellus merkitsee aina kumpi on kyseessä.

    • Erottaa ilmiöt toisistaan. Esimerkiksi seksuaalirikosten
      kokonaisluku sisältää kaksi hyvin erilaista kehitystä, jotka
      yhteenlaskettuna peittävät toisensa. Ne näytetään erikseen.

    • Merkitsee lainmuutokset. Vuoden 2023 seksuaalirikosuudistuksen
      kaltaiset muutokset katkaisevat aikasarjan vertailukelpoisuuden,
      ja se sanotaan luvun vieressä.

    • Näyttää lähtövuoden vaikutuksen. Sama data antaa eri vastauksen
      riippuen siitä mistä vuodesta katsotaan, joten pitkän aikavälin
      muutos näytetään kahdesta lähtövuodesta.

    • Kertoo mitä luvut eivät kerro. Nämä ovat viranomaisten tietoon
      tulleita rikoksia ja rikoksista epäiltyjä, eivät tuomioita
      eivätkä todellista rikollisuuden määrää.

    Sovellus toimii ilman verkkoyhteyttä, ei kerää käyttäjästä mitään
    tietoja eikä sisällä mainoksia tai ostoksia. Lähdekoodi on
    julkinen.

    Kieli vaihdettavissa suomen ja englannin välillä.

**Avainsanat** (enintään 7)

    rikostilastot
    Tilastokeskus
    Suomi
    tilastot
    rikollisuus
    avoin data
    StatFin

---

## English

**Name**

    Finnish Crime Statistics

**Short description**

    Finland official crime statistics in one place. Figures come
    straight from the Statistics Finland API and update themselves.

**Description**

    Finnish Crime Statistics brings together the public crime figures
    published by Finnish authorities. All numbers come from the
    Statistics Finland StatFin API, the Criminal Sanctions Agency and
    Eurostat, and they update automatically every month.

    Eighteen views cover assault, homicide, sexual offences, robbery,
    theft, narcotics offences, youth crime, domestic violence, prison
    figures, age distributions and an EU comparison.

    What this app does differently:

    • It names the measure. The same phenomenon looks different
      depending on whether you count police reports, case occurrences
      or persons. The app always states which one is shown.

    • It separates phenomena. The sexual offence total, for example,
      contains two very different trends that cancel each other out
      when added together. They are shown separately.

    • It flags changes in law. Reforms such as the 2023 sexual offences
      act break comparability across years, and that is stated next to
      the figure.

    • It shows the effect of the baseline year. The same data gives a
      different answer depending on where you start, so long-term
      change is shown from two baselines.

    • It says what the figures cannot tell you. These are offences
      recorded by the authorities and persons suspected of them, not
      convictions, and not the true amount of crime.

    The app works offline, collects no data about the user, and
    contains no ads or purchases. The source code is public.

    Available in Finnish and English.

**Keywords**

    crime statistics
    Finland
    statistics
    open data
    StatFin
    criminology
    official data

---

## Muut kentät

| Kenttä | Arvo |
|---|---|
| Luokka | Education, alaluokka Reference |
| Tietosuojaseloste | `https://isomaha3.github.io/rikostilastot/tietosuoja.html` |
| Verkkosivu | `https://isomaha3.github.io/rikostilastot/` |
| Hinta | Ilmainen |
| Mainokset | Ei |
| Tukisähköposti | **Päätä itse** — katso huomautus alla |

---

## Kaksi asiaa jotka vaativat sinun päätöksesi

### 1. Tukiyhteystieto on julkinen

Partner Center vaatii tukiyhteystiedon, ja se näkyy kauppasivulla
kenelle tahansa. En täyttänyt siihen sähköpostiosoitettasi, koska et
ole pyytänyt sen julkaisemista. Vaihtoehtoja:

- GitHubin issues-sivu: `https://github.com/Isomaha3/rikostilastot/issues`
- Erillinen osoite jota käytät vain julkaisuihin
- Henkilökohtainen osoitteesi, jos se sinulle sopii

Suosittelen GitHub-linkkiä: se on julkinen muutenkin eikä paljasta
sähköpostiosoitetta.

### 2. Ikärajakysely kannattaa lukea tarkasti

IARC-kysely kysyy viittauksista väkivaltaan ja seksuaaliseen sisältöön.
Sovellus ei kuvaa kumpaakaan, mutta se sisältää tilastokategorioiden
niminä tekstiä kuten "raiskaus", "lapsenraiskaus" ja "lasta
seksuaalisesti esittävä kuva-aineisto".

Vastaa kyselyyn totuudenmukaisesti. Kyse on tilastollisesta
viitetiedosta ilman kuvausta tai kuvamateriaalia, mutta se voi silti
nostaa ikärajaa, ja väärä vastaus on vakavampi ongelma kuin korkeampi
ikäraja. Älä yritä arvata mikä vastaus antaa matalimman rajan.
