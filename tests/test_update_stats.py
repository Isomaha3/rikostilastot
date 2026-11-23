"""Testaa update_stats.py:n ilman verkkoyhteytta.

Ajo: python -m unittest discover tests

Tilastokeskuksen rajapinta korvataan valerajapinnalla, joka vastaa jokaiseen
kyselyyn kelvollisella JSON-stat2-vastauksella. Testit varmistavat, etta
- ehja ajo paivittaa sivun ja jokainen taulukko loytyy,
- puuttuva vuosi, tyhja arvo tai hakuvirhe estaa tallennuksen kokonaan.
"""
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

JUURI = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, JUURI)
import update_stats as us  # noqa: E402

VUODET = [str(y) for y in range(2000, 2026)]
KUUKAUDET = [f'{y}M{m:02d}' for y in (2024, 2025, 2026) for m in range(1, 13)
             if not (y == 2026 and m > 9)]


class Vastaus:
    def __init__(self, data, status=200):
        self._data, self.status_code = data, status

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise us.requests.HTTPError(f'HTTP {self.status_code}')


class Valerajapinta:
    """Vastaa kuten PxWeb. Asetukset muuttavat yhden taulun kayttaytymista."""

    def __init__(self, puuttuva_vuosi=None, tyhja_taulu=None, virhetaulu=None):
        self.puuttuva_vuosi = puuttuva_vuosi    # (taulu, vuosi)
        self.tyhja_taulu = tyhja_taulu          # taulu, jonka arvot ovat null
        self.virhetaulu = virhetaulu            # taulu, joka vastaa 500

    def __call__(self, metodi, url, json=None, **_):
        taulu = url.rsplit('/', 1)[-1]
        if taulu == self.virhetaulu:
            return Vastaus({}, 500)
        if metodi == 'GET':
            vuodet = [v for v in VUODET
                      if (taulu, v) != self.puuttuva_vuosi]
            return Vastaus({'variables': [
                {'code': 'timeperiod_y', 'values': vuodet},
                {'code': 'timeperiod_m', 'values': KUUKAUDET}]})
        ids, koot, ulottuvuudet = [], [], {}
        for q in json['query']:
            arvot = q['selection']['values']
            ids.append(q['code'])
            koot.append(len(arvot))
            ulottuvuudet[q['code']] = {'category': {
                'index': {a: i for i, a in enumerate(arvot)}}}
        n = 1
        for k in koot:
            n *= k
        arvo = None if taulu == self.tyhja_taulu else 100
        aika = [i for i in ids if i.startswith('timeperiod')]
        return Vastaus({'id': ids, 'size': koot, 'dimension': ulottuvuudet,
                        'role': {'time': aika}, 'value': [arvo] * n})


class PaivitysTesti(unittest.TestCase):
    def setUp(self):
        self.kansio = tempfile.mkdtemp()
        self.html = os.path.join(self.kansio, 'index.html')
        shutil.copy(os.path.join(JUURI, 'index.html'), self.html)
        with open(self.html, encoding='utf-8') as f:
            self.alkuperainen = f.read()
        us.HTML_FILE = self.html
        us.DRY_RUN = False
        us.VIRHEET.clear()
        us._YEAR_CACHE.clear()
        us._RAJA_KPL = 10 ** 9          # ei odottelua testeissa
        self.uni = mock.patch.object(us.time, 'sleep')
        self.uni.start()

    def tearDown(self):
        self.uni.stop()
        shutil.rmtree(self.kansio)

    def aja(self, rajapinta):
        with mock.patch.object(us.requests, 'request', rajapinta), \
                mock.patch('sys.stdout'):
            try:
                us.main()
                return 0
            except SystemExit as e:
                return e.code

    def sisalto(self):
        with open(self.html, encoding='utf-8') as f:
            return f.read()

    def test_ehja_ajo_tallentaa(self):
        self.assertEqual(self.aja(Valerajapinta()), None or 0)
        self.assertEqual(us.VIRHEET, [])
        self.assertNotEqual(self.sisalto(), self.alkuperainen)

    def test_puuttuva_vuosi_estaa_tallennuksen(self):
        # Nuorisotaulu ei ole viela julkaissut vuotta 2025
        koodi = self.aja(Valerajapinta(puuttuva_vuosi=('13yq.px', '2025')))
        self.assertEqual(koodi, 1)
        self.assertTrue(any('2025' in v for v in us.VIRHEET))
        self.assertEqual(self.sisalto(), self.alkuperainen)

    def test_tyhja_arvo_estaa_tallennuksen(self):
        koodi = self.aja(Valerajapinta(tyhja_taulu='13jg.px'))
        self.assertEqual(koodi, 1)
        self.assertEqual(self.sisalto(), self.alkuperainen)

    def test_hakuvirhe_estaa_tallennuksen(self):
        koodi = self.aja(Valerajapinta(virhetaulu='13zk.px'))
        self.assertEqual(koodi, 1)
        self.assertEqual(self.sisalto(), self.alkuperainen)


if __name__ == '__main__':
    unittest.main()
