# -*- coding: utf-8 -*-
"""Generoi Microsoft Storen vaatimat laattakuvat.

Piirtaa saman pylvaskuvion kuin sovelluksen oma kuvake, Storen
vaatimissa koissa. Rakenne on otettu mallia MouseJiggler-projektin
msix/make_store_assets.py:sta.

Aja ennen paketointia:
    py store-assets/make_store_assets.py

Vaatii Pillow-kirjaston.
"""
import os

from PIL import Image, ImageDraw

HAKEMISTO = os.path.dirname(os.path.abspath(__file__))

# Samat varit ja mittasuhteet kuin icon-512.png:ssa. Lahdekuvio on
# 200x200 yksikkoa, jotta se vastaa etusivun SVG-tunnusta.
TAUSTA = '#151920'
VIIVAVARI = '#2b323e'
PALKIT = [(53, 114, 24, 48, '#49536b'),
          (88, 88, 24, 74, '#6d7b94'),
          (123, 58, 24, 104, '#b85c5c')]
VIIVA = (40, 168, 120, 3)


def piirra(koko, lapinakyva=False, tayta_reunoihin=True):
    """Piirtaa kuvion neliona. Ylinaytteistys pehmentaa reunat."""
    ss = 4
    w = koko * ss
    kuva = Image.new('RGBA', (w, w), (0, 0, 0, 0))
    d = ImageDraw.Draw(kuva)
    if not lapinakyva:
        sade = int(w * 0.18) if tayta_reunoihin else 0
        d.rounded_rectangle([0, 0, w - 1, w - 1], radius=sade, fill=TAUSTA)
    s = w / 200.0
    for x, y, lev, kork, vari in PALKIT:
        d.rounded_rectangle([x * s, y * s, (x + lev) * s, (y + kork) * s],
                            radius=max(1, int(6 * s)), fill=vari)
    x, y, lev, kork = VIIVA
    d.rounded_rectangle([x * s, y * s, (x + lev) * s, (y + kork) * s],
                        radius=max(1, int(1.5 * s)), fill=VIIVAVARI)
    return kuva.resize((koko, koko), Image.LANCZOS)


def nelio(koko, nimi):
    piirra(koko).save(os.path.join(HAKEMISTO, nimi))
    print('  + %-26s %dx%d' % (nimi, koko, koko))


def levea(leveys, korkeus, nimi):
    """Leveassa laatassa kuvio keskitetaan lapinakyvalle pohjalle."""
    kuvio = piirra(korkeus)
    pohja = Image.new('RGBA', (leveys, korkeus), (0, 0, 0, 0))
    pohja.paste(kuvio, ((leveys - korkeus) // 2, 0), kuvio)
    pohja.save(os.path.join(HAKEMISTO, nimi))
    print('  + %-26s %dx%d' % (nimi, leveys, korkeus))


def main():
    os.makedirs(HAKEMISTO, exist_ok=True)
    # Desktop Bridge- ja PWA-paketin pakolliset ja suositellut koot.
    nelio(44, 'Square44x44Logo.png')
    nelio(71, 'Square71x71Logo.png')
    nelio(150, 'Square150x150Logo.png')
    nelio(310, 'Square310x310Logo.png')
    nelio(50, 'StoreLogo.png')
    levea(310, 150, 'Wide310x150Logo.png')
    print()
    print('Store-laatat luotu kansioon: %s' % HAKEMISTO)


if __name__ == '__main__':
    main()
