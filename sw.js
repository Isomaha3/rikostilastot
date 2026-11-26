// Suomen rikostilastot - service worker
// Verkko ensin, valimuisti vain varalle. Nain uusi julkaisu nakyy heti
// eika asennettuun sovellukseen jaa vanhaa versiota kiinni.
const CACHE = 'rikostilastot-v6';
const CORE = ['./', './index.html', './manifest.json', './icon-192.png', './icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE)
      .then(c => c.addAll(CORE))
      .catch(() => {})
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  // Itse sivu haetaan aina verkosta HTTP-valimuisti ohittaen. GitHub Pages
  // lahettaa Cache-Control: max-age=600, joten tavallinen fetch voisi
  // palauttaa jopa 10 minuuttia vanhan sivun vaikka strategia on
  // verkko-ensin. Muut tiedostot (kuvakkeet) haetaan normaalisti.
  const isPage = req.mode === 'navigate' || req.destination === 'document';
  const hae = isPage ? fetch(new Request(req.url, { cache: 'no-store' })) : fetch(req);
  e.respondWith(
    hae
      .then(res => {
        // Vain onnistuneet oman sivuston vastaukset talteen. Muuten yksi
        // 404 tai palvelinvirhe korvaisi toimivan offline-version.
        if (res.ok && res.type === 'basic') {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(req, copy)).catch(() => {});
        }
        return res;
      })
      .catch(() => caches.match(req).then(r => r || caches.match('./index.html')))
  );
});
