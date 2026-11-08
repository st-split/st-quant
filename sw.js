/**
 * ST Quant BackTester - Offline-First Service Worker (sw.js)
 * 역할:
 *  1. 앱 최초 접속/설치 시 index.html, stock_data.js(1.45MB), manifest.json, Chart.js 등을
 *     스마트폰 내부 저장소(CacheStorage)에 영구 저장하여 오프라인(비행기 모드)에서도 즉시 실행되게 합니다.
 *  2. 온라인 상태에서는 스마트폰 내부에 저장된 stock_data.js를 먼저 즉시 로드(0.1초 구동)한 뒤,
 *     백그라운드에서 GitHub 서버의 최신 stock_data.js(update_data.py가 갱신한 데이터)를 가져와
 *     스마트폰 내부 저장소의 stock_data.js를 자동으로 덮어쓰기(갱신)합니다.
 */

const CACHE_NAME = 'st-quant-device-storage-v2';
const CORE_ASSETS = [
  './',
  './index.html',
  './stock_data.js',
  './manifest.json',
  'https://cdn.tailwindcss.com/3.4.17',
  'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js'
];

// 1. 설치(Install) 시 스마트폰 내부 저장소(CacheStorage)에 stock_data.js 및 핵심 파일 전체 저장
self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return Promise.allSettled(
        CORE_ASSETS.map((url) =>
          fetch(url, { cache: 'reload' }).then((resp) => {
            if (resp && resp.ok) {
              return cache.put(url, resp);
            }
          })
        )
      );
    })
  );
});

// 2. 활성화(Activate) 시 이전 버전 캐시 정리 및 즉시 제어권 획득
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.map((k) => {
          if (k !== CACHE_NAME) return caches.delete(k);
        })
      )
    ).then(() => self.clients.claim())
  );
});

// 3. 요청 가로채기(Fetch):
//    stock_data.js 및 index.html은 스마트폰 내부 저장소(Cache)에서 즉시 꺼내 보여주고(Offline-First),
//    동시에 백그라운드에서 GitHub의 최신 파일을 확인해 내부 저장소를 자동 업데이트(Stale-While-Revalidate)합니다.
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  // 외부 실시간 API 조회(Yahoo Finance 등)는 네트워크 우선 처리
  if (url.hostname.includes('yahoo.com') || url.hostname.includes('corsproxy.io') || url.hostname.includes('allorigins.win')) {
    return;
  }

  event.respondWith(
    caches.open(CACHE_NAME).then(async (cache) => {
      const cachedResp = await cache.match(req, { ignoreSearch: true });
      const networkFetch = fetch(req)
        .then((netResp) => {
          if (netResp && netResp.ok && (url.origin === self.location.origin || url.hostname.includes('cdn.'))) {
            cache.put(req, netResp.clone());
          }
          return netResp;
        })
        .catch(() => null);

      // 스마트폰 내부에 저장된 파일이 있으면 0.1초 만에 즉시 반환하고 백그라운드에서 최신본으로 갱신
      if (cachedResp) {
        event.waitUntil(networkFetch);
        return cachedResp;
      }
      const netResp = await networkFetch;
      if (netResp) return netResp;
      return cache.match('./index.html');
    })
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ('focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow('./index.html');
    })
  );
});