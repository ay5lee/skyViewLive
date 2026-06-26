#!/usr/bin/env python3
"""
SkyView Live proxy — local server on port 8765.
- /flights : live ADS-B positions from adsb.lol (global)
- /route   : real-time route lookup with OpenSky validation

Usage:  python3 proxy.py   (Python 3.6+, no extra packages)
"""

import json, math, time, threading, os
import urllib.request, urllib.error, urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT       = 8765
ADSB_URL   = "https://api.adsb.lol/v2/lat/{lat}/lon/{lon}/dist/{dist}"
OPENSKY_URL= "https://opensky-network.org/api/flights/aircraft?icao24={hex}&begin={begin}&end={end}"
ADSBDB_URL = "https://api.adsbdb.com/v0/callsign/{callsign}"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# ICAO -> (IATA, city, lat, lon)
AIRPORTS = {
    'VHHH':('HKG','Hong Kong',    22.3089, 113.9150),
    'VMMC':('MFM','Macau',        22.1496, 113.5916),
    'ZGGG':('CAN','Guangzhou',    23.3924, 113.2990),
    'ZGSZ':('SZX','Shenzhen',     22.6393, 113.8108),
    'ZSPD':('PVG','Shanghai',     31.1434, 121.8052),
    'ZSSS':('SHA','Shanghai',     31.1981, 121.3362),
    'ZBAA':('PEK','Beijing',      40.0799, 116.6031),
    'ZBAD':('PKX','Beijing',      39.5096, 116.4105),
    'ZUCK':('CKG','Chongqing',    29.7192, 106.6417),
    'ZUUU':('CTU','Chengdu',      30.5785, 103.9469),
    'ZPPP':('KMG','Kunming',      25.1019, 102.9292),
    'ZGHA':('CSX','Changsha',     28.1892, 113.2196),
    'ZSXM':('XMN','Xiamen',       24.5440, 118.1277),
    'ZSFZ':('FOC','Fuzhou',       25.9351, 119.6630),
    'ZGSD':('ZUH','Zhuhai',       22.0064, 113.3760),
    'ZLXY':('XIY',"Xi'an",        34.4471, 108.7516),
    'ZHHH':('WUH','Wuhan',        30.7838, 114.2081),
    'ZSQD':('TAO','Qingdao',      36.2661, 120.3745),
    'ZBTJ':('TSN','Tianjin',      39.1244, 117.3464),
    'ZGNN':('NNG','Nanning',      22.6083, 108.1722),
    'RCTP':('TPE','Taipei',       25.0777, 121.2326),
    'RCSS':('TSA','Taipei',       25.0693, 121.5522),
    'RCKH':('KHH','Kaohsiung',    22.5771, 120.3497),
    'RKSI':('ICN','Seoul',        37.4602, 126.4407),
    'RKSS':('GMP','Seoul',        37.5583, 126.7906),
    'RJAA':('NRT','Tokyo',        35.7720, 140.3929),
    'RJTT':('HND','Tokyo',        35.5533, 139.7811),
    'RJBB':('KIX','Osaka',        34.4272, 135.2440),
    'RJFF':('FUK','Fukuoka',      33.5858, 130.4508),
    'VTBS':('BKK','Bangkok',      13.6811, 100.7472),
    'VTBD':('DMK','Bangkok',      13.9126, 100.6066),
    'WSSS':('SIN','Singapore',     1.3644, 103.9915),
    'WMKK':('KUL','Kuala Lumpur',  2.7456, 101.7099),
    'VVTS':('SGN','Ho Chi Minh',  10.8188, 106.6520),
    'VVNB':('HAN','Hanoi',        21.2212, 105.8072),
    'RPLL':('MNL','Manila',       14.5086, 121.0197),
    'WIII':('CGK','Jakarta',      -6.1256, 106.6559),
    'VIDP':('DEL','Delhi',        28.5665,  77.1031),
    'VABB':('BOM','Mumbai',       19.0887,  72.8679),
    'OMDB':('DXB','Dubai',        25.2532,  55.3657),
    'OOMS':('MCT','Muscat',       23.5933,  58.2844),
    'OTBD':('DOH','Doha',         25.2731,  51.6081),
    'OERK':('RUH','Riyadh',       24.9576,  46.6988),
    'OEDF':('DMM','Dammam',       26.4712,  49.7979),
    'EDDF':('FRA','Frankfurt',    50.0333,   8.5706),
    'EGLL':('LHR','London',       51.4775,  -0.4614),
    'LFPG':('CDG','Paris',        49.0097,   2.5478),
    'EHAM':('AMS','Amsterdam',    52.3086,   4.7639),
    'LSZH':('ZRH','Zurich',       47.4647,   8.5492),
    'LIRF':('FCO','Rome',         41.8003,  12.2389),
    'LEMD':('MAD','Madrid',       40.4719,  -3.5626),
    'LTFM':('IST','Istanbul',     41.2753,  28.7519),
    'UUEE':('SVO','Moscow',       55.9726,  37.4146),
    'EDDM':('MUC','Munich',       48.3538,  11.7861),
    'LOWW':('VIE','Vienna',       48.1103,  16.5697),
    'EKCH':('CPH','Copenhagen',   55.6180,  12.6560),
    'EFHK':('HEL','Helsinki',     60.3172,  24.9633),
    'ENGM':('OSL','Oslo',         60.1939,  11.1004),
    'ESSA':('ARN','Stockholm',    59.6519,  17.9186),
    'KATL':('ATL','Atlanta',      33.6367, -84.4281),
    'KLAX':('LAX','Los Angeles',  33.9425,-118.4081),
    'KJFK':('JFK','New York',     40.6413, -73.7781),
    'KORD':('ORD','Chicago',      41.9742, -87.9073),
    'KSFO':('SFO','San Francisco',37.6213,-122.3790),
    'KDFW':('DFW','Dallas',       32.8998, -97.0403),
    'KDEN':('DEN','Denver',       39.8561,-104.6737),
    'KSEA':('SEA','Seattle',      47.4502,-122.3088),
    'KBOS':('BOS','Boston',       42.3656, -71.0096),
    'KIAH':('IAH','Houston',      29.9902, -95.3368),
    'CYVR':('YVR','Vancouver',    49.1939,-123.1844),
    'CYYZ':('YYZ','Toronto',      43.6777, -79.6248),
    'YSSY':('SYD','Sydney',      -33.9399, 151.1753),
    'YMML':('MEL','Melbourne',   -37.6690, 144.8410),
    'YBBN':('BNE','Brisbane',    -27.3842, 153.1175),
    'NZAA':('AKL','Auckland',    -37.0082, 174.7917),
    'FAOR':('JNB','Johannesburg',-26.1392,  28.2460),
    'HAAB':('ADD','Addis Ababa',   8.9779,  38.7993),
    'SBGR':('GRU','Sao Paulo',   -23.4356, -46.4731),
    'SAEZ':('EZE','Buenos Aires',-34.8222, -58.5358),
    'MMMX':('MEX','Mexico City',  19.4363, -99.0721),
}

CACHE_FILE = os.path.join(os.path.dirname(__file__), 'route_cache.json')
_route_cache = {}
_cache_dirty = 0   # count of unsaved new entries

def _load_cache():
    global _route_cache
    try:
        with open(CACHE_FILE, 'r') as f:
            _route_cache = json.load(f)
        print(f"  📂 Loaded {len(_route_cache)} cached routes from disk")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"  ⚠ Could not load route cache: {e}")

def _save_cache():
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(_route_cache, f)
    except Exception as e:
        print(f"  ⚠ Could not save route cache: {e}")


def dist_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon/2)**2)
    return 2 * R * math.asin(math.sqrt(max(0, min(1, a))))


def fetch(url, timeout=8, retries=3):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                wait = 2 ** attempt   # 1s, 2s, 4s
                print(f"  ⏳ 429 from {url[:60]}… retrying in {wait}s")
                time.sleep(wait)
            else:
                raise


def get_opensky_departure(hex24):
    now = int(time.time())
    try:
        flights = fetch(OPENSKY_URL.format(hex=hex24.lower(), begin=now-14400, end=now), timeout=10)
        if isinstance(flights, list) and flights:
            flights.sort(key=lambda x: x.get("firstSeen", 0), reverse=True)
            for f in flights:
                dep = f.get("estDepartureAirport")
                if dep:
                    return dep.upper()
    except Exception:
        pass
    return None


def icao_to_info(icao):
    """Returns (iata, city, lat, lon) or (icao, '', None, None)."""
    if not icao:
        return None, None, None, None
    info = AIRPORTS.get(icao.upper())
    if info:
        return info[0], info[1], info[2], info[3]
    return icao.upper(), '', None, None


def nearest_airport(lat, lon, max_dist_km=150):
    """Return (icao, iata, city, lat, lon) of nearest airport within max_dist_km, or None."""
    best, best_d = None, max_dist_km
    for icao, info in AIRPORTS.items():
        d = dist_km(lat, lon, info[2], info[3])
        if d < best_d:
            best_d = d
            best = (icao, info[0], info[1], info[2], info[3])
    return best


def origin_from_track(hex24):
    """
    Walk the earliest track points for hex24 and return the nearest airport
    if the aircraft started close to one (i.e. we caught it near departure).
    Returns (iata, city, lat, lon) or None.
    """
    track = _tracks.get(hex24.lower() if hex24 else '', [])
    if not track:
        return None
    # Check the first few points — the plane should still be near the airport
    for pt in track[:5]:
        apt = nearest_airport(pt[0], pt[1], max_dist_km=150)
        if apt:
            return apt[1], apt[2], apt[3], apt[4]   # iata, city, lat, lon
    return None


def _correct_origin_async(key, result, hex24, ac_lat, ac_lon):
    """Background thread: validate origin via OpenSky and update cache if stale."""
    def run():
        dep_icao = get_opensky_departure(hex24)
        if dep_icao:
            iata, city, lat, lon = icao_to_info(dep_icao)
            result.update({'orig': iata, 'orig_city': city,
                           'orig_lat': lat, 'orig_lon': lon,
                           'source': 'opensky+adsbdb', 'verified': True})
            print(f"    ✓ {key}: corrected origin → {iata} (OpenSky)")
        else:
            result['verified'] = False
        _route_cache[key] = result
        _persist_cache_if_needed()
    threading.Thread(target=run, daemon=True).start()


def _persist_cache_if_needed():
    global _cache_dirty
    _cache_dirty += 1
    if _cache_dirty >= 10:   # save every 10 new entries
        _cache_dirty = 0
        _save_cache()


def get_route(mkt_flight, icao_callsign=None, hex24=None, ac_lat=None, ac_lon=None):
    key = (mkt_flight or '').upper().strip()
    if not key:
        return None
    if key in _route_cache:
        return _route_cache[key]

    result = None

    # Step 1: adsbdb
    cs = (icao_callsign or key).upper()
    try:
        d  = fetch(ADSBDB_URL.format(callsign=cs))
        fr = d.get('response', {}).get('flightroute')
        if fr and fr.get('origin') and fr.get('destination'):
            o = fr['origin']; de = fr['destination']
            result = {
                'orig':      o.get('iata_code')  or o.get('icao_code'),
                'orig_city': o.get('municipality') or o.get('name', ''),
                'orig_lat':  o.get('latitude'),
                'orig_lon':  o.get('longitude'),
                'dest':      de.get('iata_code') or de.get('icao_code'),
                'dest_city': de.get('municipality') or de.get('name', ''),
                'dest_lat':  de.get('latitude'),
                'dest_lon':  de.get('longitude'),
                'source':    'adsbdb',
                'verified':  False,
            }
    except Exception:
        pass

    # Step 2: verify origin against GPS track (most reliable — uses observed positions)
    if result and hex24:
        track_orig = origin_from_track(hex24)
        if track_orig:
            iata, city, tlat, tlon = track_orig
            claimed_lat = result.get('orig_lat')
            claimed_lon = result.get('orig_lon')
            if claimed_lat is not None:
                d = dist_km(tlat, tlon, claimed_lat, claimed_lon)
                if d > 200:   # track says different airport than adsbdb
                    print(f"    ✓ {key}: track origin {iata} overrides adsbdb {result['orig']}")
                    result.update({'orig': iata, 'orig_city': city,
                                   'orig_lat': tlat, 'orig_lon': tlon,
                                   'source': 'track+adsbdb', 'verified': True})
                else:
                    result['verified'] = True
            else:
                result.update({'orig': iata, 'orig_city': city,
                               'orig_lat': tlat, 'orig_lon': tlon,
                               'source': 'track', 'verified': True})

    # Step 3: geometric sanity check — only flag stale if plane is off the route entirely
    if result and ac_lat is not None and result.get('orig_lat') and not result.get('verified'):
        orig_lat = result['orig_lat']; orig_lon = result['orig_lon']
        dest_lat = result.get('dest_lat'); dest_lon = result.get('dest_lon')
        d_to_orig = dist_km(ac_lat, ac_lon, orig_lat, orig_lon)
        stale = False
        if dest_lat is not None:
            # Plane should be somewhere between origin and destination.
            # If it's further from origin than the total route length it's suspicious.
            route_len = dist_km(orig_lat, orig_lon, dest_lat, dest_lon)
            d_to_dest = dist_km(ac_lat, ac_lon, dest_lat, dest_lon)
            # If the plane is far from BOTH endpoints relative to route length, route is wrong
            if d_to_orig > route_len * 1.3 and d_to_dest > route_len * 1.3:
                stale = True
        else:
            # No destination: only flag if origin is a different continent (>12000km)
            if d_to_orig > 12000:
                stale = True
        if stale and hex24:
            print(f"    ⚠ {key}: plane doesn't lie on {result['orig']}→{result.get('dest','?')} — correcting in background")
            _correct_origin_async(key, result, hex24, ac_lat, ac_lon)
        else:
            result['verified'] = True

    if result is not None:
        _route_cache[key] = result
        _persist_cache_if_needed()
    return result


# ── Track accumulation ────────────────────────────────────────────────────────
# Stores position history per aircraft hex: hex -> [(lat, lon, alt, ts), ...]
# Populated by calls to /flights; trimmed to MAX_TRACK_POINTS per aircraft.

from collections import defaultdict

_tracks = defaultdict(list)
MAX_TRACK_POINTS = 500      # ~2 hrs at 15s refresh
MAX_TRACK_AGE    = 7200     # drop points older than 2 hours


def record_positions(aircraft_list):
    """Called after every successful /flights fetch."""
    now = int(time.time())
    for ac in aircraft_list:
        if ac.get('lat') is None or ac.get('lon') is None:
            continue
        if str(ac.get('alt_baro', '')).lower() == 'ground':
            continue
        hex24 = ac['hex']
        track = _tracks[hex24]
        # Avoid duplicate if position hasn't changed
        if track and track[-1][0] == round(ac['lat'], 4) and track[-1][1] == round(ac['lon'], 4):
            continue
        track.append((round(ac['lat'], 5), round(ac['lon'], 5),
                      ac.get('alt_baro'), now))
        # Trim old points
        cutoff = now - MAX_TRACK_AGE
        _tracks[hex24] = [p for p in track if p[3] >= cutoff][-MAX_TRACK_POINTS:]


def get_track(hex24):
    """Return list of {lat, lon, alt} for a hex."""
    return [{'lat': p[0], 'lon': p[1], 'alt': p[2]} for p in _tracks.get(hex24.lower(), [])]


class ProxyHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self._cors(); self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == '/flights':
            lat  = params.get('lat',  ['22.32'])[0]
            lon  = params.get('lon',  ['114.17'])[0]
            dist = params.get('dist', ['100'])[0]
            try:
                url = ADSB_URL.format(lat=lat, lon=lon, dist=dist)
                body = None
                for attempt in range(3):
                    try:
                        req = urllib.request.Request(url, headers={'User-Agent': UA})
                        with urllib.request.urlopen(req, timeout=10) as r:
                            body = r.read()
                        break
                    except urllib.error.HTTPError as e:
                        if e.code == 429 and attempt < 2:
                            wait = 2 ** attempt
                            print(f"  ⏳ 429 on /flights, retrying in {wait}s")
                            time.sleep(wait)
                        else:
                            raise
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Cache-Control', 'no-store')
                self._cors(); self.end_headers()
                self.wfile.write(body)
                parsed_body = json.loads(body)
                ac_list = parsed_body.get('ac', [])
                record_positions(ac_list)
                print(f"  ✈  {len(ac_list)} aircraft near {lat},{lon}")
            except Exception as e:
                print(f"  ✗  /flights: {e}")
                self._json(502, {'error': str(e)})
            return

        if parsed.path == '/track':
            hex24 = params.get('hex', [''])[0].strip().lower()
            if not hex24:
                self._json(400, {'error': 'missing hex param'}); return
            track = get_track(hex24)
            self._json(200, {'hex': hex24, 'track': track})
            return

        if parsed.path == '/route':
            flight = params.get('flight', [''])[0].strip().upper()
            cs     = params.get('cs',  [''])[0].strip().upper() or flight
            hex24  = params.get('hex', [''])[0].strip().lower()
            try:
                ac_lat = float(params['lat'][0]) if 'lat' in params else None
                ac_lon = float(params['lon'][0]) if 'lon' in params else None
            except (ValueError, IndexError):
                ac_lat = ac_lon = None

            if not flight:
                self._json(400, {'error': 'missing flight param'}); return

            result = get_route(flight, icao_callsign=cs, hex24=hex24,
                               ac_lat=ac_lat, ac_lon=ac_lon)
            self._json(200, result or {})
            if result:
                v = '✓' if result.get('verified') else '?'
                print(f"  🗺  {flight}: {result['orig']}→{result.get('dest','?')} [{result['source']}] {v}")
            else:
                print(f"  🗺  {flight}: no route found")
            return

        self._json(404, {'error': 'not found'})


if __name__ == '__main__':
    _load_cache()
    server = HTTPServer(('', PORT), ProxyHandler)
    print(f"""
╔══════════════════════════════════════════════╗
║        SkyView Live — Local Proxy            ║
╠══════════════════════════════════════════════╣
║  Proxy running at http://localhost:{PORT}      ║
║  Now open index.html in your browser         ║
║  Press Ctrl+C to stop                        ║
╚══════════════════════════════════════════════╝
""")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nSaving route cache…')
        _save_cache()
        print('Proxy stopped.')

