import argparse
import hmac
import json
import logging
import os
import re
import sqlite3
import threading
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = 'https://web.cinepax.com'
CINEMA = 'CINEPAX JINNAH PARK'
PKT = timezone(timedelta(hours=5))
TITLE = re.compile(r'\bavengers[\W_]+doomsday\b', re.I)
LOG = logging.getLogger('watcher')


def request(url, payload=None, key=None):
    headers = {'Accept': 'application/json', 'Cache-Control': 'no-cache',
               'User-Agent': 'Cinepax-Jinnah-Park-Ticket-Watcher/1.0'}
    if key:
        headers['apikey'] = key
    if payload is not None:
        headers['Content-Type'] = 'application/json'
    data = None if payload is None else json.dumps(payload).encode()
    with urlopen(Request(url, data=data, headers=headers), timeout=12) as response:
        return json.load(response)


def api(path, **query):
    return request(BASE + '/api/' + path + ('?' + urlencode(query) if query else ''))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sessions(movie, payload, cinema_id, now):
    code = movie['filmCode']
    require(payload['cinemaId'] == cinema_id and payload['filmCode'] == code,
            'showtimes cinema or film mismatch')
    require(isinstance(payload['days'], list), 'unrecognized showtimes calendar')
    for day in payload['days']:
        for group in ('screens', 'lateNightScreens'):
            for screen, row in day[group].items():
                if not movie['allowTicketSales'] or row.get('disabled'):
                    continue
                for slot in row['sessions']:
                    if (slot.get('disabled') or slot.get('soldOut')
                            or slot.get('allowTicketSales') is False):
                        continue
                    session_id = slot['sessionId']
                    require(isinstance(session_id, str) and session_id.isdigit(),
                            'invalid session identifier')
                    seats = slot['seatsAvailable']
                    require(type(seats) is int, 'missing numeric seat count')
                    if seats <= 0:
                        continue
                    start = datetime.fromisoformat(slot['showtime'])
                    start = (start.astimezone(PKT) if start.tzinfo else
                             start.replace(tzinfo=PKT))
                    require(start.date().isoformat() == slot['calendarDate'] and
                            slot['businessDate'] == day['date'], 'session date mismatch')
                    if start <= now:
                        continue
                    fmt = next((value for value in slot['attributes']
                                if value in ('2D', '3D')), row['type'])
                    language = movie.get('language') or next(
                        (value for value in ('Urdu', 'English')
                         if re.search(r'\b' + value + r'\b', movie['title'], re.I)),
                        'unspecified')
                    display = slot['displayTime']
                    require(screen and re.fullmatch(r'\d{1,2}:\d{2} [AP]M', display),
                            'invalid selectable showtime')
                    require(datetime.strptime(display, '%I:%M %p').time() == start.time(),
                            'displayed time differs from session timestamp')
                    booking = BASE + '/pages/booking.html?' + urlencode(
                        dict(filmCode=code, movie=movie['title'], cinema=CINEMA,
                             cinemaId=cinema_id, screen=screen, time=display,
                             sessionId=session_id, rating=movie.get('rating', '')))
                    yield start, session_id, f'{start:%a %d %b %Y, %I:%M %p} PKT | {fmt} / {language}\n{booking}'


def check(get=api, now=None):
    now = now or datetime.now(PKT)
    cinemas = [item for item in get('cinemas') if item['name'] == CINEMA]
    require(len(cinemas) == 1 and cinemas[0]['id'], 'cannot verify Jinnah Park')
    cinema_id = cinemas[0]['id']
    found = {}
    for endpoint in ('movies', 'advance-booking', 'upcoming'):
        rows = get(endpoint, cinemaId=cinema_id)
        require(isinstance(rows, list), f'{endpoint} schema changed')
        for row in rows:
            if endpoint != 'upcoming' or row.get('cinemaId'):
                require(row['cinemaId'] == cinema_id and row['cinemaName'] == CINEMA,
                        f'{endpoint} cinema mismatch')
            if TITLE.search(row['title']):
                code = row['filmCode']
                require(re.fullmatch(r'[A-Za-z0-9_-]+', code), 'invalid film code')
                require(type(row['allowTicketSales']) is bool, 'unknown sales state')
                found.setdefault(code, row)
    matches = {}
    for code, movie in found.items():
        LOG.info('Discovered %s/pages/movie-details.html?%s', BASE,
                 urlencode(dict(filmCode=code, movie=movie['title'])))
        for item in sessions(movie, get('showtimes', cinemaId=cinema_id,
                                        filmCode=code), cinema_id, now):
            matches[item[1]] = item
    ordered = sorted(matches.values())
    LOG.info('%s (%s) verified | %d variants | %d selectable sessions | '
             '%d on December 16-18, 2026', CINEMA, cinema_id, len(found), len(ordered),
             sum('2026-12-16' <= item[0].date().isoformat() <= '2026-12-18'
                 for item in ordered))
    for _, _, detail in ordered:
        LOG.info('%s', detail)
    return ordered


def authorized_command(event, instance, number):
    if event.get('event') != 'messages.upsert' or event.get('instance') != instance:
        return None
    data = event.get('data', {})
    key = data.get('key', {})
    jid = key.get('remoteJid', '')
    alt = key.get('remoteJidAlt', '')
    expected = number + '@s.whatsapp.net'
    authorized = jid == expected or (jid.endswith('@lid') and alt == expected)
    if key.get('fromMe') is not False or not authorized or not key.get('id'):
        return None
    message = data.get('message', {})
    cmd = message.get('conversation') or message.get('extendedTextMessage', {}).get('text')
    if isinstance(cmd, str) and cmd.strip().upper() in (
            'STOP DOOMSDAY', 'START DOOMSDAY', 'STATUS DOOMSDAY'):
        return key['id'], cmd.strip().upper()
    return None


class Watcher:
    def __init__(self, path, deliver):
        self.path = path
        self.deliver = deliver
        self.lock = threading.Lock()
        with closing(self.db()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY)')

    def db(self):
        return sqlite3.connect(self.path)

    def state(self, **changes):
        with closing(self.db()) as db, db:
            db.executemany('INSERT OR REPLACE INTO state VALUES (?, ?)', changes.items())
            return dict(db.execute('SELECT key, value FROM state'))

    def send(self, message):
        try:
            self.deliver(message)
        except Exception as exc:
            error = type(exc).__name__ + (f' HTTP {exc.code}' if hasattr(exc, 'code') else '')
            self.state(last_delivery_error=error)
            LOG.warning('Evolution send failed: %s', error)
        else:
            self.state(last_delivery_error='', last_sent=datetime.now(PKT).isoformat())

    def command(self, message_id, command):
        with self.lock, closing(self.db()) as db:
            with db:
                if not db.execute('INSERT OR IGNORE INTO commands VALUES (?)',
                                  (message_id,)).rowcount:
                    return
                if command != 'STATUS DOOMSDAY':
                    db.execute('INSERT OR REPLACE INTO state VALUES (?, ?)',
                               ('stopped', 'yes' if command == 'STOP DOOMSDAY' else 'no'))
            state = self.state()
            mode = 'stopped' if state.get('stopped') == 'yes' else 'armed'
            LOG.info('Authorized %s: alerts %s', command, mode)
            self.send(f"Doomsday alerts {mode}. Availability: "
                      f"{state.get('availability', 'unknown')}. Last successful check: "
                      f"{state.get('last_success', 'pending')}. Previous send error: "
                      f"{state.get('last_delivery_error') or 'none'}.")

    def tick(self, items):
        with self.lock:
            state = self.state(last_success=datetime.now(PKT).isoformat(),
                               availability=f'{len(items)} bookable sessions')
            if not items or state.get('stopped') == 'yes':
                return
            earliest = items[0][0].date()
            lines = [item[2] for item in items if item[0].date() == earliest]
            # One urgent alert per successful polling interval.
            self.send(f'URGENT: Avengers: Doomsday tickets\n{CINEMA}, Rawalpindi\n\n'
                      + '\n\n'.join(lines[:5]) +
                      f'\n\n{len(items)} confirmed session(s). Reply STOP DOOMSDAY to stop.')


def serve(watcher, token, instance, number):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status):
            self.send_response(status)
            self.end_headers()

        def do_GET(self):
            self.reply(200 if self.path == '/health' else 404)

        def do_POST(self):
            if not hmac.compare_digest(self.path.encode(), ('/whatsapp/' + token).encode()):
                self.reply(404)
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 262144:
                    raise ValueError('invalid size')
                event = json.loads(self.rfile.read(length))
                command = authorized_command(event, instance, number)
            except (ValueError, TypeError, AttributeError):
                self.reply(400)
                return
            if command:
                watcher.command(*command)
            self.reply(204)

    server = ThreadingHTTPServer(('0.0.0.0', 8090), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    watcher = None
    if not args.dry_run:
        import fcntl

        root = Path('/state')
        lease = (root / 'watcher.lock').open('a')
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        instance = os.environ['EVOLUTION_INSTANCE']
        number = os.environ['WHATSAPP_RECIPIENT']
        token = os.environ['WATCHER_WEBHOOK_TOKEN']
        key = os.environ['AUTHENTICATION_API_KEY']
        require(re.fullmatch(r'[1-9]\d{7,14}', number), 'invalid recipient number')
        require(re.fullmatch(r'[a-zA-Z0-9_-]+', instance), 'invalid instance name')
        require(all(re.fullmatch(r'[a-fA-F0-9]{64}', value) for value in (token, key)),
                'expected 64 hexadecimal characters for API and webhook keys')

        def deliver(message):
            result = request('http://evolution:8080/message/sendText/' + instance,
                             {'number': number, 'text': message}, key)
            require(bool(result.get('key', {}).get('id')), 'missing Evolution message ID')

        watcher = Watcher(root / 'watcher.sqlite', deliver)
        serve(watcher, token, instance, number)

    interval = 60
    while True:
        start = time.monotonic()
        try:
            items = check()
        except Exception as exc:
            LOG.warning('check failed/availability unknown: %s', exc)
            if watcher:
                watcher.state(availability='unknown')
            if args.once:
                return 1
            interval = min(interval * 2, 600)
        else:
            if watcher:
                watcher.tick(items)
            if args.once:
                return 0
            interval = 60
        time.sleep(max(0, interval - (time.monotonic() - start)))


if __name__ == '__main__':
    raise SystemExit(main())
