"""Hosted watcher with outgoing HTTPS only and persistent phone acknowledgement."""
import argparse
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urlencode

from doomsday_watch import CINEMA, LOG, PKT, Watcher, check, request
from datetime import datetime

API = 'https://api.pushover.net/1'


class Push:
    def __init__(self, token, user):
        self.token = token
        self.user = user

    def call(self, path, payload=None):
        if payload is None:
            result = request(f'{API}/{path}?' + urlencode({'token': self.token}))
        else:
            result = request(f'{API}/{path}', {'token': self.token, **payload})
        if result.get('status') != 1:
            raise ValueError('Pushover rejected the request')
        return result

    def send(self, title, text, url='', **options):
        return self.call('messages.json', dict(user=self.user, title=title,
                         message=text, url=url, **options))

    def receipt(self, receipt):
        return self.call(f'receipts/{receipt}.json')

    def cancel(self, receipt):
        self.call(f'receipts/{receipt}/cancel.json', {})


def update(state, push, items):
    previous = state.state()
    receipt = previous.get('receipt')
    if receipt:
        status = push.receipt(receipt)
        if status['acknowledged']:
            state.state(stopped='yes', receipt='', session='')
            LOG.info('Phone acknowledged the alert; watcher is stopped')
            return
        if status['expired']:
            state.state(receipt='', session='')
            receipt = None

    if previous.get('stopped') == 'yes':
        return

    earliest = items[0] if items else None
    if receipt:
        if earliest and previous['session'] == earliest[1]:
            return
        if previous['session']:
            push.cancel(receipt)
        # Retain the receipt until the next poll so a racing acknowledgement
        # can still be observed before a replacement alert is sent.
        state.state(session='')
        return
    if earliest:
        _, session_id, detail = earliest
        text, url = detail.splitlines()
        result = push.send('Avengers: Doomsday tickets', f'{CINEMA}, Rawalpindi\n{text}',
                           url=url, url_title='Book on Cinepax',
                           priority=2, retry=60, expire=120)
        if not re.fullmatch(r'[A-Za-z0-9]{30}', result.get('receipt', '')):
            raise ValueError('Pushover returned an invalid receipt')
        state.state(receipt=result['receipt'], session=session_id,
                    last_sent=datetime.now(PKT).isoformat())


def poll(state, push):
    try:
        items = check()
    except Exception as exc:
        LOG.warning('check failed/availability unknown: %s', exc)
        state.state(availability='unknown')
        items = None
    else:
        state.state(availability=f'{len(items)} bookable sessions',
                    last_success=datetime.now(PKT).isoformat())
    try:
        update(state, push, items)
    except (OSError, ValueError, KeyError) as exc:
        error = type(exc).__name__
        if hasattr(exc, 'code'):
            error += f' HTTP {exc.code}'
        LOG.warning('Pushover request failed: %s', error)
        state.state(last_delivery_error=error)
    else:
        state.state(last_delivery_error='')
    return items is not None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-push', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    push = Push(os.environ['PUSHOVER_APP_TOKEN'], os.environ['PUSHOVER_USER_KEY'])
    if args.test_push:
        push.send('Doomsday watcher test', 'Your Pushover connection works.')
        return

    import fcntl

    root = Path(os.environ.get('WATCHER_STATE_DIR', '/state'))
    railway_volume = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
    if not root.is_dir() or not (str(root) == railway_volume or os.path.ismount(root)):
        raise SystemExit('Mount a persistent volume at WATCHER_STATE_DIR before starting.')
    with (root / 'push.lock').open('a') as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = Watcher(root / 'push.sqlite', None)
        generation = os.environ.get('WATCHER_ARM_GENERATION', '1')
        previous = state.state()
        if previous.get('arm_generation') != generation:
            if previous.get('receipt'):
                push.cancel(previous['receipt'])
            state.state(arm_generation=generation, stopped='no', receipt='', session='')
        LOG.info('Push watcher started; stopped=%s', state.state().get('stopped'))
        delay = 60
        while True:
            started = time.monotonic()
            delay = 60 if poll(state, push) else min(delay * 2, 600)
            time.sleep(max(0, delay - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
