import json
import os
from datetime import datetime
from urllib.request import Request, urlopen

from push_watch import Push
from doomsday_watch import PKT, CINEMA

try:
    request = Request(os.environ['WATCHER_HEALTH_URL'].rstrip('/') + '/health',
                      headers={'Authorization': 'Bearer ' + os.environ['HEALTH_TOKEN']})
    with urlopen(request, timeout=15) as response:
        state = json.load(response)
    checked = datetime.fromisoformat(state['last_success'])
    age = int((datetime.now(PKT) - checked).total_seconds())
    issues = []
    if not 0 <= age <= 180:
        issues.append(f'Last successful check is stale ({age // 60} minutes old)')
    if state['availability'] == 'unknown':
        issues.append('Latest Cinepax check failed')
    if state['delivery_error']:
        issues.append('Last alert API error: ' + state['delivery_error'])
    details = '\n'.join([f'Last successful check: {checked.astimezone(PKT):%d %b %Y %H:%M:%S} PKT',
                         f"Availability: {state['availability']}",
                         f"Ticket alerts: {state['mode']}",
                         f"Last ticket push accepted: {state['last_sent'] or 'none yet'}"])
except Exception as exc:
    issues = ['Could not verify the watcher (' + type(exc).__name__ + ')']
    details = 'Check Railway deployment logs and the health URL/token configuration.'

Push(os.environ['PUSHOVER_APP_TOKEN'], os.environ['PUSHOVER_USER_KEY']).send(
    'Doomsday daily check: ' + ('NEEDS ATTENTION' if issues else 'HEALTHY'),
    '\n'.join([CINEMA, *issues, details]), priority=1 if issues else 0)
