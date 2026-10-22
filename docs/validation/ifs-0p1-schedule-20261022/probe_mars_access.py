"""Read-only credential check and a single, metadata-only MARS list request."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parent
QUERY = ('list,class=od,stream=oper,type=fc,expver=1,date=20261014,'
         'time=12,step=0,levtype=sfc,param=167.128,output=cost,')


def main():
    credentials = Path.home() / '.ecmwfapirc'
    report = {'checked_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'credential_file_exists': credentials.is_file(), 'data_download_requested': False}
    if not credentials.is_file():
        report['status'] = 'no_configured_credentials'
    else:
        config = json.loads(credentials.read_text())
        endpoint = urllib.parse.urlparse(config['url'])
        assert endpoint.hostname == 'api.ecmwf.int', 'Unexpected credential endpoint'
        base = 'https://api.ecmwf.int' + endpoint.path.rstrip('/')

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None

        opener = urllib.request.build_opener(NoRedirect)

        def safe_text(value):
            text = str(value)
            for key in ['key', 'email']:
                if config.get(key):
                    text = text.replace(config[key], '[redacted]')
            return text[:1500]

        def request(url, payload=None):
            assert urllib.parse.urlparse(url).hostname == 'api.ecmwf.int'
            req = urllib.request.Request(
                url, data=json.dumps(payload).encode() if payload is not None else None,
                headers={'Accept': 'application/json', 'From': config['email'],
                         'X-ECMWF-KEY': config['key'], 'Content-Type': 'application/json'},
            )
            try:
                response = opener.open(req, timeout=18)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                body = response.read(128 * 1024).decode()
                try:
                    data = json.loads(body)
                except ValueError:
                    data = {'error': safe_text(body)}
                return response.code, dict(response.headers), data

        try:
            status, _, identity = request(base + '/who-am-i')
            authenticated = status == 200 and bool(identity.get('uid')) and 'error' not in identity
            report['authentication'] = {'http_status': status, 'authenticated': authenticated}
            if not authenticated:
                report['authentication']['error'] = safe_text(identity.get('error', 'Authentication not established'))
                report['status'] = 'authentication_failed'
            else:
                status, _, info = request(base + '/services/mars/info')
                report['service_info'] = {'http_status': status}
                if 'error' in info:
                    report['service_info']['error'] = safe_text(info['error'])
                status, headers, response = request(base + '/services/mars/requests', QUERY)
                report['list_request'] = {'request': QUERY, 'http_status': status,
                                          'states': [response.get('status')]}
                location = urllib.parse.urljoin(base + '/', headers.get('Location', ''))
                if location != base + '/':
                    report['list_request']['location'] = location
                started = time.monotonic()
                while (status in [200, 201, 202] and response.get('status') not in ['complete', 'failed', 'aborted']
                       and 'error' not in response and location != base + '/' and time.monotonic() - started < 36):
                    time.sleep(2)
                    status, _, response = request(location)
                    report['list_request']['states'].append(response.get('status'))
                if 'error' in response:
                    report['list_request']['error'] = safe_text(response['error'])
                report['list_request']['messages'] = [safe_text(x) for x in response.get('messages', [])][-8:]
                if status >= 400 or 'error' in response or response.get('status') in ['failed', 'aborted']:
                    report['status'] = 'mars_list_not_authorized_or_failed'
                elif response.get('status') == 'complete':
                    report['status'] = 'mars_list_completed'
                    result = response.get('result') or {}
                    report['list_request']['result_size'] = result.get('size')
                    href = result.get('href')
                    if href and int(result.get('size', 0)) <= 65536:
                        url = urllib.parse.urljoin(base + '/', href)
                        parsed = urllib.parse.urlparse(url)
                        assert parsed.scheme == 'https' and parsed.hostname.endswith('.ecmwf.int')
                        # Result URLs are fetched without the account headers.
                        with urllib.request.urlopen(url, timeout=18) as result_response:
                            text = result_response.read(65536).decode()
                        report['list_request']['result_text'] = safe_text(text)
                else:
                    report['status'] = 'mars_list_pending'
        except Exception as error:
            report['status'] = 'access_check_failed'
            report['error'] = safe_text(f'{type(error).__name__}: {error}')
    (ROOT / 'mars-access.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
