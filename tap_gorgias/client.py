import json
import requests
import requests.exceptions
import os
import singer
import time
from urllib.parse import ParseResult, parse_qs, urlencode, urlparse, unquote
from typing import Any, Dict

LOGGER = singer.get_logger()

timeout = os.getenv('DEFAULT_HTTP_TIMEOUT')
try:
    DEFAULT_TIMEOUT = int(timeout) if timeout else None
except (TypeError, ValueError):
    DEFAULT_TIMEOUT = None

def add_url_params(url: str, query_params: Dict[str, Any]) -> str:
    # Handle urls that already have query params and merge them together with other query_params
    parsed_url = urlparse(unquote(url))
    existing_get_args = parse_qs(parsed_url.query)
    existing_get_args.update(query_params)
    merged_url_encoded_get_args = urlencode(existing_get_args, doseq=True)
    # Unquote the url to preserve previous behaviour
    return unquote(ParseResult(
        parsed_url.scheme, parsed_url.netloc, parsed_url.path,
        parsed_url.params, merged_url_encoded_get_args, parsed_url.fragment
    ).geturl())

class GorgiasAPI:
    URL_TEMPLATE = 'https://{}.gorgias.com'
    MAX_RETRIES = 10

    def __init__(self, config):
        self.username = config['username']
        self.password = config['password']
        self.subdomain = config['subdomain']
        self.base_url = self.URL_TEMPLATE.format(self.subdomain)

    def get(self, url, make_log_on_request: bool=True):
        if not url:
            LOGGER.info(f'gorgias get request attempted, but no url passed through')
            return {}

        if not url.startswith('https://'):
            url = f'{self.base_url}{url}'

        for num_retries in range(self.MAX_RETRIES):
            if make_log_on_request:
                LOGGER.info(f'gorgias get request {url}, timeout={DEFAULT_TIMEOUT}')
            resp = requests.get(
                url,
                auth=(self.username, self.password),
                timeout=DEFAULT_TIMEOUT
            )
            try:
                # https://developers.gorgias.com/reference/limitations
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                if resp.status_code == 429 and num_retries < self.MAX_RETRIES:
                    retry_after = resp.headers['Retry-after']
                    LOGGER.info('api query gorgias rate limit', extra={
                        'retry_after': retry_after,
                        'subdomain': self.subdomain
                    })
                    time.sleep(int(retry_after))
                elif resp.status_code >= 500 and num_retries < self.MAX_RETRIES:
                    LOGGER.info('api query gorgias 5xx error', extra={
                        'subdomain': self.subdomain
                    })
                    time.sleep(10)
                else:
                    raise Exception(f'gorgias query error: {resp.status_code}', resp.text)

            if resp and resp.status_code == 200:
                break

        return resp.json()

    def post(self, url, params):
        if not url:
            LOGGER.info(f'gorgias post request attempted, but no url passed through')
            return {}

        if not url.startswith('https://'):
            url = f'{self.base_url}/{url}'

        resp = requests.post(
            url,
            json=params,
            auth=(self.username, self.password),
            timeout=DEFAULT_TIMEOUT
        )

        return resp.json()
