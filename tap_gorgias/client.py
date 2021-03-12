import json
import requests
import requests.exceptions
import singer
import time
import urllib

LOGGER = singer.get_logger()


class GorgiasAPI:
    URL_TEMPLATE = 'https://{}.gorgias.com'
    MAX_RETRIES = 10

    def __init__(self, config):
        self.username = config['username']
        self.password = config['password']
        self.subdomain = config['subdomain']
        self.base_url = self.URL_TEMPLATE.format(self.subdomain)

    def get(self, url):
        if not url.startswith('https://'):
            url = f'{self.base_url}{url}'

        for num_retries in range(self.MAX_RETRIES):
            LOGGER.info(f'gorgias get request {url}')
            resp = requests.get(
                url,
                auth=(self.username, self.password)
            )
            try:
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
                    raise Exception(f'gorgias query error: {resp.status_code}')

            if resp and resp.status_code == 200:
                break

        return resp.json()

    def post(self, url, params):

        if not url.startswith('https://'):
            url = f'{self.base_url}/{url}'

        resp = requests.post(
            url,
            json=params,
            auth=(self.username, self.password)
        )

        return resp.json()
