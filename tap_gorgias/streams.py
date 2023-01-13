import datetime
import singer

from typing import Tuple

from singer.utils import strptime_to_utc, strftime as singer_strftime

from tap_gorgias.client import GorgiasAPI

LOGGER = singer.get_logger()


class Stream():
    name = None
    replication_method = None
    key_properties = None
    stream = None
    view_id_key = None
    datetime_fields = None
    url = None
    results_key = None

    def __init__(self, client: GorgiasAPI, start_date=None):
        self.client: GorgiasAPI = client
        if start_date:
            self.start_date = start_date
        else:
            # Don't need to set the start date to before they're founded
            self.start_date = datetime.datetime(2015, 1, 1).strftime('%Y-%m-%d')
        self.start_date = self.reformat_date_datetimes(self.start_date)

    def is_selected(self):
        return self.stream is not None

    def update_bookmark(self, state, value):
        if not value:
            return
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        if value > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)
        else:
            LOGGER.info(f'bookmark not updating for {self.name}: current_bookmark={current_bookmark}, value={value}')

    def reformat_date_datetimes(self, value: str) -> str:
        if value:
            value = strptime_to_utc(value)
            # reformat to use RFC3339 format
            value = singer_strftime(value)
        return value

    def transform_value(self, key: str, value: str) -> str:
        if key in self.datetime_fields and value:
            value = self.reformat_date_datetimes(value)
        return value

    def get_sync_thru_dates(self, state: dict) -> Tuple[str, str]:
        """
        Helper method that gets the bookmark and
        Returns:
            sync_thru (str): the bookmark date or the start date in RFC3339 format
            max_synced_thru (str): the date at which syncing should start from in RFC3339 format
        """
        try:
            sync_thru: str = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru: str = self.start_date

        # Transform the times with the appropriate format so that our comparisons
        # of these values are correct
        sync_thru: str = self.reformat_date_datetimes(sync_thru)
        max_synced_thru: str = max(sync_thru, self.start_date)
        return sync_thru, max_synced_thru


class Tickets(Stream):
    name = 'tickets'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'updated_datetime'
    view_id_key = 'tickets_view_id'
    datetime_fields = set([
        'updated_datime', 'created_datetime', 'opened_datetime',
        'last_received_message_datetime', 'last_message_datetime', 'closed_datetime',
        'snooze_datetime'
    ])
    url = '/api/views/{}/items'
    results_key = 'data'

    def paging_get(self, url):

        while True:  # break occurs in the sync function
            data = self.client.get(url)
            for record in data[self.results_key]:
                yield record
            url = data['meta']['next_items']
            LOGGER.info(f'next url for GET: {url}')
            if not url:
                break

    def sync(self, state, config):
        view_id = config.get(self.view_id_key)
        if not view_id:
            LOGGER.exception(f'No view ID provided for {self.name}')
            return
        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        messages_stream = Messages(self.client)

        url = self.url.format(view_id)

        LOGGER.info(f'starting url for Paging GET: {url}')

        # tickets are retrieved in descending order based on updated_datetime
        # with no date filtering
        for row in self.paging_get(url):
            ticket = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru = ticket[self.replication_key]
            if curr_synced_thru > sync_thru:
                yield(self.stream, ticket)
                max_synced_thru = max(curr_synced_thru, max_synced_thru)
                if messages_stream.is_selected():
                    yield from messages_stream.sync(ticket['id'], sync_thru)
            else:
                break

        self.update_bookmark(state, max_synced_thru)


class Messages(Stream):
    name = 'messages'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'sent_datetime'
    url = '/api/tickets/{}/messages'
    datetime_fields = set([
        'created_datetime', 'sent_datetime', 'failed_datetime',
        'deleted_datetime', 'opened_datetime'
    ])
    results_key = 'data'

    def paging_get(self, url):
        next_page = 1
        total_pages = 1

        while next_page <= total_pages:
            data = self.client.get(url+f'?page={next_page}')

            for record in data.get(self.results_key):
                yield record

            total_pages = data.get('meta').get('nb_pages') or total_pages
            next_page += 1

    def sync(self, ticket_id, sync_thru):
        url = self.url.format(ticket_id)
        for row in self.paging_get(url):
            message = {k: self.transform_value(k, v) for (k, v) in row.items()}
            # going to retrieve all messages for each ticket regardless
            # of when the message was sent to ensure we have a complete
            # list (including first response time) just in case.
            # if this slows things down we can revisit
            yield(self.stream, message)

        # Since messages is a substream of tickets, we don't write any bookmarks

class SatisfactionSurveys(Stream):
    name = 'satisfaction_surveys'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_datetime'
    key_properties = ['id']
    url = '/api/satisfaction-surveys?per_page=30&page={}'
    datetime_fields = set([
        'created_datetime', 'scored_datetime', 'sent_datetime',
        'should_send_datetime'
    ])
    results_key = 'data'

    def paging_get(self, url):
        next_page = 1
        total_pages = 1

        while next_page <= total_pages:
            data = self.client.get(url.format(next_page))

            for record in data.get(self.results_key):
                yield record

            total_pages = data.get('meta').get('nb_pages') or total_pages
            next_page += 1

    def sync(self, state, config):
        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # surveys are retrieved in descending order based on created_datetime
        # with no date filtering
        for row in self.paging_get(self.url):
            survey = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru = survey[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            if curr_synced_thru > sync_thru:
                yield(self.stream, survey)
            else:
                break

        self.update_bookmark(state, max_synced_thru)


class Events(Stream):
    name = 'events'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_datetime'
    key_properties = ['id']
    url = '/api/events?limit=100&order_by=created_datetime:asc'
    datetime_fields = set([
        'created_datetime',
    ])
    results_key = 'data'

    def cursor_get(self, url, bookmark_date):
        """ Paginate through the events list response via the provided cursors. """
        cursors_seen = set()
        # Events can keep coming in while we're making requests so limit the range to utcnow
        utcnow_iso: str = self.reformat_date_datetimes(
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        )
        url = f'{url}&created_datetime[gt]={bookmark_date}&created_datetime[lt]={utcnow_iso}'
        LOGGER.info(f'Fetching {self.name} between {bookmark_date} and {utcnow_iso}')
        def _get_page(cursor=None):
            cursors_seen.add(cursor)
            # Since the URL doesn't change, don't make logs on each request
            if not cursor:
                return self.client.get(url, make_log_on_request=False)
            return self.client.get(f'{url}&cursor={cursor}', make_log_on_request=False)
        
        next_cursor = None
        while next_cursor not in cursors_seen:
            # pass an empty cursor to begin
            data = _get_page(next_cursor)
            records = data.get(self.results_key)
            try:
                # For each page, log the date range of this page
                page_start_date, page_end_date = (
                    records[0][self.replication_key],
                    records[-1][self.replication_key]
                )
                LOGGER.info(f'Fetched {self.name} between {page_start_date} and {page_end_date}')
            except:
                pass
            for record in records:
                yield record
            next_cursor = data['meta'].get('next_cursor')

    def sync(self, state, config):
        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # events are ordered in ascending order since we include an order_by query param
        for row in self.cursor_get(self.url, sync_thru):
            event = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = event[self.replication_key]
            max_synced_thru: str = max(curr_synced_thru, max_synced_thru)
            yield (self.stream, event)
        self.update_bookmark(state, max_synced_thru)


STREAMS = {
    "events": Events,
    "tickets": Tickets,
    "messages": Messages,
    "satisfaction_surveys": SatisfactionSurveys
}
