import datetime
import pytz
import singer

from singer.utils import strptime_to_utc, strftime as singer_strftime


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

    def __init__(self, client=None, start_date=None):
        self.client = client
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.min.strftime('%Y-%m-%d')

    def is_selected(self):
        return self.stream is not None

    def update_bookmark(self, state, value):
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        if value and value > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)

    def transform_value(self, key, value):
        if key in self.datetime_fields and value:
            value = strptime_to_utc(value)
            # reformat to use RFC3339 format
            value = singer_strftime(value)

        return value


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

        params = {}
        while True:  # break occurs in the sync function
            data = self.client.get(url, params)
            for record in data[self.results_key]:
                yield record
            url = data['meta']['next_items']

    def sync(self, state, config):
        view_id = config.get(self.view_id_key)
        if not view_id:
            LOGGER.info(f'No view ID provided for {self.name}')
            return

        try:
            sync_thru = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru = self.start_date
        sync_thru = self.transform_value('date', sync_thru)

        curr_synced_thru = max(sync_thru, self.start_date)

        messages_stream = Messages(self.client)

        url = self.url.format(view_id)

        # tickets are retrieved in descending order based on updated_datetime
        # with no date filtering
        for row in self.paging_get(url):
            ticket = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru = ticket['updated_datetime']
            if curr_synced_thru > sync_thru:
                yield(self.stream, ticket)
                if messages_stream.is_selected():
                    print('\n\n\n\n syncing messages for', ticket['id'], '\n\n\n')
                    yield from messages_stream.sync(ticket['id'], sync_thru)
            else:
                break

        self.update_bookmark(state, curr_synced_thru)


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
            params = {'page': next_page}
            data = self.client.get(url, params=params)

            for record in data.get(self.results_key):
                yield record

            total_pages = data.get('meta').get('nb_pages') or total_pages
            next_page += 1

    def sync(self, ticket_id, sync_thru):
        url = self.url.format(ticket_id)
        for row in self.paging_get(url):
            message = {k: self.transform_value(k, v) for (k, v) in row.items()}
            if message['created_datetime'] < sync_thru:
                yield(self.stream, message)
            else:
                break


STREAMS = {
    "tickets": Tickets,
    "messages": Messages
}
