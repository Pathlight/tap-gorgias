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
            self.start_date = datetime.datetime.min.strftime("%Y-%m-%d")

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


# 2021-11-05 test 100 ticket limit, instead of 30 default
TICKET_LIMIT = 100

class Tickets(Stream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    key_properties = ["id"]
    replication_key = "updated_datetime"
    view_id_key = "tickets_view_id"
    datetime_fields = set(
        [
            "updated_datime",
            "created_datetime",
            "opened_datetime",
            "last_received_message_datetime",
            "last_message_datetime",
            "closed_datetime",
            "snooze_datetime",
        ]
    )
    VIEW_URL = "/api/views/{}/items?limit={}"
    RESULTS_KEY = "data"
    history = set()

    def paging_get(self, url):
        while True:  # break occurs in the sync function
            data = self.client.get(url)
            LOGGER.info(f"found {len(data[self.RESULTS_KEY])} tickets")
            for record in data[self.RESULTS_KEY]:
                yield record
            url = data["meta"]["next_items"] + f"&limit={TICKET_LIMIT}"

    def sync(self, state, config):
        view_id = config.get(self.view_id_key)
        if not view_id:
            LOGGER.exception(f"No view ID provided for {self.name}")
            return
        url = self.VIEW_URL.format(view_id, TICKET_LIMIT)

        try:
            sync_thru = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru = self.start_date

        max_synced_thru = max(sync_thru, self.start_date)
        messages_stream = Messages(self.client)

        # Load all tickets, in descending order,
        # based on updated_datetime, with no date filtering
        for row in self.paging_get(url):
            # pull out ticket ids
            ticket = {k: self.transform_value(k, v) for (k, v) in row.items()}
            if ticket.get('id') not in self.history:
                self.history.add(ticket.get('id'))

            curr_synced_thru = ticket[self.replication_key]
            # Debug info
            len_t = len(self.history)
            len_m = len(messages_stream.history)
            LOGGER.info(f'gorgias curr_synced_thru={curr_synced_thru} tickets_seen={len_t} messages_seen={len_m}')
            if curr_synced_thru > sync_thru:
                yield (self.stream, ticket)
                max_synced_thru = max(curr_synced_thru, max_synced_thru)
                if messages_stream.is_selected():
                    yield from messages_stream.sync(ticket["id"], sync_thru)
            else:
                LOGGER.info(f'time to break tickets?')
                break

        self.update_bookmark(state, max_synced_thru)


class Messages(Stream):
    name = "messages"
    replication_method = "INCREMENTAL"
    key_properties = ["id"]
    replication_key = "sent_datetime"
    datetime_fields = set(
        [
            "created_datetime",
            "sent_datetime",
            "failed_datetime",
            "deleted_datetime",
            "opened_datetime",
        ]
    )
    VIEW_URL = "/api/tickets/{}/messages"
    RESULTS_KEY = "data"
    history = set()

    def paging_get(self, url):
        """Handles possible multiple pages of messages, for a ticket"""
        next_page = 1
        total_pages = 1
        while next_page <= total_pages:
            data = self.client.get(url + f"?page={next_page}")

            m = data.get(self.RESULTS_KEY)
            if m:
                m = len(m)
            for record in data.get(self.RESULTS_KEY):
                yield record

            total_pages = data.get("meta").get("nb_pages") or total_pages
            next_page += 1

    def sync(self, ticket_id, sync_thru):
        url = self.VIEW_URL.format(ticket_id)
        for row in self.paging_get(url):
            message = {k: self.transform_value(k, v) for (k, v) in row.items()}
            if message.get('id') not in self.history:
                self.history.add(message.get('id'))
            yield (self.stream, message)


class SatisfactionSurveys(Stream):
    name = "satisfaction_surveys"
    replication_method = "INCREMENTAL"
    replication_key = "created_datetime"
    key_properties = ["id"]
    url = "/api/satisfaction-surveys?per_page=30&page={}"
    datetime_fields = set(
        ["created_datetime", "scored_datetime", "sent_datetime", "should_send_datetime"]
    )
    results_key = "data"

    def paging_get(self, url):
        next_page = 1
        total_pages = 1

        while next_page <= total_pages:
            data = self.client.get(url.format(next_page))

            for record in data.get(self.results_key):
                yield record

            total_pages = data.get("meta").get("nb_pages") or total_pages
            next_page += 1

    def sync(self, state, config):
        try:
            sync_thru = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru = self.start_date
        sync_thru = self.transform_value("date", sync_thru)

        max_synced_thru = max(sync_thru, self.start_date)

        # surveys are retrieved in descending order based on created_datetime
        # with no date filtering
        for row in self.paging_get(self.url):
            survey = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru = survey[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            if curr_synced_thru > sync_thru:
                yield (self.stream, survey)
            else:
                break

        self.update_bookmark(state, max_synced_thru)


STREAMS = {
    "tickets": Tickets,
    "messages": Messages,
    "satisfaction_surveys": SatisfactionSurveys,
}
