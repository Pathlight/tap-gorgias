import datetime
import singer

from typing import Any, Dict, Tuple

from singer.utils import strptime_to_utc, strftime as singer_strftime

from tap_gorgias.client import GorgiasAPI, add_url_params

LOGGER = singer.get_logger()


class CursorStream:
    name = None
    replication_method = None
    replication_key = None
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
        self.utcnow_iso: str = self.reformat_date_datetimes(
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        )

    @property
    def uses_cursor_bookmark(self):
        # TODO: update all the other streams to bookmark the cursor
        return self.replication_key == "cursor"

    def is_selected(self):
        return self.stream is not None

    def update_bookmark(self, state, value):
        if not value:
            return
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        if self.uses_cursor_bookmark or value > current_bookmark:
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

    def cursor_get(self, url: str, query_params: Dict[str, Any]):
        """ Paginate through the streams list response via the provided cursors. """
        updated_url = add_url_params(url, query_params)
        cursors_seen = set()
        def _get_page(cursor=None):
            cursors_seen.add(cursor)
            # Since the URL doesn't change, don't make logs on each request
            if cursor:
                new_url = add_url_params(url, {**query_params, "cursor": cursor})
            else:
                new_url = updated_url
            log_on_request = cursor is None
            if self.uses_cursor_bookmark:
                log_on_request = True
            return self.client.get(new_url, make_log_on_request=log_on_request)

        next_cursor = query_params.get("cursor")
        if next_cursor:
            # When we have a bookmark and we reach the end, the next_cursor is None. However, this is the first time we're
            # seeing it, so it'll loop back all the way to the beginning. Prevent this by adding None i.e we've already already
            # seen the starting point
            cursors_seen.add(None)

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
                if self.uses_cursor_bookmark:
                    yield (record, next_cursor)
                else:
                    yield record
            next_cursor = data['meta'].get('next_cursor')


class Tickets(CursorStream):
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
    results_key = 'data'
    url = '/api/tickets'

    # There are two APIs that return the same data:
    # 1. the views API, https://developers.gorgias.com/reference/get_api-views
    # 2. the tickets API, https://developers.gorgias.com/reference/get_api-tickets

    def sync(self, state, config):
        # https://developers.gorgias.com/reference/get_api-tickets
        view_id = config.get(self.view_id_key)
        if not view_id:
            # This API doesn't require the view ID, but to preserve previous behaviour,
            # exit when this not provided in the config
            LOGGER.exception(f'No view ID provided for {self.name}')
            return

        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # Since there are no datetime filters available for this endpoint,
        # sort in descending order and stop when we've reached the bookmark
        query_params = {
            'view_id': view_id,
            'limit': 100,
            'order_by': 'created_datetime:desc',
        }
        LOGGER.info(f'Starting fetch for {self.name} stopping at {sync_thru}')
        for row in self.cursor_get(self.url, query_params):
            ticket = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = ticket[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            if curr_synced_thru > sync_thru:
                yield(self.stream, ticket)
            else:
                break

        self.update_bookmark(state, max_synced_thru)


class Messages(CursorStream):
    name = 'messages'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'created_datetime'
    url = '/api/messages'
    datetime_fields = set([
        'created_datetime', 'sent_datetime', 'failed_datetime',
        'deleted_datetime', 'opened_datetime'
    ])
    results_key = 'data'
    def sync(self, state, config):
        # https://developers.gorgias.com/reference/get_api-messages

        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # Since there are no datetime filters available for this endpoint,
        # sort in descending order and stop when we've reached the bookmark
        query_params = {
            'limit': 100,
            'order_by': 'created_datetime:desc',
        }
        LOGGER.info(f'Starting fetch for {self.name} stopping at {sync_thru}')
        for row in self.cursor_get(self.url, query_params):
            message = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = message[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            if curr_synced_thru > sync_thru:
                yield(self.stream, message)
            else:
                break

        self.update_bookmark(state, max_synced_thru)


class SatisfactionSurveys(CursorStream):
    name = 'satisfaction_surveys'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_datetime'
    key_properties = ['id']
    url = '/api/satisfaction-surveys'
    datetime_fields = set([
        'created_datetime', 'scored_datetime', 'sent_datetime',
        'should_send_datetime'
    ])
    results_key = 'data'

    def sync(self, state, config):
        # https://developers.gorgias.com/reference/get_api-satisfaction-surveys

        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # Since there are no datetime filters available for this endpoint,
        # sort in descending order and stop when we've reached the bookmark
        query_params = {
            'limit': 100,
            'order_by': 'created_datetime:desc',
        }
        LOGGER.info(f'Starting fetch for {self.name} stopping at {sync_thru}')
        for row in self.cursor_get(self.url, query_params):
            survey = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = survey[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            if curr_synced_thru > sync_thru:
                yield(self.stream, survey)
            else:
                break

        self.update_bookmark(state, max_synced_thru)


class Events(CursorStream):
    name = 'events'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_datetime'
    key_properties = ['id']
    url = '/api/events'
    datetime_fields = set([
        'created_datetime',
    ])
    results_key = 'data'

    def sync(self, state, config):
        # https://developers.gorgias.com/reference/get_api-events

        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # events are ordered in ascending order since we have both order_by and datetime
        # query params, explicitly limit the time to utcnow
        query_params = {
            'limit': 100,
            'order_by': 'created_datetime:asc',
            'created_datetime[gt]': sync_thru,
            'created_datetime[lt]': self.utcnow_iso,
        }
        LOGGER.info(f'Starting fetch for {self.name} between {sync_thru} and {self.utcnow_iso}')
        for row in self.cursor_get(self.url, query_params):
            event = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = event[self.replication_key]
            max_synced_thru: str = max(curr_synced_thru, max_synced_thru)
            yield (self.stream, event)
        self.update_bookmark(state, max_synced_thru)

class VoiceCallEvents(CursorStream):
    name = 'voice_call_events'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'cursor'
    datetime_fields = set(['created_datetime'])
    results_key = 'data'
    url = '/api/phone/voice-call-events'

    def sync(self, state, config):
        # Check https://developers.gorgias.com/reference/list-voice-call-events for updates
        # Default ordering is ascending with option of specifying order, but we can use the cursors as bookmarks
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        query_params = {'limit': 100}
        if current_bookmark:
            query_params['cursor'] = current_bookmark

        LOGGER.info(f'Starting fetch for {self.name} at cursor {current_bookmark}')
        cursor = None
        for row, cursor in self.cursor_get(self.url, query_params):
            event = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield (self.stream, event)

        if cursor:
            self.update_bookmark(state, cursor)


class VoiceCallRecordings(CursorStream):
    name = 'voice_call_recordings'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'created_datetime'
    datetime_fields = set(['created_datetime'])
    results_key = 'data'
    url = '/api/phone/voice-call-recordings'

    def sync(self, state, config):
        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # Check https://developers.gorgias.com/reference/list-voice-call-recordings for updates
        query_params = {
            'limit': 100,
        }
        LOGGER.info(f'Starting fetch for {self.name} between {sync_thru} and {self.utcnow_iso}')
        for row in self.cursor_get(self.url, query_params):
            recording = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = recording[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            # Stop fetching if the current record is older than or equal to the bookmark
            if curr_synced_thru <= sync_thru:
                LOGGER.info(f'Stopping fetch at {curr_synced_thru} as it is older than or equal to bookmark {sync_thru}')
                break
            yield (self.stream, recording)
        self.update_bookmark(state, max_synced_thru)


class VoiceCalls(CursorStream):
    name = 'voice_calls'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'created_datetime'
    datetime_fields = set(['created_datetime', 'started_datetime', 'updated_datetime'])
    results_key = 'data'
    url = '/api/phone/voice-calls'

    def sync(self, state, config):
        sync_thru, max_synced_thru = self.get_sync_thru_dates(state)
        # Check https://developers.gorgias.com/reference/list-voice-calls for updates
        query_params = {
            'limit': 100,
        }
        LOGGER.info(f'Starting fetch for {self.name} between {sync_thru} and {self.utcnow_iso}')
        for row in self.cursor_get(self.url, query_params):
            call = {k: self.transform_value(k, v) for (k, v) in row.items()}
            curr_synced_thru: str = call[self.replication_key]
            max_synced_thru = max(curr_synced_thru, max_synced_thru)
            # Stop fetching if the current record is older than or equal to the bookmark
            if curr_synced_thru <= sync_thru:
                LOGGER.info(f'Stopping fetch at {curr_synced_thru} as it is older than or equal to bookmark {sync_thru}')
                break
            yield (self.stream, call)
        self.update_bookmark(state, max_synced_thru)

STREAMS = {
    "events": Events,
    "tickets": Tickets,
    "messages": Messages,
    "satisfaction_surveys": SatisfactionSurveys,
    "voice_call_events": VoiceCallEvents,
    "voice_call_recordings": VoiceCallRecordings,
    "voice_calls": VoiceCalls
}
