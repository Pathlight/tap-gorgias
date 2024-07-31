import singer
import singer.metrics as metrics
from singer import metadata


LOGGER = singer.get_logger()


def sync_stream(state, start_date, instance, config):
    stream = instance.stream
    mdata = stream.metadata

    current_bookmark = state.get('bookmarks', {}).get(stream.tap_stream_id, {}).get(instance.replication_key)
    # If we have a bookmark, use it; otherwise use start_date for streams that don't use cursor bookmarks
    if (
        not instance.uses_cursor_bookmark and
        not current_bookmark and
        instance.replication_method == 'INCREMENTAL'
    ):
        singer.write_bookmark(state, stream.tap_stream_id, instance.replication_key, start_date)

    parent_stream = stream
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for (stream, record) in instance.sync(state, config):
            # NB: Only count parent records in the case of sub-streams
            if stream.tap_stream_id == parent_stream.tap_stream_id:
                counter.increment()

            with singer.Transformer() as transformer:
                rec = transformer.transform(record, stream.schema.to_dict(), metadata=metadata.to_map(mdata))
            singer.write_record(stream.tap_stream_id, rec)
            # NB: We will only write state at the end of a stream's sync:
            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
            #  but we don't know if we can guarentee the order of emitted records.

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        return counter.value
