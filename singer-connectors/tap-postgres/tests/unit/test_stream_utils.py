"""Unit tests for :func:`tap_postgres.stream_utils.refresh_streams_schema`.

A LOG_BASED run refreshes a stream's schema mid-run when a WAL row carries
a column the catalog does not know. Column selection comes from the
catalog (e.g. Meltano ``!table.column`` rules), not from discovery, so the
refresh must not reset it: a refused column has to stay refused, and a
column the catalog has never seen is held back until a run with a fresh
catalog decides on it.
"""

import contextlib
import logging
from unittest.mock import patch

from singer import metadata

from tap_postgres import stream_utils


def _discovered(columns):
    """A stream as discovery returns it: no selection, no replication method."""
    md = metadata.write({}, (), 'table-key-properties', ['id'])
    for col in columns:
        md = metadata.write(md, ('properties', col), 'inclusion', 'automatic' if col == 'id' else 'available')
        md = metadata.write(md, ('properties', col), 'selected-by-default', True)
    return {
        'table_name': 'conversion_events',
        'stream': 'conversion_events',
        'tap_stream_id': 'public-conversion_events',
        'schema': {'type': 'object', 'properties': {c: {'type': ['null', 'string']} for c in columns}},
        'metadata': metadata.to_list(md),
    }


def _catalog_stream():
    """The stream as the catalog hands it to the tap, with selection applied."""
    stream = _discovered(['id', 'event_name', 'email_sha256'])
    md = metadata.to_map(stream['metadata'])
    md[()].update({'selected': True, 'replication-method': 'LOG_BASED'})
    md[('properties', 'id')]['selected'] = True
    md[('properties', 'event_name')]['selected'] = True
    md[('properties', 'email_sha256')]['selected'] = False
    stream['metadata'] = metadata.to_list(md)
    return stream


def test_refresh_keeps_column_selection_and_holds_back_new_columns(caplog):
    stream = _catalog_stream()
    rediscovered = _discovered(['id', 'event_name', 'email_sha256', 'ip_address'])

    with patch.object(stream_utils, 'open_connection', lambda _cfg: contextlib.nullcontext(None)), \
            patch.object(stream_utils, 'discover_db', return_value=[rediscovered]), \
            caplog.at_level(logging.INFO):
        stream_utils.refresh_streams_schema({}, [stream])

    md = metadata.to_map(stream['metadata'])

    assert md[('properties', 'email_sha256')]['selected'] is False
    assert md[('properties', 'event_name')]['selected'] is True
    assert md[('properties', 'ip_address')]['selected'] is False

    assert md[()]['selected'] is True
    assert md[()]['replication-method'] == 'LOG_BASED'
    assert md[()]['table-key-properties'] == ['id']

    assert 'ip_address' in stream['schema']['properties']
    assert any('conversion_events' in r.getMessage() and 'ip_address' in r.getMessage()
               for r in caplog.records if r.levelno == logging.INFO)
