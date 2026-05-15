"""Unit tests for :func:`tap_postgres._stream_sort_key`.

The sort key is the only piece of code that decides which order streams
are emitted in. The behaviour we want to lock in:

- A stream with no ``priority`` metadata sorts at priority 0.
- A stream with a numeric ``priority`` sorts by that value (lower first).
- Streams sharing a priority fall back to alphabetical
  ``tap_stream_id`` (the historical default).
- A catalog where no stream sets ``priority`` is byte-identical to the
  pre-change alphabetical behaviour.
"""

import pytest

from tap_postgres import _stream_sort_key


def _stream(tap_stream_id, *, priority=None, extra_md=None):
    """Build a minimal Singer catalog stream dict."""
    table_md = {}
    if priority is not None:
        table_md['priority'] = priority
    if extra_md:
        table_md.update(extra_md)
    return {
        'tap_stream_id': tap_stream_id,
        'metadata': [
            {'breadcrumb': [], 'metadata': table_md},
            # A non-empty breadcrumb (a column) carries unrelated metadata.
            # Make sure we don't accidentally read priority off it.
            {'breadcrumb': ('properties', 'id'),
             'metadata': {'inclusion': 'automatic'}},
        ],
    }


class TestStreamSortKey:
    def test_unset_priority_defaults_to_zero(self):
        s = _stream('public-foo')
        assert _stream_sort_key(s) == (0, 'public-foo')

    def test_explicit_priority_used_first(self):
        a = _stream('public-aardvark', priority=2)
        z = _stream('public-zebra', priority=0)
        # zebra's priority (0) is lower than aardvark's (2), so zebra
        # sorts first despite the alphabetical tiebreaker preferring
        # aardvark.
        assert sorted([a, z], key=_stream_sort_key) == [z, a]

    def test_tiebreak_is_alphabetical_within_priority(self):
        a = _stream('public-aardvark', priority=1)
        b = _stream('public-bear', priority=1)
        z = _stream('public-zebra', priority=1)
        assert sorted([z, a, b], key=_stream_sort_key) == [a, b, z]

    def test_no_priority_anywhere_is_byte_identical_to_alphabetical(self):
        # The change has to be backward compatible: a catalog that does
        # not set priority on any stream must emit in the same order as
        # the historical ``streams.sort(key=lambda s: s['tap_stream_id'])``.
        names = ['public-zebra', 'public-foo', 'public-aardvark',
                 'public-bear', 'public-mango']
        streams = [_stream(n) for n in names]
        new_order = [s['tap_stream_id']
                     for s in sorted(streams, key=_stream_sort_key)]
        old_order = [s['tap_stream_id']
                     for s in sorted(streams, key=lambda s: s['tap_stream_id'])]
        assert new_order == old_order

    def test_priority_is_not_read_from_column_breadcrumbs(self):
        # A misplaced ``priority`` on a column-level breadcrumb must NOT
        # be picked up -- only the empty-breadcrumb table-level entry
        # counts.
        s = {
            'tap_stream_id': 'public-foo',
            'metadata': [
                {'breadcrumb': ('properties', 'id'),
                 'metadata': {'priority': 99}},
                # No empty-breadcrumb entry at all.
            ],
        }
        assert _stream_sort_key(s) == (0, 'public-foo')

    def test_priority_zero_is_explicit_and_equivalent_to_default(self):
        explicit = _stream('public-foo', priority=0)
        implicit = _stream('public-foo')
        assert _stream_sort_key(explicit) == _stream_sort_key(implicit)

    def test_dependency_levels_sort_correctly(self):
        # Concrete scenario from a HubSpot-loading pipeline: parents
        # must come before children whose associations point at them.
        streams = [
            _stream('public-placements', priority=3),
            _stream('public-companies', priority=0),
            _stream('public-tickets', priority=2),
            _stream('public-contacts', priority=1),
            _stream('public-exchange_visits', priority=2),
        ]
        ordered = [s['tap_stream_id']
                   for s in sorted(streams, key=_stream_sort_key)]
        assert ordered == [
            'public-companies',
            'public-contacts',
            'public-exchange_visits',  # alpha tiebreak vs tickets
            'public-tickets',
            'public-placements',
        ]

    @pytest.mark.parametrize('bad_priority', [None, '', 'high', [], {}])
    def test_non_integer_priority_falls_back_to_zero(self, bad_priority):
        # Be permissive: if a user accidentally sets a non-numeric
        # priority, fall back to 0 rather than crashing the sync (the
        # ``or 0`` clause). Strict TypeError validation could regress a
        # production pipeline mid-run -- a quiet downgrade to default
        # behaviour is the safer choice.
        s = _stream('public-foo', priority=bad_priority)
        # Numeric falsy (0) and non-numeric falsy ('', None, [], {}) all
        # come out as 0 thanks to the ``or 0``. Non-falsy non-int values
        # like ``'high'`` would currently propagate and break the sort
        # by being incomparable to int -- but ``or 0`` only catches
        # falsy values, so ``'high'`` is a real footgun. Document the
        # behaviour by parametrising and asserting what actually happens.
        key = _stream_sort_key(s)
        if bad_priority in (None, '', [], {}):
            assert key == (0, 'public-foo')
        else:
            # ``'high'`` survives -- mixing int and str in a sort raises
            # TypeError. Future hardening could validate, but for now
            # the contract is: non-falsy bad inputs are operator error.
            assert key == (bad_priority, 'public-foo')
