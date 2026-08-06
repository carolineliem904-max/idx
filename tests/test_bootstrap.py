import math

from idx.jobs.bootstrap import _chunk, _none_if_nan


def test_chunk_splits_into_batches_of_requested_size():
    items = list(range(120))
    batches = _chunk(items, 50)
    assert [len(b) for b in batches] == [50, 50, 20]
    assert sum(batches, []) == items


def test_chunk_empty_input():
    assert _chunk([], 50) == []


def test_none_if_nan_passes_through_real_values():
    assert _none_if_nan(4300.0) == 4300.0
    assert _none_if_nan(0) == 0


def test_none_if_nan_converts_nan_and_none():
    assert _none_if_nan(math.nan) is None
    assert _none_if_nan(None) is None
