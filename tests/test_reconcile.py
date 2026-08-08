from idx.jobs.reconcile import tick_size


def test_tick_size_bands_match_idx_fraksi_harga():
    assert tick_size(50) == 1
    assert tick_size(199) == 1
    assert tick_size(200) == 2
    assert tick_size(499) == 2
    assert tick_size(500) == 5
    assert tick_size(1999) == 5
    assert tick_size(2000) == 10
    assert tick_size(4999) == 10
    assert tick_size(5000) == 25
    assert tick_size(50000) == 25
