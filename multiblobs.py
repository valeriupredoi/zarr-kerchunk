import os
import fsspec
import h5py
import itertools
import numpy as np
import xarray as xr
import time
import yaml
import struct
import zarr

import kerchunk
import kerchunk.zarr as zr
import dask.array as da

from collections.abc import MutableMapping


print(f"Kerchunk library at: {kerchunk.zarr.__file__}")
print(f"Zarr library at: {zarr.__file__}")
print(f"h5py library at: {h5py.__file__}")

def _kerzarr_multiblob():
    """Use kerchunks for multinlobbed Zarrs."""
    store = "/home/valeriu/zarr-kerchunk/example.zarr"
    so = dict(
        anon=True, default_fill_cache=False, default_cache_type='first'
    )

    # understand the Zarr file structure
    # single_zarr is a nice wrapper to fsspec zarr loader
    zr_file = zr.single_zarr(store, storage_options=so)  # tis a dictionary!
    print(zr_file)
    print(f"Zarr file keys: {zr_file.keys()}")
    # load with zarr
    ds = zarr.open("./example.zarr")
    print(f"Data file loaded by Zarr\n: {ds}")
    print(f"Info of Data file loaded by Zarr\n: {ds.info}")
    print(f"Data array loaded by Zarr\n: {ds[:]}")
    print(f"Data chunks: {ds.chunks}")

    # Zarr chunking information
    # from zarr.convenience._copy(); convenience module l.897
    # https://zarr.readthedocs.io/en/stable/api/convenience.html#zarr.convenience.copy
    shape = ds.shape
    chunks = ds.chunks
    chunk_offsets = [range(0, s, c) for s, c in zip(shape, chunks)]
    print("Chunk offsets", chunk_offsets)
    for offset in itertools.product(*chunk_offsets):
        sel = tuple(slice(o, min(s, o + c))
                    for o, s, c in zip(offset, shape, chunks))
        print(f"Slice {sel}")

    return zr_file


# copy of h5py test in 
# /lib/python3.10/site-packages/h5py/tests/test_dataset.py
# with iden input array as Zarr file above
def test_get_chunk_details():
    from io import BytesIO
    buf = BytesIO()
    with h5py.File(buf, 'w') as fout:
        fout.create_dataset('test', shape=(10, 10), chunks=(5, 5), dtype='i4')
        fout['test'][:] = 1

    buf.seek(0)
    with h5py.File(buf, 'r') as fin:
        ds = fin['test'].id

        assert ds.get_num_chunks() == 100
        for j in range(100):
            offset = tuple(np.array(np.unravel_index(j, (10, 10))) * 10)

            si = ds.get_chunk_info(j)
            assert si.chunk_offset == offset
            assert si.filter_mask == 0
            assert si.byte_offset is not None
            assert si.size > 0

        si = ds.get_chunk_info_by_coord((0, 0))
        assert si.chunk_offset == (0, 0)
        assert si.filter_mask == 0
        assert si.byte_offset is not None
        assert si.size > 0


def main():
    """Run the damn thing."""
    zrf = _kerzarr_multiblob()
    test_get_chunk_details()


if __name__ == '__main__':
    main()
