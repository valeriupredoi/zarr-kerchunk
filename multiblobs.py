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

print("\nGet some libs paths")
print("=====================")
print(f"Kerchunk library at: {kerchunk.zarr.__file__}")
print(f"Zarr library at: {zarr.__file__}")
print(f"h5py library at: {h5py.__file__}")

def build_zarr_array():
    """Create the zarr array."""
    # multiblob array: each chunk stored separately
    # see https://zarr.readthedocs.io/en/stable/api/storage.html
    store = zarr.DirectoryStore('data/array.zarr')
    z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
    z[...] = 42
    zarr.save("example.zarr", z)


def _kerzarr_multiblob():
    """Use kerchunks for multinlobbed Zarrs."""
    print("\nZarr stuff")
    print("===============")
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
    # print(f"Data array loaded by Zarr\n: {ds[:]}")
    print(f"Data chunks: {ds.chunks}")

    # Zarr chunking information
    # from zarr.convenience._copy(); convenience module l.897
    # https://zarr.readthedocs.io/en/stable/api/convenience.html#zarr.convenience.copy
    shape = ds.shape
    chunks = ds.chunks
    chunk_offsets = [range(0, s, c) for s, c in zip(shape, chunks)]
    print("Chunk offsets", chunk_offsets)
    print("Zarr number of chunks", len(list(itertools.product(*chunk_offsets))))
    for offset in itertools.product(*chunk_offsets):
        print("Slice offset", offset)
        sel = tuple(slice(o, min(s, o + c))
                    for o, s, c in zip(offset, shape, chunks))
        print(f"Slice {sel}")

    return zr_file


# copy of h5py test in 
# /lib/python3.10/site-packages/h5py/tests/test_dataset.py
# with iden input array as Zarr file above
def test_get_chunk_details():
    print("\nHDF5 stuff")
    print("===============")
    from io import BytesIO
    buf = BytesIO()
    with h5py.File(buf, 'w') as fout:
        fout.create_dataset('test', shape=(10, 10), chunks=(5, 5), dtype='i4')
        fout['test'][:] = 1

    buf.seek(0)
    with h5py.File(buf, 'r') as fin:
        ds = fin['test'].id

        num_chunks = ds.get_num_chunks()
        print("h5py number of chunks", num_chunks)
        for j in range(num_chunks):
            si = ds.get_chunk_info(j)
            print("Blob (Slice) (chunk offset) ", si.chunk_offset)
            print("Blob Byte offset", si.byte_offset)
            print("Blob size", si.size)


def main():
    """Run the damn thing."""
    build_zarr_array()
    zrf = _kerzarr_multiblob()
    test_get_chunk_details()


if __name__ == '__main__':
    main()
