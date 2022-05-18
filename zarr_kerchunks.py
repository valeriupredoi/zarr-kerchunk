"""
Use zarr and fsspec
"""
import os
import fsspec
import numpy as np
import xarray as xr
import time
import yaml
import struct
import zarr

import kerchunk
import kerchunk.zarr as zr
import dask.array as da


print(f"Kerchunk library at: {kerchunk.zarr.__file__}")


def _kerzarr():
    """Load a zarr file with fsspec.kerchunk and mess about."""
    store = "/home/valeriu/zarr-kerchunk/zarr_object"
    so = dict(
        anon=True, default_fill_cache=False, default_cache_type='first'
    )

    # understand the Zarr file structure
    # single_zarr is a nice wrapper to fsspec zarr loader
    zr_file = zr.single_zarr(store, storage_options=so)  # tis a dictionary!
    print(f"Zarr file keys: {zr_file.keys()}")

    # write a binary yaml with var matadata
    var_metadata_file = "./tas.yml"

    # get the info about variable data: chunks
    with fsspec.open(zr_file['tas/.zarray'][0]) as fil:
        lines = fil.readlines()
        with open(var_metadata_file, 'w') as file:
            yaml.safe_dump(lines, file, allow_unicode=True, default_flow_style=False)
        with open(var_metadata_file, 'rb') as file:
            params = yaml.safe_load(file)
            chunk_t = int(params[2].decode("ascii").strip().strip(","))
            chunk_x = int(params[3].decode("ascii").strip().strip(","))
            chunk_y = int(params[4].decode("ascii").strip())
    print(f"Data chunks: {chunk_t}, {chunk_x}, {chunk_y}")

    # get the actual data with fsspec
    # it's loooong list of binaries
    with fsspec.open(zr_file['tas/0.0.0'][0]) as fil:
        data_lines = fil.readlines()
        print(f"Flattened data len: {len(data_lines)}")  # bazillions
        print(f"222nd element in data lines {data_lines[222]}")

    # rechunk over flattened data
    # TODO need to find a way to pass binary data to dask !!!
    # rechunked_data = [
    #     data_lines[i:i + 10] for i in range(0, len(data_lines), 10)
    # ]
    # print(f"Rechunking raw data in {len(rechunked_data)} chunks.")
    # local_means = [
    #     da.mean(da.array(chunk)).compute() for chunk in rechunked_data
    # ]
    # print(local_means)

    # load with xarray
    ds = xr.open_dataset(
        "reference://", engine="zarr",
        backend_kwargs={
            "storage_options": {
                "fo": zr_file,
            },
            "consolidated": False
        }
    )
    print(f"Data file loaded by Xarray\n: {ds}")

    # big_chunks = chunks_t
    mean = da.mean(ds["tas"]).compute()
    print(f"Kerchunk/fsspec mean: {mean}")


    return


def _load_zarr():
    """Load Zarr file directly with Xarray."""
    zarr_dir = "/home/valeriu/zarr-kerchunk/zarr_object"
    coll = xr.open_zarr(zarr_dir, consolidated=True)
    global_mean = da.mean(coll["tas"]).compute()
    print(f"Simple Xarray/Zarr global mean: {global_mean}")

    return global_mean


def main():
    """Run the routine."""
    t0 = time.time()
    zarr_data = _load_zarr()
    t1 = time.time()
    dt1 = float(t1) - float(t0)
    print("Simple global mean took %.2f seconds" % dt1)
    print("\n")

    kerchunks_data = _kerzarr()    
    t2 = time.time()
    dt = float(t2) - float(t1)
    print("Kerchunk mean took %.2f seconds" % dt)


if __name__ == '__main__':
    main()
