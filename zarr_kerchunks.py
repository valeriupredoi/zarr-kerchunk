"""
Use zarr and fsspec
"""
import os
import fsspec
import numpy as np
import xarray as xr
import time
import yaml
import zarr

import kerchunk
import kerchunk.zarr as zr
print("Kerchunk library at: ", kerchunk.zarr.__file__)


def _kerzarr():
    """Load a zarr file with fsspec.kerchunk and mess about."""
    store = "/home/valeriu/zarr-kerchunk/zarr_object"

    so = dict(
        anon=True, default_fill_cache=False, default_cache_type='first'
    )

    zr_file = zr.single_zarr(store, storage_options=so)  # tis a dictionary!
    print("Zarr file keys: ", zr_file.keys())
    var_metadata_file = "./tas.yml"
    with fsspec.open(zr_file['tas/.zarray'][0]) as fil:
        lines = fil.readlines()
        with open(var_metadata_file, 'w') as file:
            yaml.safe_dump(lines, file, allow_unicode=True, default_flow_style=False)
        with open(var_metadata_file, 'rb') as file:
            params = yaml.safe_load(file)
            chunk_t = int(params[2].decode("ascii").strip().strip(","))
            chunk_x = int(params[3].decode("ascii").strip().strip(","))
            chunk_y = int(params[4].decode("ascii").strip())
            print("Chunks: ", chunk_t, chunk_x, chunk_y)
    with fsspec.open(zr_file['tas/0.0.0'][0]) as fil:
        print(fil.readlines())


    return


def load_zarr():
    zarr_dir = "/home/valeriu/zarr-kerchunk/zarr_object"
    coll = xr.open_zarr(zarr_dir, consolidated=True)
    return coll

def main():
    """Run the routine."""
    t0 = time.time()

    zarr_data = load_zarr()
    kerchunks_data = _kerzarr()
    
    t1 = time.time()
    dt = float(t1) - float(t0)
    print("Loop time %.2f seconds" % dt)


if __name__ == '__main__':
    main()
