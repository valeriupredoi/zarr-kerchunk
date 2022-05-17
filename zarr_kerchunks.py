"""
Use zarr and fsspec
"""
import os
import fsspec
import numpy as np
import xarray as xr
import time
import zarr
from fsspec.implementations.reference import ReferenceFileSystem

import kerchunk.zarr as zr

# def load_reference():
    # store = "/home/valeriu/Excalibur-and-Parallel-Data/initial_exploration/initial_testing/zarr_object"
    # z1 = zarr.open(store)
    # a = np.arange(10)
    # zarr.save("example.zarr", a)
    # ref_ds = ReferenceFileSystem(store)  # (fo=z1)
    # returns a ReferenceFileSystem object

    # play around with the zr module
    # zr_file = zr.single_zarr(store)
    # print(dir(zr_file))
    # return zr_file

def _kerzarr():
   """Load a zarr file with fsspec.kerchunk and mess about."""
   store = "/home/valeriu/zarr-kerchunks/zarr_object"
   zr_file = zr.single_zarr(store)  # tis a dictionary!
   print(zr_file.keys())
   return


def load_zarr():
    zarr_dir = "/home/valeriu/zarr-kerchunks/zarr_object"
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
