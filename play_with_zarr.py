import numpy as np
import zarr


# multiblob array: each chunk stored separately
# see https://zarr.readthedocs.io/en/stable/api/storage.html
store = zarr.DirectoryStore('data/array.zarr')
z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
z[...] = 42
zarr.save("example.zarr", z)
