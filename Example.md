## Zarr and Kerchung: match made in Hell

### Installing

`kerchunk` is a `conda-forge` package and can be installed with `mamba install -c conda-forge kerchunk`.

### Loading a `zarr` file with `kerchunk` library

`kerchunk` has a module called `zarr` that can be used with the `single_zarr()` method:

```python
import kerchunk.zarr as zr


# loading a zarr file I created from a netCDF CMIP dataset
store = "/home/valeriu/zarr-kerchunks/zarr_object"
zr_file = zr.single_zarr(store)  # tis a dictionary!
```
