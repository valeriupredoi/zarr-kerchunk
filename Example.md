## Zarr and Kerchunks: match made in Hell

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

The `zr_file` object is a Python dictionary with the following keys:

```
dict_keys(['.zattrs', '.zgroup', '.zmetadata', 'height/.zarray', 'height/.zattrs', 'height/0', 'lat/.zarray', 'lat/.zattrs', 'lat/0', 'lat_bnds/.zarray', 'lat_bnds/.zattrs', 'lat_bnds/0.0', 'lon/.zarray', 'lon/.zattrs', 'lon/0', 'lon_bnds/.zarray', 'lon_bnds/.zattrs', 'lon_bnds/0.0', 'tas/.zarray', 'tas/.zattrs', 'tas/0.0.0', 'time/.zarray', 'time/.zattrs', 'time/0', 'time_bnds/.zarray', 'time_bnds/.zattrs', 'time_bnds/0.0'])
```

and we can examine the values eg `zr_file["lon/.zarray"]` is a list of one file URL:

```
zr_file["lon/.zarray"] = ['file:///home/valeriu/zarr-kerchunk/zarr_object/lon/.zarray']

```

that can then be opened with:

```python
   with fsspec.open(zr_file["lon/.zarray"][0]) as fil:
       for line in fil.readlines():
           print(line)
```

which returns a bunch of bytes-stuff:

```
b'{\n'
b'    "chunks": [\n'
b'        288\n'
b'    ],\n'
b'    "compressor": {\n'
b'        "blocksize": 0,\n'
b'        "clevel": 5,\n'
b'        "cname": "lz4",\n'
b'        "id": "blosc",\n'
b'        "shuffle": 1\n'
b'    },\n'
b'    "dtype": "<f8",\n'
b'    "fill_value": "NaN",\n'
b'    "filters": null,\n'
b'    "order": "C",\n'
b'    "shape": [\n'
b'        288\n'
b'    ],\n'
b'    "zarr_format": 2\n'
b'}'
```

or even better - but don't execue this - it'll print a bazillion lines (lots of data eh):

```python
   so = dict(
       anon=True, default_fill_cache=False, default_cache_type='first'
   )
   with fsspec.open(zr_file['tas/0.0.0'][0], **so) as fil:
       for line in fil.readlines():
           print(line)
```

`fil.readlines()` returns the flattened data array - a list of binaries of len 1872x192x288 = 104620032.

The variable metadata is accessible from here:

```python
   with fsspec.open(zr_file['tas/.zarray'][0]) as fil:
       for line in fil.readlines():
           print(line)
```

and that returns the var data specs:

```
b'{\n'
b'    "chunks": [\n'
b'        1872,\n'
b'        192,\n'
b'        288\n'
b'    ],\n'
b'    "compressor": {\n'
b'        "blocksize": 0,\n'
b'        "clevel": 5,\n'
b'        "cname": "lz4",\n'
b'        "id": "blosc",\n'
b'        "shuffle": 1\n'
b'    },\n'
b'    "dtype": "<f4",\n'
b'    "fill_value": 1.0000000200408773e+20,\n'
b'    "filters": null,\n'
b'    "order": "C",\n'
b'    "shape": [\n'
b'        1872,\n'
b'        192,\n'
b'        288\n'
b'    ],\n'
b'    "zarr_format": 2\n'
b'}'
```

The newly loaded file is usable with `xarray` like this:

```python
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
```

but ideally, we'd want to "steal" methods from `xarray` (specifically methods that return chunks and blobs)
and use them oustide the need to have the `xarray` package installed.
