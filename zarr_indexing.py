import h5py
import itertools
import zarr

from zarr.indexing import PartialChunkIterator


print("\nGet some libs paths")
print("=====================")
print(f"Zarr library at: {zarr.__file__}")

# CMIP6 files
cmip6_test_file = r"/home/valeriu/climate_data/CMIP6/CMIP/MPI-M/MPI-ESM1-2-HR/historical/r1i1p1f1/Omon/sos/gn/v20190710/sos_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_198001-198412.nc"


def build_zarr_array():
    """Create the zarr array."""
    store = zarr.DirectoryStore('data/array.zarr')
    z = zarr.zeros((60, 404, 802), chunks=(60, 404, 802), store=store, overwrite=True)
    z[...] = 42
    zarr.save_array("example.zarr", z, compressor=None)


with h5py.File(cmip6_test_file, 'r') as fout:
    print(f"netcdf4 file {fout}")
    print(f"Data file keys {list(fout.keys())}")
    print("Variable is: sos")
    ds = fout['sos']
    print(f"Variable dataset {ds}")
    print(f"Variable data shape {ds.shape}")
    ds_id = ds.id
    print(f"Dataset id is {ds_id}")
    num_chunks = ds_id.get_num_chunks()
    print(f"Dataset number of chunks is {num_chunks}")
    print("Analyzing first 3 chunks")
    for j in range(3):
        print(f"Chunk index {j}")
        si = ds_id.get_chunk_info(j)  # return a StoreInfo object
        print("Blob (Slice) chunk offset", si.chunk_offset)
        print("Blob (Slice) byte offset", si.byte_offset)
        print("Blob (Slice) size", si.size)

    # slice this cake
    data_slice = ds[0:2]  # ndarray
    print(ds.shape)
    PCI = PartialChunkIterator((slice(0, 2, 2), ), ds.shape)
    print(list(PCI))
    print(PCI.arr_shape)

    # build_zarr_array()

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
    print("Chunk offsets", [tuple(k) for k in chunk_offsets])
    print("Zarr number of chunks", len(list(itertools.product(*chunk_offsets))))
    for offset in itertools.product(*chunk_offsets):
        print("Slice offset", offset)
        sel = tuple(slice(o, min(s, o + c))
                    for o, s, c in zip(offset, shape, chunks))
        islice = ds[sel]
        print(f"Slice indices {sel}")
        slice_size = islice.size * islice.dtype.itemsize
        print(f"Slice size {slice_size}")

    tot_size = ds.size * ds.dtype.itemsize
    print(f"Total chunks size {tot_size}")
