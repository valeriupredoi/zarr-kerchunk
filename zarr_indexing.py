import h5py
import itertools
import os
import zarr

from io import BytesIO
from zarr.indexing import PartialChunkIterator


def build_zarr_dataset():
    """Create a zarr array and save it."""
    store = zarr.DirectoryStore('data/array.zarr')
    z = zarr.zeros((60, 404, 802), chunks=(12, 80, 160), store=store, overwrite=True)
    z[...] = 42.
    zarr.save_array("example.zarr", z, compressor=None)


def h5py_chunk_slice_info():
    """Use h5py and zarr utility to get chunk/slice info."""
    buf = BytesIO()
    with h5py.File(buf, 'w') as fout:
        fout.create_dataset('test', shape=(60, 404, 802),
                            chunks=(12, 80, 160), dtype='f8')
        fout['test'][:] = 42.
    buf.seek(0)

    with h5py.File(buf, 'r') as fin:
        ds = fin['test']
        ds_id = fin['test'].id
        num_chunks = ds_id.get_num_chunks()
        print("\n")
        print("H5Py stuffs")
        print("==================")
        print(f"Dataset number of chunks is {num_chunks}")
        print("Analyzing first 3 chunks")
        for j in range(3):
            print(f"Chunk index {j}")
            si = ds_id.get_chunk_info(j)  # return a StoreInfo object
            print("Blob (Slice) chunk offset", si.chunk_offset)
            print("Blob (Slice) byte offset", si.byte_offset)
            print("Blob (Slice) size", si.size)

        # slice this cake
        data_slice = ds[0:2]  # get converted to an ndarray
        print(f"Slice array shape {ds.shape}")
        PCI = PartialChunkIterator((slice(0, 2, 2), ), ds.shape)
        print(list(PCI))
        print(PCI.arr_shape)
        print("\n")


def zarr_chunk_slice_info():
    """Use pure zarr insides to get chunk/slice info."""
    zarr_dir = "./example.zarr"
    if not os.path.isdir(zarr_dir):
        build_zarr_dataset()
    ds = zarr.open("./example.zarr")
    print("Zarr stuffs")
    print("==================")
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

    # slice this cake
    data_slice = ds[0:2]  # get converted to an ndarray
    print(f"Slice array shape {ds.shape}")
    PCI = PartialChunkIterator((slice(0, 2, 2), ), ds.shape)
    print(list(PCI))
    print(PCI.arr_shape)
    print("\n")


def main():
    "Run the meat."""
    h5py_chunk_slice_info()
    zarr_chunk_slice_info()


if __name__ == '__main__':
    main()
