import h5py
import numpy as np

# import xarray as xr


# CMIP5 files
#classic_cmip5_test_file = r"/home/valeriu/climate_data/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20120315/ta_Amon_MPI-ESM-LR_historical_r1i1p1_200001-200512.nc"
#bit64_cmip5_test_file = r"/home/valeriu/climate_data/cmip5/output1/NOAA-GFDL/GFDL-ESM2G/historical/mon/atmos/Amon/r1i1p1/v20120412/ta_Amon_GFDL-ESM2G_historical_r1i1p1_200101-200512.nc"

# CMIP6 files
cmip6_test_file = r"/home/valeriu/climate_data/CMIP6/CMIP/MPI-M/MPI-ESM1-2-HR/historical/r1i1p1f1/Omon/sos/gn/v20190710/sos_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_198001-198412.nc"

# CMIP6 data files open very well with h5py
# h5py=3.6.0nompi_py310he751f51_100=conda-forge
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
        si = ds_id.get_chunk_info(j)
        print("Blob (Slice) chunk offset", si.chunk_offset)
        print("Blob (Slice) byte offset", si.byte_offset)
        print("Blob (Slice) size", si.size)

    # slice this cake
    h5py_slice = ds[0:2]
    print(f"Slice on first dimension [0:2] {h5py_slice}")

    # multi block slice (hyperslab)
    multi_block = h5py.MultiBlockSlice(start=1, count=1, stride=2, block=2)
    multi_slice = ds[multi_block]
    print(f"Multiblock slice {multi_slice}")

    # get chunk info for slice
    idx1 = 0
    si = ds_id.get_chunk_info(idx1)
    # NOTE: we can't get arbitrary SLICE info, but only CHUNK info
    print(f"Slice chunk info is {si}")

    # so an intermediate mean operation can be done on each chunk
    chunk_means = []
    chunk_means_info = []
    for cidx in range(num_chunks):
        si = ds_id.get_chunk_info(cidx)
        chunk_array = ds[cidx]
        # need to mask off masked values of 1e20
        masked_chunk_array = np.ma.masked_where(chunk_array == 1.e20,
                                                chunk_array)
        chunk_mean = np.ma.mean(masked_chunk_array)
        chunk_means.append(chunk_mean)
        chunk_means_info.append(si)
        print(f"Local mean per chunk is {chunk_mean}")
        print(f"Chunk info is {si}")


# CMIP5 stuff
###########################
# CMIP5 classic not so much
# ncdump -k returns "classic" so Classic format
#  — Original NetCDF format, used by all NetCDF files created between 1989 and 2004 
#try:
#    with h5py.File(classic_cmip5_test_file, 'r') as fout:
#        print(fout)
#except OSError as exc:
#    print(exc)

# BUT a single pass through Xarray's converter does the trick!
# Caveat: need disk space to save it in netCDF4
#xr.open_dataset(classic_cmip5_test_file)[['ta']].to_netcdf('classic-outfile.nc')
#classic_reformed = r"/home/valeriu/zarr-kerchunk/classic-outfile.nc"
#try:
#    with h5py.File(classic_reformed, 'r') as fout:
#        print(f"netcdf4 file {fout} fresh from netCDF3 via Xarray")
#        print(f"Data file keys {list(fout.keys())}")
#        print("Variable is: ta")
#        ds = fout['ta'].id
#
#        num_chunks = ds.get_num_chunks()
#        print("h5py number of chunks", num_chunks)
#        for j in range(num_chunks):
#            si = ds.get_chunk_info(j)
#            print("Blob (Slice) chunk offset", si.chunk_offset)
#            print("Blob (Slice) byte offset", si.byte_offset)
#            print("Blob (Slice) size", si.size)
#except OSError as exc:
#    print(exc)
#
# CMIP5 64-bit offset is suffering too
#xr.open_dataset(bit64_cmip5_test_file)[['ta']].to_netcdf('bit64-outfile.nc')
#bit64_reformed = r"/home/valeriu/zarr-kerchunk/bit64-outfile.nc"
#try:
#    with h5py.File(bit64_reformed, 'r') as fout:
#        print(f"netcdf4 file {fout} fresh from netCDF3-64bit_offset via Xarray")
#        print(f"Data file keys {list(fout.keys())}")
#        print("Variable is: ta")
#        ds = fout['ta'].id
#
#        num_chunks = ds.get_num_chunks()
#        print("h5py number of chunks", num_chunks)
#        for j in range(num_chunks):
#            si = ds.get_chunk_info(j)
#            print("Blob (Slice) chunk offset", si.chunk_offset)
#            print("Blob (Slice) byte offset", si.byte_offset)
#            print("Blob (Slice) size", si.size)
#except OSError as exc:
#    print(exc)
