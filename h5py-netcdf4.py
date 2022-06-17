import h5py

classic_cmip5_test_file = r"/home/valeriu/climate_data/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/atmos/Amon/r1i1p1/v20120315/ta_Amon_MPI-ESM-LR_historical_r1i1p1_200001-200512.nc"
bit64_cmip5_test_file = r"/home/valeriu/climate_data/cmip5/output1/NOAA-GFDL/GFDL-ESM2G/historical/mon/atmos/Amon/r1i1p1/v20120412/ta_Amon_GFDL-ESM2G_historical_r1i1p1_200101-200512.nc"
cmip6_test_file = r"/home/valeriu/climate_data/CMIP6/CMIP/MPI-M/MPI-ESM1-2-HR/historical/r1i1p1f1/Omon/sos/gn/v20190710/sos_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_198001-198412.nc"

# CMIP6 data files open very well with h5py
# h5py=3.6.0nompi_py310he751f51_100=conda-forge
with h5py.File(cmip6_test_file, 'r') as fout:
    print(f"netcdf4 file {fout}")
    print(f"Data file keys {list(fout.keys())}")
    print("Variable is: sos")
    ds = fout['sos'].id

    num_chunks = ds.get_num_chunks()
    print("h5py number of chunks", num_chunks)
    for j in range(num_chunks):
        si = ds.get_chunk_info(j)
        print("Blob (Slice) chunk offset", si.chunk_offset)
        print("Blob (Slice) byte offset", si.byte_offset)
        print("Blob (Slice) size", si.size)

# CMIP5 classic not so much
# ncdump -k returns "classic" so Classic format
#  — Original NetCDF format, used by all NetCDF files created between 1989 and 2004 
try:
    with h5py.File(classic_cmip5_test_file, 'r') as fout:
        print(fout)
except OSError as exc:
    print(exc)

# CMIP5 64-bit offset is suffering too
try:
    with h5py.File(bit64_cmip5_test_file, 'r') as fout:
        print(fout)
except OSError as exc:
    print(exc)
