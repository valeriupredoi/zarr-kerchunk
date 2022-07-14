import h5py
import dask.array as da
import dask
import numpy as np


def mini(arr):
    return da.min(arr)


cmip6_test_file = r"/home/valeriu/climate_data/CMIP6/CMIP/MPI-M/MPI-ESM1-2-HR/historical/r1i1p1f1/Omon/sos/gn/v20190710/sos_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_198001-198412.nc"

# CMIP6 data files open very well with h5py
# h5py=3.6.0nompi_py310he751f51_100=conda-forge
with h5py.File(cmip6_test_file, 'r') as fout:
    print(f"netcdf4 file {fout}")
    print(f"Data file keys {list(fout.keys())}")
    print("Variable is: sos")
    ds = fout['sos']

    # method 1: slice locally
    # make two slices; returns numpy.ndarray
    slice1 = ds[0:3]
    slice2 = ds[3:]

    # load slces in dask arrays and min down
    da_slice1 = da.from_array(slice1)
    da_slice2 = da.from_array(slice2)
    mean_da_slice1 = da.min(da_slice1)
    mean_da_slice2 = da.min(da_slice2)

    # compare results
    global_min = np.min(ds)
    dask_min = np.min([mean_da_slice1.compute(), mean_da_slice2.compute()])
    print(global_min, dask_min)

    # method 2: load hdf5 dataset straight into dask array
    dask_hdf = da.from_array(ds)
    print(dask_hdf, dask_hdf.chunks)
    slice_1 = dask_hdf[0]
    slice_2 = dask_hdf[3]
    m_1 = dask.delayed(mini)(slice_1)
    m_2 = dask.delayed(mini)(slice_2)
    min_g = dask.delayed(mini)(m_1, m_2)
    print(min_g)
    min_g.visualize(filename="df1.svg")
