# use gdal to warp

from osgeo import gdal
import osgeo
import os
import h5py
import multiprocessing
import subprocess
import datetime

def geotransform(h5_file, dst):
    print(h5_file)
    h5= h5py.File(h5_file, 'r')['precipitationUncal'][:].transpose().squeeze()
    tif= gdal.Open('TCresults/cc_gauge.tif')
    lons= h5py.File(h5_file, 'r')['lon'][:]
    lats= h5py.File(h5_file, 'r')['lat'][:]
    rows, cols= h5.shape
    Xori= -101.0
    Yori= 25.5
    xRes= lats[2]-lats[1]
    yRes= lons[2]- lons[1]
    geo_trans= [Xori, xRes, 0, Yori, 0, yRes]
    driver= gdal.GetDriverByName("GTiff")
    outdata = driver.Create(dst, cols, rows, 1, gdal.GDT_Float32)
    # crs= tif.GetProjection()
    outdata.SetGeoTransform(geo_trans)
    outdata.SetProjection(tif.GetProjection())
    outdata.GetRasterBand(1).WriteArray(h5)

def geotransform_gird(grid_file, dst):

    h5= h5py.File(h5_file, 'r')['precipitationUncal'][:].transpose().squeeze()

    lons= h5py.File(h5_file, 'r')['lon'][:]
    lats= h5py.File(h5_file, 'r')['lat'][:]
    rows, cols= h5.shape
    Xori= -101.0
    Yori= 25.5
    xRes= lats[2]-lats[1]
    yRes= lons[2]- lons[1]
    geo_trans= [Xori, xRes, 0, Yori, 0, yRes]
    driver= gdal.GetDriverByName("GTiff")
    outdata = driver.Create(dst, cols, rows, 1, gdal.GDT_Float32)
    outdata.SetGeoTransform(geo_trans)
    outdata.SetProjection("EPSG:4326")
    outdata.GetRasterBand(1).WriteArray(h5)

def single(f):
    dst= '/Users/allen/Documents/TC/rainfall_analysis/GPM_data_warped/%s.tif'%f.split('/')[-1][:-5]
    dst_cleaned= '/Users/allen/Documents/TC/rainfall_analysis/GPM_data_warped/nimerg%sS%s.tif'%(f.split('/')[-1].split('.')[0][:8],
                                                                                                f.split('/')[-1].split('.')[0][8:])
    geotransform(f, dst)
    gdal.Warp(dst_cleaned, dst, xRes=0.0441979522, yRes=-0.04423076923, resampleAlg=osgeo.gdalconst.GRIORA_Bilinear)
    os.system('rm %s'%dst)

def main():
    pool= multiprocessing.Pool(4)
    files= os.listdir('/Users/allen/Documents/TC/rainfall_analysis/GPM_data')
    full_path= [os.path.join('/Users/allen/Documents/TC/rainfall_analysis/GPM_data', file) for file in files if file.endswith('.HDF5')]
    pool.map(single, full_path)

def agg(file_1, file_2):
    time_prev= datetime.datetime.strptime(file_1, 'nimerg%Y%m%dS%H%M%S.tif')
    time_now= datetime.datetime.strptime(file_2, 'nimerg%Y%m%dS%H%M%S.tif')
    time_new= (time_prev+ datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S')
    prefix= '../rainfall_analysis/GPM_data_agg/'
    if time_prev.hour==time_now.hour:
        print(time_prev, time_now)
        prev= gdal.Open('../rainfall_analysis/GPM_data_warped/'+file_1)
        now= gdal.Open('../rainfall_analysis/GPM_data_warped/'+file_2)
        new= prev.ReadAsArray()+ now.ReadAsArray()
        rows, cols= new.shape
        driver= gdal.GetDriverByName("GTiff")
        outdata = driver.Create(prefix+'nimerg%sS%s.tif'%(time_new[:8], time_new[8:]), cols, rows, 1, gdal.GDT_Float32)
        outdata.SetGeoTransform(prev.GetGeoTransform())
        outdata.SetProjection(prev.GetProjection())
        outdata.GetRasterBand(1).WriteArray(new)


def agg_GPM():
    folder= '../rainfall_analysis/GPM_data_warped'
    files= sorted([file for file in os.listdir(folder) if file.endswith('.tif')])
    num= len(files)//2
    i=0
    while i<num:
        prev, now= files[i*2:(i+1)*2]
        agg(prev, now)
        i+=1

def mask():
    '''This function select common timestamp for gauge, radar and GPM to reduce computation burden'''
    gauge_folder= 'raingauge'
    GPM_folder= 'GPM_data_agg'
    radar_folder= 'mrmsPrate'
    time_gauge= []
    time_radar= []
    time_sat= []

if __name__=='__main__':
    main()
    # agg_GPM()
