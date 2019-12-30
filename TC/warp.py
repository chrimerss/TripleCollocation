# use gdal to warp

from osgeo import gdal
import osgeo
import os
import h5py
import multiprocessing
import subprocess
import datetime
from glob import glob
import pandas as pd
import numpy as np

def geotransform(h5_file, dst):
    print(h5_file)
    try:
        h5= h5py.File(h5_file, 'r')['precipitationUncal'][:].transpose().squeeze()
    except KeyError:
        h5= h5py.File(h5_file, 'r')['precipitationCal'][:].transpose().squeeze()
    tif= gdal.Open('TCresultsHarvey/cc_gauge_mtc.tif')
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



def single(f):
    dst= '/Users/allen/Documents/TC/rainfall_analysis/GPM_data_warped/%s.tif'%f.split('/')[-1][:-5]
    timeIndex= datetime.datetime.strptime(f.split('/')[-1],'%Y%m%d%H%M%S.HDF5')
    dst_cleaned= '/Users/allen/Documents/TC/rainfall_analysis/GPM_data_warped/nimerg%sS%s.tif'%(timeIndex.strftime('%Y%m%d'),
                                                                                                timeIndex.strftime('%H%M%S'))
    if not os.path.exists(dst_cleaned):
        try:
            geotransform(f, dst)
            gdal.Warp(dst_cleaned, dst, xRes=0.0441979522, yRes=-0.04423076923, resampleAlg=osgeo.gdalconst.GRIORA_Bilinear)
        
            os.system('rm %s'%dst)
        except:
            print(dst)

def main():
    pool= multiprocessing.Pool(4)
    files= os.listdir('/Users/allen/Documents/TC/rainfall_analysis/GPM_data')
    full_path= [os.path.join('/Users/allen/Documents/TC/rainfall_analysis/GPM_data', file) for file in files if file.endswith('.HDF5')]
    pool.map(single, full_path)


def agg(file_1, file_2):
    time_prev= datetime.datetime.strptime(file_1, 'nimerg%Y%m%dS%H%M%S.tif')
    time_now= datetime.datetime.strptime(file_2, 'nimerg%Y%m%dS%H%M%S.tif')
    assert time_now-time_prev==datetime.timedelta(minutes=30), print(time_now, time_prev)
    time_new= (time_prev+ datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S')
    prefix= '../rainfall_analysis/GPM_data_agg/'
    if time_prev.hour==time_now.hour:
        print(time_prev, time_now)
        prev= gdal.Open('../rainfall_analysis/GPM_data_warped/'+file_1)
        now= gdal.Open('../rainfall_analysis/GPM_data_warped/'+file_2)
        new= (prev.ReadAsArray()+ now.ReadAsArray())/2.
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

def inspect():
    '''check the converted file'''
    prefix= '../rainfall_analysis/GPM_data_warped'
    names= os.listdir(prefix)
    bill= pd.date_range(start='2015-06-16', end='2015-06-18', freq='30T')
    cindy= pd.date_range(start='2017-06-22', end='2017-06-23', freq='30T')
    imeda= pd.date_range(start='2019-09-18', end='2019-09-21', freq='30T')
    total= bill.append(cindy)
    total= total.append(imeda)
    # print(total)
    # print(imeda)
    # total= pd.concat([bill, cindy, imeda])
    stored= sorted([datetime.datetime.strptime(name,'nimerg%Y%m%dS%H%M%S.tif') for name in names if name.endswith('.tif')])
    for time in total:
        if time not in stored:
            print(time)

def rename():
    '''rename Harvey GPM and mrms data because they are 1 hour ahead'''
    def _funcRadar(name):
        print(1)
        time= datetime.datetime.strptime(name,'nPrecipRate_00.00_%Y%m%d-%H%M%S.grib2-var0-z0.tif')+ datetime.timedelta(hours=1)
        new_name= time.strftime('nPrecipRate_00.00_%Y%m%d-%H%M%S.grib2-var0-z0.tif')
        os.system('mv ../cleaned/Harvey_mrms/%s ../cleaned/Harvey_mrms/%s'%(name, new_name))
        
    def _funcSat(name):
        print(1)
        time= datetime.datetime.strptime(name, 'nimerg%Y%m%dS%H%M%S.tif')+ datetime.timedelta(hours=1)
        new_name= time.strftime('nimerg%Y%m%dS%H%M%S.tif')
        os.system('mv ../cleaned/Harvey_GPM/%s ../cleaned/Harvey_GPM/%s'%(name, new_name))

    radar_pth= '../cleaned/Harvey_mrms'
    sat_pth= '../cleaned/Harvey_GPM'
    sat_names= sorted([f for f in os.listdir(sat_pth) if f.endswith('.tif')])[::-1]
    radar_names= sorted([f for f in os.listdir(radar_pth) if f.endswith('.tif')])[::-1]
    # print(radar_names)
    _= [_funcRadar(name) for name in radar_names]
    _= [_funcSat(name) for name in sat_names]

def _sum():
    pth= '../rainfall_analysis/GPM_data_agg'
    # _sum= np.zeros((208,293))
    first= True
    i=0
    for arr in os.listdir(pth):
        if arr.endswith('.tif'):
            data= gdal.Open(os.path.join(pth, arr)).ReadAsArray()
            if first:
                _sum= data.copy()
                first= False
            # try:
            #     data= h5py.File(os.path.join(pth, arr), 'r')['precipitationUncal'][:].transpose().squeeze()/2.
            # except KeyError:
            #     data= h5py.File(os.path.join(pth, arr), 'r')['precipitationCal'][:].transpose().squeeze()/2.
            _sum+=data
            i+=1
    
    print(np.nanmax(_sum))
    print(i)

if __name__=='__main__':
    # main()
    # agg_GPM()
    # inspect()
    #rename()
    _sum()