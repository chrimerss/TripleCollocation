import sys
sys.path.append('/Users/allen/Documents/Python/geoPackage')
from dataprocess import GeoData, ProductData,PixelTS
from tc import TripleCollocation
import os
import time
import random
from osgeo import gdal
import matplotlib.pyplot as plt
from geoPackage.visualize import layout
from geoPackage.io import ReadFile


def test_consistency():
    # radar_file= random.choice(os.listdir('../cleaned/radars'))
    # sat_file= random.choice(os.listdir('../cleaned/satellites'))
    # gauge_file= random.choice(os.listdir('../cleaned/gauges'))
    sat_file= 'nimerg20190919S140000.tif'
    radar_file= 'PrecipRate_00.00_20190919-140000.grib2-var0-z0.tif'
    gauge_file= 'ST2gg2019091914.Grb.tif'
    radar= ReadFile(os.path.join('..','cleaned','radars',radar_file)).raster
    sat= ReadFile(os.path.join('..','cleaned','satellites',sat_file)).raster
    gauge= ReadFile(os.path.join('..','cleaned','gauges',gauge_file)).raster
    fig, ax= plt.subplots(1,3,figsize=(12,4))
    map= layout(gauge, extent='local')
    ax[0].set_title(gauge_file)
    
    map= layout(radar, extent='local')
    ax[1].set_title(radar_file)

    map= layout(sat, extent='local')
    ax[2].set_title(sat_file)

    plt.show()

def test_radar_name():
    radar_files= os.listdir(os.path.join('..','cleaned','mrmsrt1H4kmw'))
    radar_folder= os.path.join('..','cleaned','mrmsrt1H4kmw')
    for each in sorted(radar_files):
        if each.endswith('.tif'):
            print(each)
            gd= GeoData(each, radar_folder,dType='radar')
            print(gd.getDateTime())
            print(gd)

def test_sat_name():
    sat_files= os.listdir(os.path.join('..','cleaned','GPMrt1H4kmw'))
    sat_folder= os.path.join('..','cleaned','GPMrt1H4kmw')
    for each in sorted(sat_files):
        if each.endswith('.tif'):
            print(each)
            gd= GeoData(each, sat_folder,dType='satellite')
            print(gd.getDateTime())
            print(gd)

def test_gauge_name():
    gau_files= os.listdir(os.path.join('..','cleaned','gauge4km'))
    gau_folder= os.path.join('..','cleaned','gauge4km')
    for each in sorted(gau_files):
        if each.endswith('.tif'):
            print(each)
            gd= GeoData(each, gau_folder,dType='gauge')
            print(gd.getDateTime(), gd.data)
            print(gd)

def test_product_ts():
    #test data object to produce time series plot
    folders= ['radar', 'satellite','gauge']
    for folder in folders:
        pdf= ProductData(folder)
        print(pdf.ts)

def test_pixelts():
    start= time.time()
    ts= PixelTS().singlePixel(0,0)
    end= time.time()
    print('time spent for retrieving one pixel: %.2f seconds'%(end-start))
    print(ts)

def test_preprocess():
    start= time.time()
    ts= PixelTS().singlePixel(26,26)
    tc= TripleCollocation()
    df= tc.preprocess(ts)
    print(ts)
    print(df)
    print(df.cov())
    end= time.time()
    print('time spent for retrieving one pixel: %.2f seconds'%(end-start))


def test_mtc():
    start= time.time()
    tc= TripleCollocation()
    tc.main()
    end= time.time()
    print('time spent for retrieving one pixel: %.2f seconds'%(end-start))

def test_parallel():
    start= time.time()
    tc= TripleCollocation()
    tc.parallel(cores=4)
    end= time.time()
    print('time spent for retrieving one pixel: %.2f seconds'%(end-start))

def test_parallel_write():
    start= time.time()
    tc= TripleCollocation()
    tc.parallel(cores=4,write=True)
    end= time.time()
    print('time spent for retrieving one pixel: %.2f seconds'%(end-start))

def test_ts_tc():
    start= time.time()
    tc= TripleCollocation()
    tc.ts_tc(cores=4)
    end= time.time()
    print('time spent for retrieving one pixel: %.2f seconds'%(end-start))

if __name__=='__main__':
    #test_radar_name() #pass radar name
    #test_sat_name().  #pass satellite name
    # test_gauge_name() #pass gauge name test
    # test_product_ts() #pass product test
    # test_pixelts().   # passed with 0.37s
    # test_preprocess() #passed
    # test_parallel()     #passed 17.97 for 100 pixels
    test_parallel_write() #passed 2.85 hours for whole map
    # test_ts_tc()
    # test_consistency()

