from dataprocess import GeoData, ProductData,PixelTS
from tc import TripleCollocation
import os
import time

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

if __name__=='__main__':
    #test_radar_name() #pass radar name
    #test_sat_name().  #pass satellite name
    # test_gauge_name() #pass gauge name test
    # test_product_ts() #pass product test
    # test_pixelts().   # passed with 0.37s
    # test_preprocess() #passed
    # test_parallel()     #passed 17.97 for 100 pixels
    test_parallel_write() #passed 2.85 hours for whole map

