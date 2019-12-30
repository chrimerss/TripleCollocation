import numpy as np
import matplotlib.pyplot as plt
import os
from osgeo import gdal
from dataprocess import GeoData, ProductData, PixelTS
import multiprocessing
from functools import partial
from scipy.stats import pearsonr
from tqdm import tqdm
import time

def parallel(x='gauge', y='radar'):
    i= 208; j= 293
    stats= {'rmse': np.zeros((i,j), dtype=np.float16),
            'mae': np.zeros((i,j), dtype=np.float16),
            'norm rmse':np.zeros((i,j), dtype=np.float16),
            'norm mae':np.zeros((i,j), dtype=np.float16),
            'bias':np.zeros((i,j), dtype=np.float16),
            'r':np.zeros((i,j), dtype=np.float16),
            'pod':np.zeros((i,j), dtype=np.float16),
            'far':np.zeros((i,j), dtype=np.float16),
            'csi':np.zeros((i,j), dtype=np.float16),
            'sum_%s'%x:np.zeros((i,j), dtype= np.float16),
            'sum_%s'%y:np.zeros((i,j), dtype= np.float16)
            }

    start= time.time()
    pool= multiprocessing.Pool(4)
    comb= [(ii,jj,x,y) for ii in range(i) for jj in range(j)]
    results= pool.map(single, comb)
    for stat, ii, jj in results:
        stats['rmse'][ii,jj]= stat['rmse']
        stats['mae'][ii,jj]= stat['mae']
        stats['r'][ii,jj]= stat['r']
        stats['norm mae'][ii,jj]= stat['norm mae']
        stats['norm rmse'][ii,jj]= stat['norm rmse']
        stats['pod'][ii,jj]= stat['pod']
        stats['far'][ii,jj]= stat['far']
        stats['csi'][ii,jj]= stat['csi']
        stats['sum_%s'%x][ii,jj]= stat['sum_%s'%x]
        stats['sum_%s'%y][ii,jj]= stat['sum_%s'%(y)]
    end= time.time()
    print('total elapsed time : %.2f hours!'%((end-start)/3600.))

    write_geotiff('./intercomparison/'+x+'_'+y, stats)

def single(arg):
    ii,jj,x,y= arg
    print('processing %d-%d'%(ii,jj))
    ts= PixelTS().singlePixel(ii,jj)
    ts.radar= ts.radar.shift()
    ts.satellite= ts.satellite.shift()
    rmse= RMSE(ts[x].values.astype(float), ts[y].values.astype(float))
    mae= MAE(ts[x].values.astype(float), ts[y].values.astype(float))
    r= R(ts[x].values.astype(float), ts[y].values.astype(float))
    norm_rmse= normRMSE(ts[x].values.astype(float), ts[y].values.astype(float))
    norm_mae= normMAE(ts[x].values.astype(float), ts[y].values.astype(float))
    bias= totalVolumeRatio(ts[x].values.astype(float), ts[y].values.astype(float))
    pod= POD(ts[x].values.astype(float), ts[y].values.astype(float),)
    far=FAR(ts[x].values.astype(float), ts[y].values.astype(float))
    csi= CSI(ts[x].values.astype(float), ts[y].values.astype(float))
    _sum_x= Sum(ts[x].values.astype(float))
    _sum_y= Sum(ts[y].values.astype(float))
    stat= {'rmse': rmse,
            'mae': mae,
            'r': r,
            'norm rmse': norm_rmse,
            'norm mae': norm_mae,
            'bias': bias,
            'pod': pod,
            'far': far,
            'csi': csi,
            'sum_%s'%x: _sum_x,
            'sum_%s'%y: _sum_y}

    return stat, ii, jj

def stats(x='gauge', y='radar'):
    i= 208; j= 293
    stat= {'rmse': np.zeros((i,j), dtype=np.float16),
            'mae': np.zeros((i,j), dtype=np.float16),
            'norm rmse':np.zeros((i,j), dtype=np.float16),
            'norm mae':np.zeros((i,j), dtype=np.float16),
            'bias':np.zeros((i,j), dtype=np.float16),
            'r':np.zeros((i,j), dtype=np.float16),
            'pod':np.zeros((i,j), dtype=np.float16),
            'far':np.zeros((i,j), dtype=np.float16),
            'csi':np.zeros((i,j), dtype=np.float16),
            'sum_%s'%x: np.zeros((i,j), dtype=np.float16),
            'sum_%s'%y: np.zeros((i,j), dtype=np.float16)
            }

    for ii in tqdm(range(i)):
        for jj in range(j):
            print('processing %d-%d'%(ii,jj))
            ts= PixelTS().singlePixel(ii,jj)
            stat['rmse'][ii,jj]= RMSE(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['mae'][ii,jj]= MAE(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['r'][ii,jj]= R(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['norm rmse'][ii,jj]= normRMSE(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['norm mae'][ii,jj]= normMAE(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['bias'][ii,jj]= biasRatio(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['pod'][ii,jj]= POD(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['far'][ii,jj]=FAR(ts[x].values.astype(float), ts[y].values.astype(float))
            stat['csi'][ii,jj]= CSI(ts[x].values.astype(float), ts[y].values.astype(float))

    write_geotiff(x+'_'+y, stat)

def nonnan(x,y):
    # print(type(x))
    mask= (x>=0) & (y>=0)
    x= x[mask]
    y= y[mask]

    return x, y

def RMSE(x,y):
    '''calculation of rmse'''
    x,y= nonnan(x, y)
    if len(x)!=len(y):
        raise ValueError('length of x is not equal to y')
    elif len(x)==0:
        return np.nan
    else:
        return (((x-y)**2).sum()/len(x))**0.5

def R(x,y):
    x,y= nonnan(x, y)
    if len(x)!=len(y):
        raise ValueError('length of x is not equal to y')
    elif len(x)<=1:
        return np.nan
    else:
        return pearsonr(x,y)[0]

def normRMSE(x, y):
    x,y= nonnan(x, y)
    if len(x)==0:
        return np.nan
    elif x.max()-x.mean()==0:
        return np.nan
    else:
        return RMSE(x,y)/(x.max()-x.mean())

def MAE(x, y):
    x,y= nonnan(x, y)
    if len(x)!=len(y):
        raise ValueError('length of x is not equal to y')
    elif len(x)==0:
        return np.nan
    else:
        return np.abs(x-y).sum()/len(x)

def normMAE(x, y):
    x,y= nonnan(x, y)
    if len(x)==0:
        return np.nan
    elif x.max()-x.mean()==0:
        return np.nan
    else:
        return MAE(x, y)/(x.max()-x.mean())

def totalVolumeRatio(x, y):
    x,y= nonnan(x, y)
    if len(x)==0 or y.sum()==0:
        return np.nan
    else:
        return x.sum()/y.sum()

def POD(x, y, threshold=0.2):
    x,y= nonnan(x, y)
    a= (x>=threshold) & (y>=threshold)
    b= (x<threshold) & (y>=threshold)
    if len(a)==0 or (a.sum()+b.sum())==0:
        return np.nan
    else:
        return a.sum()/(a.sum()+b.sum())

def FAR(x,y,threshold=0.2):
    x,y= nonnan(x, y)
    c= (x>=threshold) & (y<threshold)
    a= (x>=threshold) & (y>=threshold)
    if len(a)==0 or (a.sum()+c.sum())==0:
        return np.nan
    else:
        return c.sum()/(a.sum()+c.sum())

def CSI(x,y,threshold=0.2):
    x,y= nonnan(x, y)
    a= (x>=threshold) & (y>=threshold)
    b= (x<threshold) & (y>=threshold)
    c= (x>=threshold) & (y<threshold)
    if len(a)==0 or (a.sum()+b.sum()+c.sum())==0:
        return np.nan
    else:
        return a.sum()/(a.sum()+b.sum()+c.sum())

def Sum(x):
    y= np.zeros(len(x))
    x,y= nonnan(x, y)
    if len(x)==0:
        return np.nan
    else:
        return x.sum()

def write_geotiff(dst, new_dict):
        pth= '../cleaned/Harvey_gauge/ST2gg2017082500.Grb.tif'
        sample= gdal.Open(pth)
        projection= sample.GetProjection()
        trans= sample.GetGeoTransform()
        bands= 1
        for key in new_dict.keys():
            new_array= new_dict[key]
            driver= gdal.GetDriverByName("GTiff")
            rows, cols= new_array.shape
            outdata = driver.Create(dst+'_'+key+'.tif', cols, rows, bands, gdal.GDT_Float32)
            outdata.SetGeoTransform(trans)
            outdata.SetProjection(projection)
            outdata.GetRasterBand(bands).WriteArray(new_array)
            outdata.FlushCache()
            outdata = None

def rainAmount():
    radar_pth= '../cleaned/Harvey_mrms'
    gauge_pth= '../cleaned/Harvey_gauge'
    sat_pth= '../cleaned/Harvey_GPM'
    seqs= len(os.listdir(gauge_pth))

if __name__=='__main__':
    parallel('satellite', 'radar')     #2.48 hours to run
