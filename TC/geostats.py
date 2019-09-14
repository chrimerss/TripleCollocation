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
            'csi':np.zeros((i,j), dtype=np.float16)
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
    end= time.time()
    print('total elapsed time : %.2f hours!'%((end-start)/3600.))

    write_geotiff('./intercomparison/'+x+'_'+y, stats)

def single(arg):
    ii,jj,x,y= arg
    print('processing %d-%d'%(ii,jj))
    ts= PixelTS().singlePixel(ii,jj)
    rmse= RMSE(ts[x].values.astype(float), ts[y].values.astype(float))
    mae= MAE(ts[x].values.astype(float), ts[y].values.astype(float))
    r= R(ts[x].values.astype(float), ts[y].values.astype(float))
    norm_rmse= normRMSE(ts[x].values.astype(float), ts[y].values.astype(float))
    norm_mae= normMAE(ts[x].values.astype(float), ts[y].values.astype(float))
    bias= biasRatio(ts[x].values.astype(float), ts[y].values.astype(float))
    pod= POD(ts[x].values.astype(float), ts[y].values.astype(float))
    far=FAR(ts[x].values.astype(float), ts[y].values.astype(float))
    csi= CSI(ts[x].values.astype(float), ts[y].values.astype(float))
    stat= {'rmse': rmse,
            'mae': mae,
            'r': r,
            'norm rmse': norm_rmse,
            'norm mae': norm_mae,
            'bias': bias,
            'pod': pod,
            'far': far,
            'csi': csi}

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
            'csi':np.zeros((i,j), dtype=np.float16)
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
    mask= np.isnan(x) & np.isnan(y)
    x= x[~mask]
    y= y[~mask]

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
    if x.mean()==0:
        return np.nan
    else:
        return RMSE(x,y)/x.mean()

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
    if x.mean()==0:
        return np.nan
    else:
        return MAE(x, y)/x.mean()

def biasRatio(x, y):
    x,y= nonnan(x, y)
    if y.sum()==0:
        return np.nan
    else:
        return (x-y).sum()/y.sum()

def POD(x, y, threshold=0.2):
    x,y= nonnan(x, y)
    a= (x>threshold) & (y>threshold)
    b= (x<threshold) & (y>threshold)
    if (a.sum()+b.sum())==0:
        return np.nan
    else:
        return a.sum()/(a.sum()+b.sum())

def FAR(x,y,threshold=0.2):
    x,y= nonnan(x, y)
    c= (x>threshold) & (y<threshold)
    a= (x>threshold) & (y>threshold)
    if (a.sum()+c.sum())==0:
        return np.nan
    else:
        return c.sum()/(a.sum()+c.sum())

def CSI(x,y,threshold=0.2):
    x,y= nonnan(x, y)
    a= (x>threshold) & (y>threshold)
    b= (x<threshold) & (y>threshold)
    c= (x>threshold) & (y<threshold)
    if (a.sum()+b.sum()+c.sum())==0:
        return np.nan
    else:
        return a.sum()/(a.sum()+b.sum()+c.sum())

def write_geotiff(dst, new_dict):
        pth= '../cleaned/gauge4km/ST2gg2017082500.Grb.tif'
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

if __name__=='__main__':
    parallel()     #2.48 hours to run
