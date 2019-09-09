'''
Triple Collocation Method
'''

import numpy as np
import matplotlib.pyplot as plt
import os
from osgeo import gdal
from dataprocess import GeoData, ProductData, PixelTS
import multiprocessing
from functools import partial

class TripleCollocation(object):
    '''
    About Triple Collocation, see provided .pdf file
    Main Program

    Methods:
    -----------------

    '''

    def __init__(self):
        pass

    def main(self):
        i= 208; j= 293
        RMSE= np.zeros((i*j,3), dtype=np.float16)
        CC= np.zeros((i*j,3), dtype=np.float16)
        ind=0
        for ii in range(i):
            for jj in range(j):
                print('processing (%d,%d)'%(ii,jj))
                ts= self.preprocess(ts)
                ts[ts<0]= 0
                if len(ts)==0 or len(ts)<3:
                    RMSE[ind,:]= -9999
                    CC[ind,:]= -9999
                else:
                    _sig, _r= self.mtc(ts)
                    RMSE[ind,:]= _sig
                    CC[ind,:]= _r
        RMSE_radar= RMSE[:,0].reshape(i,j)
        RMSE_sat= RMSE[:,1].reshape(i,j)
        RMSE_gauge= RMSE[:,2].reshape(i,j)

        CC_radar= CC[:,0].reshape(i,j)
        CC_sat= CC[:,1].reshape(i,j)
        CC_gauge= CC[:,2].reshape(i,j)

        return (RMSE_radar, RMSE_sat, RMSE_gauge), (CC_radar, CC_sat, CC_gauge)

    def parallel(self, cores=4, write=False):
        RMSE= np.zeros((208,293,3), dtype=np.float16)
        CC= np.zeros((208, 293,3), dtype=np.float16)
        inputs= [(i,j) for i in range(208) for j in range(293)]
        pool= multiprocessing.Pool(cores)
        results= pool.map(self.single, inputs)
        for r,c,i,j in results:
            RMSE[i,j,:]= r
            CC[i,j,:]= c
        RMSE_radar= RMSE[:,:,0]
        RMSE_sat= RMSE[:,:,1]
        RMSE_gauge= RMSE[:,:,2]

        CC_radar= CC[:,:,0]
        CC_sat= CC[:,:,1]
        CC_gauge= CC[:,:,2]

        if write:
            self.write_geotif('rmse_radar.tif',RMSE_radar)
            self.write_geotif('rmse_sat.tif',  RMSE_sat)
            self.write_geotif('rmse_gauge.tif',RMSE_gauge)

            self.write_geotif('cc_radar.tif',CC_radar)
            self.write_geotif('cc_sat.tif',  CC_sat)
            self.write_geotif('cc_gauge.tif',CC_gauge)

            # self.write_geotif('test.tif', np.zeros((208,293)))

        return RMSE, CC

    def single(self, args):
        i,j = args
        print('processing (%d,%d)'%(i,j))
        RMSE= np.zeros(3, dtype=np.float16)
        CC= np.zeros(3, dtype=np.float16)
        ts= PixelTS().singlePixel(i,j)
        ts= self.preprocess(ts)
        # print('length of ts: ', len(ts))
        if len(ts)==0 or len(ts)<3:
            RMSE[:]= np.array([-9999]*3)
            CC[:]= np.array([-9999]*3)
        else:
            _sig, _r= self.mtc(ts)
            RMSE[:]= _sig
            CC[:]= _r

        return RMSE, CC, i,j



    def preprocess(self, data, threshold1=0, threshold2=0.01):
        # this function drops nan and values between thresholds
        data= data.astype('float32')
        cols= data.columns
        for col in cols:
            data=data[data[col]>=threshold2]
            data.clip(lower=threshold2, inplace=True)
            # data.dropna(inplace=True)
        # print(data)
        data= data.apply(np.log)

        return data


    def mtc(self, X):
        #check X has shape (time sequence, 3)
        N_boot= 100
        rmse= np.zeros((N_boot,3))
        cc= np.zeros((N_boot, 3))
        for i in range(N_boot):
            sigma= np.zeros(3)
            r= np.zeros(3)
            sample= self.bootstrap_resample(X, n=N_boot)
            cov= X.cov().to_numpy()
            # print(cov)
            # compute RMSE
            if (cov==0).any().any():
                rmse[i,:]=np.nan
                cc[i,:]= np.nan
            else:
                sigma[0]= cov[0,0] - (cov[0,1]*cov[0,2])/(cov[1,2])
                sigma[1]= cov[1,1] - (cov[0,1]*cov[1,2])/(cov[0,2])
                sigma[2]= cov[2,2] - (cov[0,2]*cov[1,2])/(cov[0,1])

                sigma[sigma<0]= 0.0001
                sigma= sigma**.5
                sigma[sigma<10^-3] = 0

                #compute correlation coefficient
                r[0] = (cov[0,1]*cov[0,2])/(cov[0,0]*cov[1,2])
                r[1] = (cov[0,1]*cov[1,2])/(cov[1,1]*cov[0,2]);
                r[2] = (cov[0,2]*cov[1,2])/(cov[2,2]*cov[0,1]);

                #sign function here?
                r[r<0] = 0.0001
                r[r>1] = 1
                r= r**.5
                r[r<10^-3] = 0

                rmse[i,:]= sigma
                cc[i, :]= r

        return rmse.mean(axis=0), cc.mean(axis=0)




    def bootstrap_resample(self, X, n=None):
        """ Bootstrap resample an array_like
        Parameters
        ----------
        X : pandas data frame
          data to resample
        n : int, optional
          length of resampled array, equal to len(X) if n==None
        Results
        -------
        returns X_resamples
        """
        if n == None:
            n = len(X)

        resample_i = np.floor(np.random.rand(n)*len(X)).astype(int)
        X_resample = X.iloc[resample_i, :]
        return X_resample

    def write_geotif(self, dst, new_array):
        #read sample
        pth= '../cleaned/gauge4km/ST2gg2017082500.Grb.tif'
        sample= gdal.Open(pth)
        projection= sample.GetProjection()
        trans= sample.GetGeoTransform()
        bands= 1
        driver= gdal.GetDriverByName("GTiff")
        rows, cols= new_array.shape

        outdata = driver.Create(dst, cols, rows, bands, gdal.GDT_Float32)

        outdata.SetGeoTransform(trans)
        outdata.SetProjection(projection)
        outdata.GetRasterBand(bands).WriteArray(new_array)
        outdata.FlushCache()
        outdata = None
