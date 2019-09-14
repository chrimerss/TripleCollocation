from osgeo import gdal
import re
import numpy as np
import datetime
import os
import pandas as pd
# import xarray as xr

class PixelTS(object):
    '''
    pixel wise time series data producer

    Methods:
    ------------------
    :generator() - generator; return pixel time series data
    :singlePixel - return pixel time series data

    Attributes:
    ------------------
    :self.pixelts - pandas.DataFrame; pixel time series
    '''
    def __init__(self):
        self.products= ['radar', 'satellite', 'gauge']
        _data= self.getProductData(self.products[0])
        self.size= list(_data.values())[0].shape
        self._df= pd.DataFrame(index=_data.keys(), columns=['radar', 'satellite', 'gauge'])

    def generator(self):

        for ii in self.size[0]:
            for jj in self.size[1]:
                yield singlePixel(ii,jj)

    def singlePixel(self, i, j):
        for product in self.products:
            _data= self.getProductData(product)
            for it, arr in _data.items():
                self._df.loc[it,product]= arr[i,j]

        return self._df

    def getProductData(self, product):
        return ProductData(product).ts

    @property
    def pixelts(self):
        return self._df


class ProductData(object):
    '''
    Load product data into time series dictionary

    Arguments:
    ------------------
    :product - str; one of [radar, satellite, gauge]
    :folder - list; ordered list [radar, satellite, gauge]

    Attributes:
    ------------------
    :self.ts - dict; {datetime: array}; return time series data
    '''

    def __init__(self, product, folder=None):

        self.product= product
        self.folder= folder
        if self.folder is None:
            if self.product== 'radar':
                folder= '../cleaned/mrmsrt1H4kmw'
            elif self.product== 'satellite':
                folder= '../cleaned/GPMrt1H4kmw'
            elif self.product== 'gauge':
                folder= '../cleaned/gauge4km'
        else:
            radar= self.folder[0]
            satellite= self.folder[1]
            gauge= self.floder[2]
        # load time series
        self._ts_data= self.loadTS(folder, product)

    def loadTS(self, folder, product):
        files= sorted(os.listdir(folder))
        duration= len(files)
        ts_dict= {}
        for i, file in enumerate(files):
            if file.endswith('.tif'):
                obj= GeoData(file, folder, product)
                timestamp= obj.getDateTime().strftime('%Y%m%d%H')
                data= obj.data
                ts_dict[timestamp]= data

        return ts_dict

    #TODO read as xarray
    def loadxr(self):
        pass

    @property
    def ts(self):
        return self._ts_data


class GeoData(object):
    '''
    This creates data object to facilitate geo-referenced data process
    The framework used to process .tif file is GDAL

    Methods:
    --------------------
    .getDateTime - return date time with sigle file's name
    .getGeoProjection - return projection
    .getGeoTransform - necessary if do affine transform
    .getDict - paired {date: data} per time stamp
    .visualize - plot time series along assigned pixel
    '''

    def __init__(self, src, folder, dType= 'satellite'):
        self.file_name= src
        self.type= dType
        self.obj= gdal.Open(os.path.join(folder,src))

    def getDateTime(self):
        if self.type== 'satellite':
            timestamp= self._satName()
        elif self.type== 'radar':
            timestamp= self._radName()
        elif self.type== 'gauge':
            timestamp= self._gauName()
        else:
            raise ValueError('expected type to be one of satellite, radar and gauge')

        return timestamp

    def getDict(self, key):
        timestamp= self.getDateTime()
        data= self.obj.ReadAsArray().astype(np.float32)
        paired= {'data': data,
                'datetime': self.getDateTime()}

        return paired

    @property
    def data(self):
        return self.obj.ReadAsArray()


    def _satName(self):
        # this function returns date and time from satellite file name
        date_parttern= r'(2017[0-1][0-9][0-3][0-9])'
        time_parttern= r'(S[0-2][0-9]0000)'
        date= re.search(date_parttern, self.file_name).group()
        time= re.search(time_parttern, self.file_name).group()[1:]

        return datetime.datetime.strptime(date+time, '%Y%m%d%H%M%S')

    def _radName(self):
        # this function returns date and time from satellite file name
        date_parttern= r'(2017[0-1][0-9][0-3][0-9])'
        time_parttern= r'(-[0-2][0-9]0000)'
        date= re.search(date_parttern, self.file_name).group()
        time= re.search(time_parttern, self.file_name).group()[1:]

        return datetime.datetime.strptime(date+time, '%Y%m%d%H%M%S')

    def _gauName(self):
        # this function returns date and time from satellite file name
        date_parttern= r'(2017[0-1][0-9][0-3][0-9][0-2][0-9])'
        date= re.search(date_parttern, self.file_name).group()[:8]
        time= re.search(date_parttern, self.file_name).group()[8:]
        # print(date, time)

        return datetime.datetime.strptime(date+time, '%Y%m%d%H')

