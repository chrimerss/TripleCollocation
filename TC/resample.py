'''
In this module, we are gonna adjust the resolution of three products and resample them

'''

from osgeo import gdal, osr
import numpy as np
import matplotlib.pyplot as plt
import os
import affine
from multiprocessing import Pool
from PIL import Image

__author__='allen'
__version__= 0.1

#==define some global variable==#
num_rows= 209
num_cols= 293
width= (101.0-88.05)/(num_cols)
height= (35-25.8)/(num_rows)
begX= -101.0
begY= 25.8
rot1= 0
rot2= 0
geo_trans= (begX, width, rot1, begY, rot2, height)

def main(num_process=3):
    '''
    This function controls the main program as it splits tasks by three cores

    Inputs:
    --------------
    :num_process - int; number of cores you want to use; by default 3

    Outputs:
    --------------
    None, but write .tif files
    '''
    gauge_folder= 'gauge4km'
    sat_folder= 'GPMrt1H4kmw'
    radar_folder= 'mrmsrt1H4kmw'
    folders= [gauge_folder, sat_folder, radar_folder]
    pool= Pool(3)
    pool.map(process, folders)
    # process(radar_folder)


def process(folder):
    '''
    This function controls single process

    Inputs:
    ------------------
    :folder - str; the folder data was in

    Outputs:
    ------------------
    None
    '''
    files= sorted(os.listdir(os.path.join('..','rainfall_analysis', folder)))
    for i, each in enumerate(files):
        if each.endswith('.tif'):
            each_pth= os.path.join('..','rainfall_analysis', folder, each)
            dst= os.path.join('..','cleaned',folder, each)
            resample(each_pth, dst)

def resample(file, dst):
    '''
    This function controls single file procedure

    Inputs:
    -----------------
    :file - str; fine name want to process
    :dst - str; destination want to store your processed data

    Outputs:
    -----------------
    None
    '''
    #construct a new array with shape (210,294)

    # src= gdal.Open(file)
    # new_arr= get_array(src)
    # write(new_arr, src, dst)
    cmd= 'gdalwarp -te -101.0 25.8 -88.05 35 %s %s'%(file, dst)
    os.system(cmd)

def projection(src):
    # This function get geo projection from data source
    return src.GetProjection()

def getGeoTransform(src):
    # This function get geo transformation information from data source
    return src.GetGeoTransform()

def initial_estimate(data_source):
    '''
    This function is to initially estimate the column number from one data source

    Inputs:
    -----------------
    :data_source - osgeo.gdal Object;

    Outputs:
    -----------------
    :py - column number
    '''
    global width, rot1, begX, rot2, height, begY
    xoffset, px_w, rot1, yoffset, rot2 , px_h = getGeoTransform(data_source)
    forward_transform =  \
        affine.Affine.from_gdal(*data_source.GetGeoTransform())
    reverse_transform = ~forward_transform
    px, py = reverse_transform * (begX, begY)
    px, py = int(px + 0.5), int(py + 0.5)

    return py

def get_value(src, row, col):
    # this function returns the value given row and column
    return src.ReadAsArray()[row, col]

def get_array(src):
    '''
    This funciton generates new array that satisfy your requirement.
    Notice that we read all global variables to produce the new array.
    Please modify these variables outside

    Inputs:
    ----------------
    :src - osgeo.gdal Object;

    Outputs:
    ----------------
    new_arr: numpy.array Object; with number of rows and columns you specified
    '''
    global width, rot1, begX, rot2, height, begY, num_cols, num_rows
    first= True
    new_arr= np.zeros((num_rows,num_cols), dtype=np.float64)
    for i in range(num_rows):
        if first:
            prev_col=initial_estimate(src)-1
            first= False
        _temp= 0
        for j in range(num_cols):
            x= j*width+begX
            y= height*i+begY
            val, row, col= retrieve_pixel_value(src, (x, y))
            # print(i,j,row,col)
            new_arr[i,j]= val
            if col-prev_col!=1 and j!=0:
                _temp= get_value(src, row, prev_col)
                if _temp>val: new_arr[i,j]=_temp
                prev_col= col
#                 print(prev_col, col)
            else:
                prev_col= col

    return new_arr


def retrieve_pixel_value(data_source, geo_coord):
    """Return floating-point value that corresponds to given point."""
    x, y = geo_coord[0], geo_coord[1]
    forward_transform =  \
        affine.Affine.from_gdal(*data_source.GetGeoTransform())
    reverse_transform = ~forward_transform
    col, row = reverse_transform * (x, y)
    col, row = int(col), int(row)
    pixel_coord = row, col

    data_array = np.array(data_source.GetRasterBand(1).ReadAsArray())
    return data_array[pixel_coord[0]][pixel_coord[1]], row, col

def write(new_arr,old_obj, dst):
    # This function writes new array to the envrionment you want
    global geo_trans
    rows, cols= new_arr.shape
    driver= gdal.GetDriverByName("GTiff")
    outdata = driver.Create(dst, cols, rows, 1, gdal.GDT_Float32)
    crs= old_obj.GetProjection()
    outdata.SetGeoTransform(geo_trans)
    outdata.SetProjection(crs)
    outdata.GetRasterBand(1).WriteArray(new_arr)

if __name__=='__main__':
    main()
