# TripleCollocation

This is the python implementation of Triple Collocation method.

## Introduction
The idea of using TC method in precipitation estimation is because under severe climate e.g. hurricane, turnado, None of the morden techniques of estimating rainfall (satellite, radar, gauge) is recognized as ground truth. Thus, without leveraging any one of these, we actually propose a error model that take advantage of these three methods.

The big assumption of utilizing TC method is: 1) three data source must be independent. 2) The expectation of error due to three techniques is 0. 3)The errors for different products are independent.

The MTC method can be formularized as follow,

$$R_i=A_iT^{\beta_i}E_i$$

The following steps are rearanging these terms by calculating its corvariance, and coorelation coefficient.

### NCEP vs. gauge raw data

spatial correlation map goes here...

## How to use

## Results

### NCEP + msmr + IMERG

The first combination is from NCEP gauge data, msmr radar data and IMERG satellite data. NCEP data are derived from raw data by reducing the covariance. msmr is the result calibrated by multiple radar network, and IMERG final run data without calibrated. So these three products are independent because they come from different sources. 

Image goes here...

### Kriging gauge+ msmr + IMERG
