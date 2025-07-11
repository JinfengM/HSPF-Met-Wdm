# HSPF Meteorological Data Processing API Documentation

## Table of Contents

1. [Overview](#overview)
2. [Installation & Dependencies](#installation--dependencies)
3. [Main Processing Functions](#main-processing-functions)
4. [Daily Data Processing](#daily-data-processing)
5. [Hourly Data Processing](#hourly-data-processing)
6. [Utility Functions](#utility-functions)
7. [Calculation Algorithms](#calculation-algorithms)
8. [Data Storage Functions](#data-storage-functions)
9. [Missing Data Handling](#missing-data-handling)
10. [Complete Usage Examples](#complete-usage-examples)
11. [Error Handling](#error-handling)
12. [DSN (Data Set Number) Reference](#dsn-reference)

## Overview

This project provides comprehensive tools for processing meteorological data for HSPF (Hydrological Simulation Program - Fortran) models. It includes functions for data preprocessing, unit conversions, temporal disaggregation, and storage in WDM (Watershed Data Management) format.

### Key Features

- **Daily Data Processing**: Temperature, wind, cloud cover, solar radiation, evapotranspiration, evaporation
- **Hourly Data Disaggregation**: Converting daily data to hourly using scientifically-based algorithms
- **Unit Conversions**: Automatic conversion between metric and imperial units
- **Missing Data Handling**: Intelligent algorithms for filling missing values
- **WDM Integration**: Direct storage to WDM files with proper metadata
- **Parallel Processing**: Multi-core processing support for large datasets

## Installation & Dependencies

### Required Dependencies

```python
import pandas as pd
import numpy as np
import wdmtoolbox as wdm
from wdmtoolbox import wdmutil
from pandarallel import pandarallel
import warnings
import os
from typing import List, Dict, Union
```

### Setup

```python
from pandarallel import pandarallel
pandarallel.initialize(verbose=0)
```

## Main Processing Functions

### Core Import Statement

```python
from hspf_met import *
```

## Daily Data Processing

### 1. Maximum Temperature (`metTmax`)

Processes daily maximum temperature data and stores it in WDM format.

```python
def metTmax(inputfile: str, wdmpath: str, data_col: str, stations: List[str], 
           invalid_value: int, scale=0.1) -> pd.DataFrame
```

**Parameters:**
- `inputfile`: Path to input CSV file containing temperature data
- `wdmpath`: Path to output WDM file
- `data_col`: Column name containing temperature data
- `stations`: List of station IDs to process
- `invalid_value`: Value representing invalid/missing data
- `scale`: Scaling factor (default: 0.1)

**Returns:** DataFrame with processed temperature data

**Example:**
```python
stations = ['59843', '59848', '59851', '59854']
temp_df = metTmax(
    inputfile="data/temp.csv",
    wdmpath="output.wdm", 
    data_col='f9',
    stations=stations,
    invalid_value=32766,
    scale=0.1
)
```

**DSN Range:** 19, 39, 59, ... (19 + i*20)

### 2. Minimum Temperature (`metTmin`)

Processes daily minimum temperature data.

```python
def metTmin(inputfile: str, wdmpath: str, data_col: str, stations: List[str], 
           invalid_value: int, scale=0.1)
```

**Parameters:** Same as `metTmax`

**Example:**
```python
metTmin(
    inputfile="data/temp.csv",
    wdmpath="output.wdm",
    data_col='f10',
    stations=stations,
    invalid_value=32766
)
```

**DSN Range:** 20, 40, 60, ... (20 + i*20)

### 3. Daily Wind Speed (`metDailyWind`)

Processes daily wind speed data with unit conversion from m/s to wind travel distance.

```python
def metDailyWind(inputfile: str, wdmpath: str, data_col: str, stations: List[str],
                invalid_value: int, scale=0.1)
```

**Parameters:**
- `inputfile`: Path to wind speed data file
- `wdmpath`: Output WDM file path
- `data_col`: Wind speed column name
- `stations`: Station IDs
- `invalid_value`: Invalid data marker
- `scale`: Scaling factor

**Example:**
```python
metDailyWind(
    inputfile="data/wind.csv",
    wdmpath="output.wdm",
    data_col='f8',
    stations=stations,
    invalid_value=32766
)
```

**Unit Conversions:**
- Input: m/s × scale
- Converted to: mph (×2.23694)
- Final: Wind travel distance (mph × 24 hours)

**DSN Range:** 21, 41, 61, ... (21 + i*20)

### 4. Daily Cloud Cover (`metDailyCloud`)

Processes daily cloud cover data using sunshine duration.

```python
def metDailyCloud(inputfile: str, wdmpath: str, data_col: str, stations: List[str],
                 invalid_value: int, scale=0.1)
```

**Example:**
```python
metDailyCloud(
    inputfile="data/sunshine.csv",
    wdmpath="output.wdm",
    data_col='f8',
    stations=stations,
    invalid_value=32766
)
```

**DSN Range:** 22, 42, 62, ... (22 + i*20)

### 5. Daily Dewpoint Temperature (`metDailyDewpointTemperature`)

Calculates dewpoint temperature from air temperature and relative humidity using Magnus-Tetens formula.

```python
def metDailyDewpointTemperature(atem_file: str, atem_col: str, rhum_file: str, 
                               rhum_col: str, wdmpath: str, stations: List[str], 
                               invalid_value: int, scale=0.1)
```

**Parameters:**
- `atem_file`: Air temperature data file
- `atem_col`: Temperature column name
- `rhum_file`: Relative humidity data file  
- `rhum_col`: Humidity column name
- `wdmpath`: Output WDM file
- `stations`: Station IDs
- `invalid_value`: Invalid data marker
- `scale`: Temperature scaling factor

**Example:**
```python
metDailyDewpointTemperature(
    atem_file="data/temp.csv",
    atem_col='f8',
    rhum_file="data/humidity.csv",
    rhum_col='f8',
    wdmpath="output.wdm",
    stations=stations,
    invalid_value=32766,
    scale=0.1
)
```

**DSN Range:** 23, 43, 63, ... (23 + i*20)

### 6. Daily Solar Radiation (`metDailySolar`)

Processes daily solar radiation data with unit conversion from MJ/m² to Langleys.

```python
def metDailySolar(inputfile: str, wdmpath: str, data_col: str, stations: List[str], 
                 invalid_value: int, scale=0.01)
```

**Example:**
```python
metDailySolar(
    inputfile="data/radiation.csv",
    wdmpath="output.wdm",
    data_col='f8',
    stations=stations,
    invalid_value=999998,
    scale=0.01
)
```

**Unit Conversion:** 1 MJ/m² = 23.9 Langleys

**DSN Range:** 24, 44, 64, ... (24 + i*20)

### 7. Daily Evapotranspiration (`metDailyEvapotranspiration`)

Calculates potential evapotranspiration using Hamon method based on existing temperature data.

```python
def metDailyEvapotranspiration(wdmpath: str, stations: List[str], 
                              station_2_latDeg: Dict[str, float],
                              hMonCoeff: List[float] = None)
```

**Parameters:**
- `wdmpath`: WDM file path (must contain TMAX and TMIN data)
- `stations`: Station IDs
- `station_2_latDeg`: Dictionary mapping station IDs to latitude in degrees
- `hMonCoeff`: Hamon coefficients (optional, uses defaults if None)

**Example:**
```python
station_coords = {
    '59843': 19.7333, 
    '59848': 19.2333, 
    '59851': 19.7, 
    '59854': 19.3666
}

metDailyEvapotranspiration(
    wdmpath="output.wdm",
    stations=stations,
    station_2_latDeg=station_coords
)
```

**DSN Range:** 25, 45, 65, ... (25 + i*20)

### 8. Daily Evaporation (`metDailyEvaporation`)

Calculates pan evaporation using Penman equation requiring multiple meteorological variables.

```python
def metDailyEvaporation(wdmpath: str, stations: List[str])
```

**Required Data in WDM:**
- TMAX (maximum temperature)
- TMIN (minimum temperature)  
- DPTP (dewpoint temperature)
- DSOL (solar radiation)
- DWND (wind speed)

**Example:**
```python
metDailyEvaporation(
    wdmpath="output.wdm",
    stations=stations
)
```

**DSN Range:** 26, 46, 66, ... (26 + i*20)

## Hourly Data Processing

### 1. Hourly Precipitation (`metHourlyPREC`)

Disaggregates daily precipitation to hourly values using distribution methods.

```python
def metHourlyPREC(prec_file: str, wdmpath: str, stations: List[str], 
                 data_col: str, scale=0.01, method: str = "equal")
```

**Parameters:**
- `prec_file`: Daily precipitation data file
- `wdmpath`: Output WDM file
- `stations`: Station IDs
- `data_col`: Precipitation column name
- `scale`: Scaling factor (default: 0.01)
- `method`: Distribution method ("equal" or "triangular")

**Distribution Methods:**

**Equal Distribution:**
```python
# Uniform distribution - each hour gets 1/24 of daily total
metHourlyPREC(
    prec_file="data/precipitation.csv",
    wdmpath="output.wdm",
    stations=stations,
    data_col='f10',
    method="equal"
)
```

**Triangular Distribution:**
```python
# Triangular distribution - follows natural precipitation patterns
metHourlyPREC(
    prec_file="data/precipitation.csv", 
    wdmpath="output.wdm",
    stations=stations,
    data_col='f10',
    method="triangular"
)
```

**Unit Conversion:** mm → inches (×0.0393701)

**DSN Range:** 11, 31, 51, ... (11 + i*20)

### 2. Hourly Evaporation (`metHourlyEVAP`)

Disaggregates daily evaporation to hourly using solar radiation patterns.

```python
def metHourlyEVAP(wdmpath: str, stations: List[str], 
                 station_2_latDeg: Dict[str, float], tstype: str = 'DEVP')
```

**Parameters:**
- `wdmpath`: WDM file path (must contain daily evaporation data)
- `stations`: Station IDs
- `station_2_latDeg`: Station coordinates
- `tstype`: Time series type ('DEVP' for evaporation, 'DEVT' for evapotranspiration)

**Example:**
```python
metHourlyEVAP(
    wdmpath="output.wdm",
    stations=stations,
    station_2_latDeg=station_coords,
    tstype='DEVP'
)
```

**DSN Range:** 12, 32, 52, ... (12 + i*20)

### 3. Hourly Air Temperature (`metHourlyATEM`)

Disaggregates daily temperature using diurnal temperature patterns.

```python
def metHourlyATEM(wdmpath: str, stations: List[str], aObsTime: int)
```

**Parameters:**
- `wdmpath`: WDM file (must contain TMAX and TMIN)
- `stations`: Station IDs
- `aObsTime`: Observation time (24 = midnight, 6 = 6 AM, etc.)

**Example:**
```python
metHourlyATEM(
    wdmpath="output.wdm",
    stations=stations,
    aObsTime=24  # Midnight observation
)
```

**Algorithm:** Uses sinusoidal interpolation between daily min/max temperatures

**DSN Range:** 13, 33, 53, ... (13 + i*20)

### 4. Hourly Wind Speed (`metHourlyWIND`)

Disaggregates daily wind speed using diurnal wind patterns.

```python
def metHourlyWIND(wdmpath: str, stations: List[str], 
                 aDCurve: List[float] = None, tstype: str = 'DWND')
```

**Parameters:**
- `wdmpath`: WDM file path
- `stations`: Station IDs
- `aDCurve`: 24-hour diurnal curve coefficients (optional)
- `tstype`: Time series type

**Example:**
```python
# Using default diurnal curve
metHourlyWIND(
    wdmpath="output.wdm",
    stations=stations
)

# Using custom diurnal curve
custom_curve = [0.034, 0.034, 0.034, 0.034, 0.034, 0.034, 
                0.034, 0.035, 0.037, 0.041, 0.046, 0.05,
                0.053, 0.054, 0.058, 0.057, 0.056, 0.05,
                0.043, 0.04, 0.038, 0.035, 0.035, 0.034]

metHourlyWIND(
    wdmpath="output.wdm",
    stations=stations,
    aDCurve=custom_curve
)
```

**DSN Range:** 14, 34, 54, ... (14 + i*20)

### 5. Hourly Solar Radiation (`metHourlySOLR`)

Disaggregates daily solar radiation using astronomical calculations.

```python
def metHourlySOLR(wdmpath: str, stations: List[str], 
                 station_2_latDeg: Dict[str, float], tstype: str = 'DSOL')
```

**Example:**
```python
metHourlySOLR(
    wdmpath="output.wdm",
    stations=stations,
    station_2_latDeg=station_coords
)
```

**Algorithm:** Based on solar position calculations considering latitude and day of year

**DSN Range:** 15, 35, 55, ... (15 + i*20)

### 6. Hourly Potential Evapotranspiration (`metHourlyPEVT`)

Disaggregates daily potential evapotranspiration.

```python
def metHourlyPEVT(wdmpath: str, stations: List[str], 
                 station_2_latDeg: Dict[str, float], tstype: str = 'DEVT')
```

**Example:**
```python
metHourlyPEVT(
    wdmpath="output.wdm",
    stations=stations,
    station_2_latDeg=station_coords
)
```

**DSN Range:** 16, 36, 56, ... (16 + i*20)

### 7. Hourly Dewpoint Temperature (`metHourlyDEWP`)

Disaggregates daily dewpoint temperature (constant assumption).

```python
def metHourlyDEWP(wdmpath: str, stations: List[str], tstype: str = 'DPTP')
```

**Example:**
```python
metHourlyDEWP(
    wdmpath="output.wdm",
    stations=stations
)
```

**Algorithm:** Forward fill - assumes dewpoint remains constant throughout the day

**DSN Range:** 17, 37, 57, ... (17 + i*20)

### 8. Hourly Cloud Cover (`metHourlyCLOU`)

Disaggregates daily cloud cover (constant assumption).

```python
def metHourlyCLOU(wdmpath: str, stations: List[str], tstype: str = 'DCLO')
```

**Example:**
```python
metHourlyCLOU(
    wdmpath="output.wdm",
    stations=stations
)
```

**DSN Range:** 18, 38, 58, ... (18 + i*20)

## Utility Functions

### Unit Conversion Functions (`MetUtils.py`)

#### Temperature Conversion

```python
def celsius_to_fahrenheit(aInTS: pd.DataFrame, column: Union[int, str]) -> pd.DataFrame
```

**Example:**
```python
from MetUtils import celsius_to_fahrenheit

# Convert temperature data from Celsius to Fahrenheit
fahrenheit_data = celsius_to_fahrenheit(temp_df, 'temperature_column')
```

#### Solar Radiation Conversion

```python
def mjm2_to_Ly(aInTS: pd.DataFrame, column: Union[int, str]) -> pd.DataFrame
```

**Conversion:** 1 MJ/m² = 23.9 Langleys

**Example:**
```python
from MetUtils import mjm2_to_Ly

# Convert solar radiation from MJ/m² to Langleys
ly_data = mjm2_to_Ly(radiation_df, 'radiation_column')
```

#### Wind Speed Conversion

```python
def ms_to_mph(aInTS: pd.DataFrame, column: Union[int, str]) -> pd.DataFrame
```

**Conversion:** 1 m/s = 2.23694 mph

```python
def windTravelFromWindSpeed(aInTS: pd.DataFrame, column: Union[int, str]) -> pd.DataFrame
```

**Conversion:** Wind travel = wind speed (mph) × 24 hours

**Example:**
```python
from MetUtils import ms_to_mph, windTravelFromWindSpeed

# Convert wind speed and calculate daily wind travel
mph_data = ms_to_mph(wind_df, 'wind_speed')
wind_travel = windTravelFromWindSpeed(mph_data, 'wind_speed')
```

#### Data Validation

```python
def validate_data(aInTS: pd.DataFrame, column: Union[int, str]) -> str
```

**Purpose:** Validates DataFrame structure and column existence

**Example:**
```python
from MetUtils import validate_data

# Validate data before processing
column_name = validate_data(df, 'temperature')
```

#### WDM Utility Functions

```python
def checkStatios(checkstns: List[str], exist_stns_attr: List[object], tstype: str)
def get_stns_dsn(target_stns: List[str], exist_stns_attr: List[object], tstype: str) -> Dict
def read_dsn(wdmpath: str, dsn: int, tstype: str) -> pd.DataFrame
```

**Example:**
```python
from MetUtils import read_dsn
import wdmtoolbox as wdm
from wdmtoolbox import wdmutil

# Read data from WDM file
wdmts = wdmutil.WDM()
dsns = wdm.listdsns("output.wdm")
all_dsn_attrs = [wdmts.describe_dsn("output.wdm", dsn) for dsn in dsns]

# Check if stations exist
checkStatios(['59843', '59848'], all_dsn_attrs, 'TMAX')

# Get DSN mappings
tmax_dsns = get_stns_dsn(['59843', '59848'], all_dsn_attrs, 'TMAX')

# Read specific DSN data
tmax_data = read_dsn("output.wdm", 19, "TMAX")
```

#### Precipitation Special Values

```python
def prec_special_values(x) -> float
```

**Purpose:** Handles special precipitation codes used in meteorological data

**Special Codes:**
- 32766, 32744: Invalid data → NaN
- 32700: Trace precipitation → 0
- ≥3200: Coded values → subtract 32000
- ≥31000: Coded values → subtract 31000  
- ≥30000: Coded values → subtract 30000

## Calculation Algorithms

### Meteorological Calculations (`Metcalalg.py`)

#### Evapotranspiration Calculations

```python
def PanEvaporationValueComputedByHamon(aTMinTS: pd.DataFrame, aTMaxTS: pd.DataFrame, 
                                      aDegF: bool, aLatDeg: float, 
                                      aCTS: List[float] = None) -> pd.DataFrame
```

**Parameters:**
- `aTMinTS`: Minimum temperature DataFrame
- `aTMaxTS`: Maximum temperature DataFrame  
- `aDegF`: Temperature in Fahrenheit (True) or Celsius (False)
- `aLatDeg`: Latitude in degrees
- `aCTS`: Monthly coefficients (optional)

**Example:**
```python
from Metcalalg import PanEvaporationValueComputedByHamon

et_data = PanEvaporationValueComputedByHamon(
    aTMinTS=tmin_df,
    aTMaxTS=tmax_df,
    aDegF=True,  # Temperature in Fahrenheit
    aLatDeg=19.7333,
    aCTS=None  # Use default coefficients
)
```

#### Penman Evaporation

```python
def PanEvaporationValueComputedByPenman(aMinTmp: pd.DataFrame, aMaxTmp: pd.DataFrame,
                                       aDewTmp: pd.DataFrame, aWindSp: pd.DataFrame, 
                                       aSolRad: pd.DataFrame) -> pd.DataFrame
```

**Required Variables:**
- Minimum temperature
- Maximum temperature
- Dewpoint temperature
- Wind speed
- Solar radiation

**Example:**
```python
from Metcalalg import PanEvaporationValueComputedByPenman

penman_evap = PanEvaporationValueComputedByPenman(
    aMinTmp=tmin_df,
    aMaxTmp=tmax_df, 
    aDewTmp=dewpoint_df,
    aWindSp=wind_df,
    aSolRad=solar_df
)
```

#### Dewpoint Temperature Calculation

```python
def DewpointTemperatureByMagnusTetens(aAvgTmp: pd.DataFrame, temp_column: str,
                                     aRelHum: pd.DataFrame, rhu_column: str) -> pd.DataFrame
```

**Formula:** Magnus-Tetens equation for dewpoint calculation

**Example:**
```python
from Metcalalg import DewpointTemperatureByMagnusTetens

dewpoint = DewpointTemperatureByMagnusTetens(
    aAvgTmp=avg_temp_df,
    temp_column='temperature',
    aRelHum=humidity_df,
    rhu_column='humidity'
)
```

#### Cloud Cover from Sunshine

```python
def MetDataDailyCloudBySunshine(aInTS: pd.DataFrame, column_name: Union[int, str]) -> pd.DataFrame
```

**Purpose:** Estimates cloud cover from sunshine duration data

#### Temporal Disaggregation Functions

```python
def DisTemp(aMnTmpTS: pd.DataFrame, aMxTmpTS: pd.DataFrame, aObsTime: int) -> pd.DataFrame
def DisSolar(aDayRad: pd.DataFrame, aLatDeg: float) -> pd.DataFrame  
def DisPET(aDayPet: pd.DataFrame, column: str, aLatDeg: float) -> pd.DataFrame
def DisWnd(aInTs: pd.DataFrame, aDCurve: List[float] = None) -> pd.DataFrame
```

**Example:**
```python
from Metcalalg import DisTemp, DisSolar

# Disaggregate temperature
hourly_temp = DisTemp(tmin_df, tmax_df, aObsTime=24)

# Disaggregate solar radiation  
hourly_solar = DisSolar(daily_solar_df, aLatDeg=19.7333)
```

## Data Storage Functions

### WDM Storage (`MetSave.py`)

#### Generic Save Function

```python
def SaveDataToWdm(aDyTSer: pd.DataFrame, column_name: str, dsn: int, 
                 wdmpath: str, location: str, tcode: int,
                 scenario: str = 'OBSERVED', description: str = None)
```

**Parameters:**
- `aDyTSer`: Data to save
- `column_name`: Column name for WDM
- `dsn`: Data Set Number
- `wdmpath`: WDM file path
- `location`: Station identifier
- `tcode`: Time code (3=hourly, 4=daily)
- `scenario`: Data scenario ('OBSERVED' or 'COMPUTED')
- `description`: Data description

#### Daily Data Storage Functions

```python
# Temperature storage
def saveDailyTmax(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveDailyTmin(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)

# Meteorological data storage
def saveDailyWIND(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveDailyDCLO(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveDailyDPTP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveDailyDSOL(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveDailyDEVT(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveDailyDEVP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
```

#### Hourly Data Storage Functions

```python
def saveHourlyPREC(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlyEVAP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlyATEM(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlyWIND(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlySOLR(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlyPEVT(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlyDEWP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
def saveHourlyCLOU(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None)
```

**Example:**
```python
from MetSave import saveDailyTmax, saveHourlyPREC

# Save daily maximum temperature
saveDailyTmax(
    aDyTSer=tmax_df,
    wdmpath="output.wdm",
    location="59843",
    dsn=19
)

# Save hourly precipitation  
saveHourlyPREC(
    aDyTSer=hourly_precip_df,
    wdmpath="output.wdm",
    location="59843",
    dsn=11
)
```

## Missing Data Handling

### Missing Value Functions (`missingfill.py`)

#### Mean-Based Filling

```python
def fill_missing_values_bymean(df: pd.DataFrame, station_column: str = "f1",
                              data_col: str = 'f8', year_col: str = 'f5',
                              month_col: str = 'f6', day_col: str = 'f7',
                              stations: List[str] = None) -> pd.DataFrame
```

**Filling Rules:**
1. **Continuous missing values**: Backward fill
2. **Isolated missing values**: Mean of previous and next values

**Parameters:**
- `df`: Input DataFrame
- `station_column`: Column containing station IDs
- `data_col`: Column to fill missing values
- `year_col`, `month_col`, `day_col`: Date component columns
- `stations`: List of stations to process

**Example:**
```python
from missingfill import fill_missing_values_bymean

# Fill missing temperature data
filled_df = fill_missing_values_bymean(
    df=temp_df,
    station_column='f1',
    data_col='f8',
    stations=['59843', '59848', '59851', '59854']
)
```

#### Linear Interpolation Filling

```python
def fill_missing_values(df: pd.DataFrame, data_col: str = 'f8',
                       year_col: str = 'f5', month_col: str = 'f6', 
                       day_col: str = 'f7', method: str = 'linear') -> pd.DataFrame
```

**Filling Rules:**
1. **Continuous missing**: Backward fill
2. **Isolated missing**: Linear interpolation

**Example:**
```python
from missingfill import fill_missing_values

filled_df = fill_missing_values(
    df=data_df,
    data_col='f8',
    method='linear'
)
```

#### Simple Mean Fill

```python
def miss_fill_mean(obs_data: pd.DataFrame, station_column: str = "f1",
                  year_column: str = "f5", month_column: str = "f6",
                  day_column: str = "f7", data_column: str = "f8",
                  station_id: int = 59843, invalid_value: int = 32766) -> pd.DataFrame
```

**Purpose:** Simple filling for single station using adjacent day mean

## Complete Usage Examples

### Example 1: Complete Daily Processing Workflow

```python
from hspf_met import *

# Configuration
stations = ['59843', '59848', '59851', '59854']
station_coords = {
    '59843': 19.7333, 
    '59848': 19.2333, 
    '59851': 19.7, 
    '59854': 19.3666
}
invalid_value = 32766
wdmpath = "data/meteorological_data.wdm"

# Process all daily meteorological variables
print("Processing daily meteorological data...")

# 1. Temperature data
metTmax(
    inputfile="data/temperature.csv",
    wdmpath=wdmpath,
    data_col='f9',  # Max temperature column
    stations=stations,
    invalid_value=invalid_value
)

metTmin(
    inputfile="data/temperature.csv", 
    wdmpath=wdmpath,
    data_col='f10',  # Min temperature column
    stations=stations,
    invalid_value=invalid_value
)

# 2. Wind data
metDailyWind(
    inputfile="data/wind.csv",
    wdmpath=wdmpath,
    data_col='f8',
    stations=stations,
    invalid_value=invalid_value
)

# 3. Solar radiation
metDailySolar(
    inputfile="data/radiation.csv",
    wdmpath=wdmpath,
    data_col='f8',
    stations=stations,
    invalid_value=999998,
    scale=0.01
)

# 4. Cloud cover
metDailyCloud(
    inputfile="data/sunshine.csv",
    wdmpath=wdmpath,
    data_col='f8',
    stations=stations,
    invalid_value=invalid_value
)

# 5. Dewpoint temperature
metDailyDewpointTemperature(
    atem_file="data/temperature.csv",
    atem_col='f8',
    rhum_file="data/humidity.csv",
    rhum_col='f8',
    wdmpath=wdmpath,
    stations=stations,
    invalid_value=invalid_value
)

# 6. Evapotranspiration (calculated from temperature)
metDailyEvapotranspiration(
    wdmpath=wdmpath,
    stations=stations,
    station_2_latDeg=station_coords
)

# 7. Evaporation (calculated from multiple variables)
metDailyEvaporation(
    wdmpath=wdmpath,
    stations=stations
)

print("Daily data processing complete!")
```

### Example 2: Complete Hourly Processing Workflow

```python
# Process all hourly meteorological variables
print("Processing hourly meteorological data...")

# 1. Precipitation (with different distribution methods)
metHourlyPREC(
    prec_file="data/precipitation.csv",
    wdmpath=wdmpath,
    stations=stations,
    data_col='f10',
    scale=0.1,
    method="triangular"  # or "equal"
)

# 2. Evaporation
metHourlyEVAP(
    wdmpath=wdmpath,
    stations=stations,
    station_2_latDeg=station_coords,
    tstype='DEVP'
)

# 3. Air temperature
metHourlyATEM(
    wdmpath=wdmpath,
    stations=stations,
    aObsTime=24  # Midnight observation
)

# 4. Wind speed
metHourlyWIND(
    wdmpath=wdmpath,
    stations=stations
)

# 5. Solar radiation
metHourlySOLR(
    wdmpath=wdmpath,
    stations=stations,
    station_2_latDeg=station_coords
)

# 6. Potential evapotranspiration
metHourlyPEVT(
    wdmpath=wdmpath,
    stations=stations,
    station_2_latDeg=station_coords
)

# 7. Dewpoint temperature
metHourlyDEWP(
    wdmpath=wdmpath,
    stations=stations
)

# 8. Cloud cover
metHourlyCLOU(
    wdmpath=wdmpath,
    stations=stations
)

print("Hourly data processing complete!")
```

### Example 3: Data Verification and Analysis

```python
import wdmtoolbox as wdm
from wdmtoolbox import wdmutil

# Verify processed data
print("Verifying processed data...")

# List all DSNs in the WDM file
dsns = wdm.listdsns(wdmpath)
print(f"Total DSNs created: {len(dsns)}")
print(f"DSN list: {sorted(dsns)}")

# Get detailed information about each DSN
wdmts = wdmutil.WDM()
for dsn in sorted(dsns):
    attrs = wdmts.describe_dsn(wdmpath, dsn)
    print(f"DSN {dsn}: {attrs['TSTYPE']} - Station {attrs['IDLOCN']} - {attrs['TSSTEP']}")

# Read and analyze specific data
tmax_data = wdmts.read_dsn(wdmpath, 19)  # First station TMAX
print(f"TMAX data shape: {tmax_data.shape}")
print(f"Date range: {tmax_data.index.min()} to {tmax_data.index.max()}")
print(f"Statistics:\n{tmax_data.describe()}")
```

### Example 4: Error Handling and Data Quality

```python
try:
    # Process with error handling
    metTmax(
        inputfile="data/temperature.csv",
        wdmpath=wdmpath,
        data_col='f9',
        stations=stations,
        invalid_value=invalid_value
    )
    print("✅ Temperature processing successful")
    
except FileNotFoundError:
    print("❌ Input file not found")
except ValueError as e:
    print(f"❌ Data validation error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")

# Data quality checks
def check_data_quality(wdmpath: str, dsn: int, expected_records: int):
    """Check data quality after processing"""
    wdmts = wdmutil.WDM()
    data = wdmts.read_dsn(wdmpath, dsn)
    
    # Check for missing values
    missing_count = data.isnull().sum().iloc[0]
    if missing_count > 0:
        print(f"⚠️  DSN {dsn}: {missing_count} missing values found")
    
    # Check record count
    if len(data) != expected_records:
        print(f"⚠️  DSN {dsn}: Expected {expected_records} records, found {len(data)}")
    
    # Check for outliers (basic check)
    Q1 = data.quantile(0.25).iloc[0]
    Q3 = data.quantile(0.75).iloc[0]
    IQR = Q3 - Q1
    outliers = data[(data.iloc[:, 0] < (Q1 - 1.5 * IQR)) | 
                   (data.iloc[:, 0] > (Q3 + 1.5 * IQR))]
    
    if len(outliers) > 0:
        print(f"⚠️  DSN {dsn}: {len(outliers)} potential outliers found")
    
    return {
        'missing_values': missing_count,
        'total_records': len(data),
        'outliers': len(outliers)
    }

# Check quality for all temperature DSNs
for i, station in enumerate(stations):
    dsn = 19 + i * 20  # TMAX DSN
    quality = check_data_quality(wdmpath, dsn, expected_records=365)
    print(f"Station {station} TMAX quality: {quality}")
```

## Error Handling

### Common Error Types

#### 1. File Not Found Errors
```python
try:
    metTmax(inputfile="missing_file.csv", ...)
except FileNotFoundError:
    print("Input file does not exist")
```

#### 2. Data Validation Errors
```python
try:
    celsius_to_fahrenheit(df, 'nonexistent_column')
except ValueError as e:
    print(f"Column validation failed: {e}")
```

#### 3. WDM File Errors
```python
try:
    SaveDataToWdm(data, ...)
except Exception as e:
    print(f"WDM save failed: {e}")
```

#### 4. Latitude Range Errors
```python
try:
    PanEvaporationValueComputedByHamon(tmin, tmax, True, 100.0)  # Invalid latitude
except ValueError as e:
    print(f"Latitude out of range: {e}")
```

### Best Practices

1. **Always validate input data:**
```python
if not isinstance(df.index, pd.DatetimeIndex):
    raise TypeError("Index must be DatetimeIndex")
```

2. **Check for required columns:**
```python
required_columns = ['f1', 'f8', 'time']
missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")
```

3. **Handle missing data appropriately:**
```python
# Check for excessive missing data
missing_pct = df[data_col].isnull().sum() / len(df) * 100
if missing_pct > 50:
    warnings.warn(f"High percentage of missing data: {missing_pct:.1f}%")
```

## DSN (Data Set Number) Reference

### DSN Allocation Pattern

DSNs are allocated in blocks of 20 for each station:
- Station 1: 11-30
- Station 2: 31-50  
- Station 3: 51-70
- Station 4: 71-90
- etc.

### Daily Data DSNs (Time Code = 4)

| Variable | DSN Formula | Description | Units |
|----------|-------------|-------------|-------|
| TMAX | 19 + i*20 | Daily maximum temperature | °F |
| TMIN | 20 + i*20 | Daily minimum temperature | °F |
| DWND | 21 + i*20 | Daily wind travel | miles |
| DCLO | 22 + i*20 | Daily cloud cover | tenths |
| DPTP | 23 + i*20 | Daily dewpoint temperature | °F |
| DSOL | 24 + i*20 | Daily solar radiation | Langleys |
| DEVT | 25 + i*20 | Daily evapotranspiration | inches |
| DEVP | 26 + i*20 | Daily evaporation | inches |

### Hourly Data DSNs (Time Code = 3)

| Variable | DSN Formula | Description | Units |
|----------|-------------|-------------|-------|
| PREC | 11 + i*20 | Hourly precipitation | inches |
| EVAP | 12 + i*20 | Hourly evaporation | inches |
| ATEM | 13 + i*20 | Hourly air temperature | °F |
| WIND | 14 + i*20 | Hourly wind speed | mph |
| SOLR | 15 + i*20 | Hourly solar radiation | Langleys |
| PEVT | 16 + i*20 | Hourly potential evapotranspiration | inches |
| DEWP | 17 + i*20 | Hourly dewpoint temperature | °F |
| CLOU | 18 + i*20 | Hourly cloud cover | tenths |

### Example DSN Calculation

For 4 stations ['59843', '59848', '59851', '59854']:

**Station 59843 (i=0):**
- TMAX: DSN 19, TMIN: DSN 20, PREC: DSN 11, EVAP: DSN 12

**Station 59848 (i=1):**  
- TMAX: DSN 39, TMIN: DSN 40, PREC: DSN 31, EVAP: DSN 32

**Station 59851 (i=2):**
- TMAX: DSN 59, TMIN: DSN 60, PREC: DSN 51, EVAP: DSN 52

**Station 59854 (i=3):**
- TMAX: DSN 79, TMIN: DSN 80, PREC: DSN 71, EVAP: DSN 72

---

## Conclusion

This comprehensive API provides all necessary tools for processing meteorological data for HSPF models. The functions handle data validation, unit conversions, temporal disaggregation, and proper storage in WDM format while maintaining scientific accuracy and computational efficiency.

For additional support or custom implementations, refer to the individual module documentation and example usage patterns provided throughout this guide.