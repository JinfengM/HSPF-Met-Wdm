import math
from typing import Union

import numpy as np
import pandas as pd
import warnings
import missingfill as msfl
import wdmtoolbox as wdm
from wdmtoolbox import wdmutil
from MetUtils import *
from pandarallel import pandarallel
pandarallel.initialize(verbose=0)
"""
逐日分解
1. 温度分解：DisTemp
2. 辐射分解：DisSolar
3. 风速分解：DisWnd
4. 云量分解：恒定假设，直接使用重采样，向前填充
5. 露点温度：恒定假设，直接使用重采样，向前填充
6,7. 潜在蒸散或蒸发：DisPET
8. 降雨分解：
"""
"""
计算逐日
1. PET: PanEvaporationValueComputedByHamon
2. ET: PanEvaporationValueComputedByPenman

"""

DegreesToRadians = 0.01745329252
MetComputeLatitudeMax = 66.5
MetComputeLatitudeMin = -66.5
X1 = [0, 10.00028, 41.0003, 69.22113, 100.5259, 130.8852,
      161.2853, 191.7178, 222.1775, 253.66, 281.1629, 309.6838, 341.221]
c = [
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 4.0, 2.0, -1.5, -3.0, -2.0, 1.0, 3.0, 2.5, 1.0, 1.0, 2.0, 1.0],
    [0, 3.0, 4.0, 0.0, -3.0, -2.5, 0.0, 2.0, 3.0, 2.0, 1.5, 2.0, 1.0],
    [0, 0.0, 3.5, 1.5, -1.0, -2.0, -1.0, 1.5, 3.0, 3.0, 1.5, 2.0, 1.0],
    [0, -2.0, 2.5, 3.5, 0.0, -2.0, -1.0, 0.5, 3.0, 3.0, 2.0, 2.0, 1.0],
    [0, -4.0, 0.5, 3.0, 1.0, -0.5, -1.0, 0.0, 2.0, 2.5, 2.5, 2.0, 1.0],
    [0, -5.0, -1.5, 2.0, 3.0, 0.5, -1.0, -0.5, 1.0, 2.5, 2.5, 2.0, 1.0],
    [0, -5.0, -3.5, 1.0, 3.0, 1.5, 0.0, -0.5, 1.0, 2.0, 2.0, 2.0, 1.0],
    [0, -4.0, -4.5, -1.0, 2.5, 3.0, 1.0, 0.0, 0.0, 1.5, 2.0, 2.0, 1.0],
    [0, -2.0, -4.0, -3.0, 1.0, 3.0, 2.0, 0.5, 0.0, 1.5, 2.0, 1.0, 1.0],
    [0, 0.0, -3.5, -4.0, -0.5, 3.0, 3.0, 1.5, 1.0, 1.0, 2.0, 1.0, 1.0]
]
XLax = [
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, -9, -9, -9, -9, -9, -9],
    [-9, 616.17, -147.83, -27.17, -3.17, 11.84, 2.02],
    [-9, 609.97, -154.71, -27.49, -2.97, 12.04, 1.3],
    [-9, 603.69, -161.55, -27.69, -2.78, 12.22, 0.64],
    [-9, 597.29, -168.33, -27.78, -2.6, 12.38, 0.02],
    [-9, 590.81, -175.05, -27.74, -2.43, 12.53, -0.56],
    [-9, 584.21, -181.72, -27.57, -2.28, 12.67, -1.1],
    [-9, 577.53, -188.34, -27.29, -2.14, 12.8, -1.6],
    [-9, 570.73, -194.91, -26.89, -2.02, 12.92, -2.05],
    [-9, 563.85, -201.42, -26.37, -1.91, 13.03, -2.45],
    [-9, 556.85, -207.29, -25.72, -1.81, 13.13, -2.8],
    [-9, 549.77, -214.29, -24.96, -1.72, 13.22, -3.1],
    [-9, 542.57, -220.65, -24.07, -1.64, 13.3, -3.35],
    [-9, 535.3, -226.96, -23.07, -1.59, 13.36, -3.58],
    [-9, 527.9, -233.22, -21.95, -1.55, 13.4, -3.77],
    [-9, 520.44, -239.43, -20.7, -1.52, 13.42, -3.92],
    [-9, 512.84, -245.59, -19.33, -1.51, 13.42, -4.03],
    [-9, 505.19, -251.69, -17.83, -1.51, 13.41, -4.1],
    [-9, 497.4, -257.74, -16.22, -1.52, 13.39, -4.13],
    [-9, 489.52, -263.74, -14.49, -1.54, 13.36, -4.12],
    [-9, 481.53, -269.7, -12.63, -1.57, 13.32, -4.07],
    [-9, 473.45, -275.6, -10.65, -1.63, 13.27, -3.98],
    [-9, 465.27, -281.45, -8.55, -1.71, 13.21, -3.85],
    [-9, 456.99, -287.25, -6.33, -1.8, 13.14, -3.68],
    [-9, 448.61, -292.99, -3.98, -1.9, 13.07, -3.47],
    [-9, 440.14, -298.68, -1.51, -2.01, 13.0, -3.3],
    [-9, 431.55, -304.32, 1.08, -2.13, 12.92, -3.17],
    [-9, 431.55, -304.32, 1.08, -2.13, 12.92, -3.17]]
aDCurve_Default = [0.034, 0.034, 0.034, 0.034, 0.034, 0.034, 0.034, 0.035, 0.037, 0.041, 0.046, 0.05, 0.053, 0.054, 0.058, 0.057, 0.056, 0.05, 0.043, 0.04, 0.038, 0.035, 0.035, 0.034]
Triang = [
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.01, 0.01],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0.01, 0.01, 0.1, 0.11],
    [0, 0, 0, 0, 0, 0, 0, 0.01, 0.01, 0.08, 0.09, 0.45, 0.55],
    [0, 0, 0, 0, 0, 0.01, 0.01, 0.06, 0.07, 0.28, 0.36, 1.2, 1.65],
    [0, 0, 0, 0.01, 0.01, 0.04, 0.05, 0.15, 0.21, 0.56, 0.84, 2.1, 3.3],
    [0, 0.01, 0.01, 0.02, 0.03, 0.06, 0.1, 0.2, 0.35, 0.7, 1.26, 2.52, 4.62],
    [0, 0, 0.01, 0.01, 0.03, 0.04, 0.1, 0.15, 0.35, 0.56, 1.26, 2.1, 4.62],
    [0, 0, 0, 0, 0.01, 0.01, 0.05, 0.06, 0.21, 0.28, 0.84, 1.2, 3.3],
    [0, 0, 0, 0, 0, 0, 0.01, 0.01, 0.07, 0.08, 0.36, 0.45, 1.65],
    [0, 0, 0, 0, 0, 0, 0, 0, 0.01, 0.01, 0.09, 0.1, 0.55],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.01, 0.01, 0.11],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.01],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]
Sums = [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12, 10.24, 20.48]
defHMonCoeff = np.array([0, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055, 0.0055])


def CloudCoverTimeseriesFromSolar(solar_data: pd.DataFrame, aDegLat):
    """
    根据辐射数据计算云量，辐射数据的索引为时间
    :param solar_data: 辐射数据
    :param aDegLat: 维度
    :return:
    """
    lLatInt = int(np.floor(aDegLat))
    lLatFrac = aDegLat - lLatInt
    lLatFrac = 0.0 if lLatFrac <= 0.0001 else lLatFrac

    A0 = XLax[lLatInt][1] + lLatFrac * (XLax[lLatInt + 1][1] - XLax[lLatInt][1])
    A1 = XLax[lLatInt][2] + lLatFrac * (XLax[lLatInt + 1][2] - XLax[lLatInt][2])
    A2 = XLax[lLatInt][3] + lLatFrac * (XLax[lLatInt + 1][3] - XLax[lLatInt][3])
    A3 = XLax[lLatInt][4] + lLatFrac * (XLax[lLatInt + 1][4] - XLax[lLatInt][4])
    b1 = XLax[lLatInt][5] + lLatFrac * (XLax[lLatInt + 1][5] - XLax[lLatInt][5])
    b2 = XLax[lLatInt][6] + lLatFrac * (XLax[lLatInt + 1][6] - XLax[lLatInt][6])

    a = aDegLat - 25.0
    b = aDegLat - 44.0

    Exp1 = 0.7575 - 0.0018 * a
    Exp2 = 0.725 + 0.00288 * b
    Lat1 = 2.139 + 0.0423 * a
    Lat2 = 30.0 - 0.667 * a
    Lat3 = 2.9 - 0.0629 * b
    Lat4 = 18.0 + 0.833 * b

    # Percent sunshine
    SS = 100 * (1 - solar_data / 10) ** (5 / 3)
    SS = SS.clip(lower=0)

    # convert to radians
    x = np.array(list(map(lambda i: X1[i[0]] + i[1], zip(solar_data.index.month - 1, solar_data.index.day))))
    x *= DegreesToRadians

    Y100 = (A0
            + A1 * math.cos(x)
            + A2 * math.cos(2.0 * x)
            + A3 * math.cos(3.0 * x)
            + b1 * math.sin(x)
            + b2 * math.sin(2.0 * x))

    ii = np.ceil((SS + 10) / 10)

    if aDegLat > 43.0:
        YRD = Lat3 * SS ** Exp2 + Lat4
    else:
        YRD = Lat1 * SS ** Exp1 + Lat2
    c11 = np.array(list(map(lambda i: c[i[0]][i[1]], zip(ii[ii < 11], solar_data.index.month[ii < 11] - 1))))
    YRD[ii < 11] = YRD[ii < 11] + c11
    YRD[YRD < 100] = Y100[YRD < 100] * YRD[YRD < 100] / 100
    return YRD


def DisTemp(aMnTmpTS: pd.DataFrame, aMxTmpTS: pd.DataFrame, aObsTime):
    """
    根据逐日的最小最大温度和观测时间分解到逐小时的温度，最小温度和最大温度索引都是逐日的时间。
    :param aMnTmpTS: 最小温度
    :param aMxTmpTS: 最大温度
    :param aObsTime: 观察时间
    :return:
    """
    if aObsTime < 6:
        lCurMin = aMnTmpTS.shift(periods=-1)
        lNxtMin = aMnTmpTS.shift(periods=-2)
        lCurMin.ffill(inplace=True)
        lNxtMin.ffill(inplace=True)
    else:
        lCurMin = aMnTmpTS
        lNxtMin = aMnTmpTS.shift(periods=-1)
        lNxtMin.ffill(inplace=True)
    if aObsTime > 16:
        lCurMax = aMxTmpTS
        lPreMax = aMxTmpTS.shift(periods=1)
        lPreMax.bfill(inplace=True)
    else:
        lPreMax = aMxTmpTS
        lCurMax = aMxTmpTS.shift(periods=-1)
        lCurMax.ffill(inplace=True)

    lDif1 = lPreMax['TMAX'] - lCurMin['TMIN']
    lDif2 = lCurMin['TMIN'] - lCurMax['TMAX']
    lDif3 = lCurMax['TMAX'] - lNxtMin['TMIN']

    enddate = aMnTmpTS.index.max() + pd.Timedelta(days=1)
    lHRTemp = pd.DataFrame(index=pd.date_range(aMnTmpTS.index.min(), enddate, freq="h", inclusive="left"),
                           columns=["ATEM"])

    lHRTemp.iloc[1::24, 0] = lCurMin['TMIN'] + lDif1 * 0.15
    lHRTemp.iloc[2::24, 0] = lCurMin['TMIN'] + lDif1 * 0.1
    lHRTemp.iloc[3::24, 0] = lCurMin['TMIN'] + lDif1 * 0.06
    lHRTemp.iloc[4::24, 0] = lCurMin['TMIN'] + lDif1 * 0.03
    lHRTemp.iloc[5::24, 0] = lCurMin['TMIN'] + lDif1 * 0.01
    lHRTemp.iloc[6::24, 0] = lCurMin['TMIN']
    lHRTemp.iloc[7::24, 0] = lCurMin['TMIN'] - lDif2 * 0.16
    lHRTemp.iloc[8::24, 0] = lCurMin['TMIN'] - lDif2 * 0.31
    lHRTemp.iloc[9::24, 0] = lCurMin['TMIN'] - lDif2 * 0.45
    lHRTemp.iloc[10::24, 0] = lCurMin['TMIN'] - lDif2 * 0.59
    lHRTemp.iloc[11::24, 0] = lCurMin['TMIN'] - lDif2 * 0.71
    lHRTemp.iloc[12::24, 0] = lCurMin['TMIN'] - lDif2 * 0.81
    lHRTemp.iloc[13::24, 0] = lCurMin['TMIN'] - lDif2 * 0.89
    lHRTemp.iloc[14::24, 0] = lCurMin['TMIN'] - lDif2 * 0.95
    lHRTemp.iloc[15::24, 0] = lCurMin['TMIN'] - lDif2 * 0.99
    lHRTemp.iloc[16::24, 0] = lCurMax['TMAX']
    lHRTemp.iloc[17::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.89
    lHRTemp.iloc[18::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.78
    lHRTemp.iloc[19::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.67
    lHRTemp.iloc[20::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.57
    lHRTemp.iloc[21::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.47
    lHRTemp.iloc[22::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.38
    lHRTemp.iloc[23::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.29
    lHRTemp.iloc[0::24, 0] = lNxtMin['TMIN'] + lDif3 * 0.22
    return lHRTemp


def DisSolar(aDayRad: pd.DataFrame, aLatDeg: float):
    """
    Disaggregate daily SOLAR or PET to hourly
    :param aInTs: input timeseries to be disaggregated
    :param aLatDeg: Latitude, in degrees
    :return:
    """
    # check latitude
    if MetComputeLatitudeMin > aLatDeg or aLatDeg > MetComputeLatitudeMax:
        raise ValueError(f'Latitude must be between {MetComputeLatitudeMin} and {MetComputeLatitudeMax}')
    # convert to radians
    LatRdn = aLatDeg * DegreesToRadians
    Phi = LatRdn

    def cal_dis(row):
        # julian date
        rad_datetime = row['time']
        # pandas中时间类可以直接获取年中的第几天
        # JulDay = row['time'].dayofyear
        #BASINS中
        JulDay = 30.5 * (row['time'].month - 1) + row['time'].day
        AD = 0.40928 * np.cos(0.0172141 * (172.0 - JulDay))
        SS = np.sin(Phi) * np.sin(AD)
        CS = np.cos(Phi) * np.cos(AD)
        X2 = -SS / CS
        Delt = 7.6394 * (1.5708 - np.atan(X2 / np.sqrt(1.0 - X2 ** 2)))
        SunR = 12.0 - Delt / 2.0
        # develop hourly distribution given sunrise,sunset and length of day (DELT)
        DTR2 = Delt / 2.0
        DTR4 = Delt / 4.0
        CRAD = 0.66666667 / DTR2
        SL = CRAD / DTR4
        TRise = SunR
        TR2 = TRise + DTR4
        TR3 = TR2 + DTR2
        TR4 = TR3 + DTR4
        RK = rad_datetime.hour
        if RK > TRise:
            if RK > TR2:
                if RK > TR3:
                    if RK > TR4:
                        return 0.0
                    else:
                        return (CRAD - (RK - TR3) * SL) * row['DSOL']
                else:
                    return CRAD * row['DSOL']
            else:
                return (RK - TRise) * SL * row['DSOL']
        else:
            return 0.0

    aHrRad = aDayRad.resample("h").ffill()
    aHrRad['time'] = aHrRad.index.to_series()
    # aHrRad['SOLR'] = aHrRad.apply(cal_dis, axis=1)
    # parallel_apply
    aHrRad['SOLR'] = aHrRad.parallel_apply(cal_dis, axis=1)
    target_data = aHrRad['SOLR'].to_frame()
    target_data = target_data.groupby(target_data.index.day).shift(periods=-1, fill_value=0)
    return target_data


def DisPET(aDayPet: pd.DataFrame, column: str, aLatDeg):
    """
    Distributes daily PET to hourly values,based on a method used to disaggregate solar radiation
    in HSP (Hydrocomp, 1976) using latitude, month, day,and daily PET.
    :param aDayPet: input daily PET (inches)
    :param aLatDeg: latitude(degrees)
    :return:
    """
    # check latitude
    if MetComputeLatitudeMin > aLatDeg or aLatDeg > MetComputeLatitudeMax:
        raise ValueError(f'Latitude must be between {MetComputeLatitudeMin} and {MetComputeLatitudeMax}')
    # convert to radians
    LatRdn = aLatDeg * DegreesToRadians
    Phi = LatRdn
    if aDayPet.columns[0] not in ['DEVP', 'DEVT']:
        raise ValueError(f'{aDayPet.columns[0]} must be DEVP or DEVT')

    def cal_dis(row):
        # julian date
        rad_datetime = row['time']
        JulDay = row['time'].dayofyear
        # BASINS中
        # JulDay = 30.5 * (row['time'].month - 1) + row['time'].day
        AD = 0.40928 * np.cos(0.0172141 * (172.0 - JulDay))
        SS = np.sin(Phi) * np.sin(AD)
        CS = np.cos(Phi) * np.cos(AD)
        X2 = -SS / CS
        Delt = 7.6394 * (1.5708 - np.atan(X2 / np.sqrt(1.0 - X2 ** 2)))
        SunR = 12.0 - Delt / 2.0
        # develop hourly distribution given sunrise,sunset and length of day (DELT)
        DTR2 = Delt / 2.0
        DTR4 = Delt / 4.0
        CRAD = 0.66666667 / DTR2
        SL = CRAD / DTR4
        TRise = SunR
        TR2 = TRise + DTR4
        TR3 = TR2 + DTR2
        TR4 = TR3 + DTR4
        RK = rad_datetime.hour+1
        # calculate hourly distribution curve
        if RK > TRise:
            if RK > TR2:
                if RK > TR3:
                    if RK > TR4:
                        aHrPet_value = 0.0
                    else:
                        aHrPet_value = (CRAD - (RK - TR3) * SL) * row[column]
                else:
                    aHrPet_value = CRAD * row[column]
            else:
                aHrPet_value = (RK - TRise) * SL * row[column]
        else:
            aHrPet_value = 0.0
        if pd.notna(aHrPet_value) and aHrPet_value > 40:
            warnings.warn(f"Bad Hourly Value {aHrPet_value}")
        return aHrPet_value

    aHrPet = aDayPet.resample("h").ffill()
    aHrPet['time'] = aHrPet.index.to_series()
    output_column = 'PEVT' if column == 'DEVT' else 'EVAP'
    # aHrPet[output_column] = aHrPet.apply(cal_dis, axis=1)
    # parallel_apply
    aHrPet[output_column] = aHrPet.parallel_apply(cal_dis, axis=1)
    target_data = aHrPet[output_column].to_frame()
    # target_data = target_data.groupby(target_data.index.day).shift(periods=-1, fill_value=0)
    return target_data


def DisWnd(aInTs: pd.DataFrame, aDCurve:List[float] = None):
    """
    Disaggregate daily wind to hourly
    :param aInTs: input daily wind timeseries
    :param aDCurve: hourly diurnal curve for wind disaggregation
    :return:
    """
    if aDCurve is None:
        aDCurve = aDCurve_Default
    aHrWind = aInTs.resample("h").ffill()
    #Distribution coefficient 获取分布系数
    aHrWind['dist_coef'] = np.array([aDCurve[h] for h in aHrWind.index.hour])
    aHrWind['WIND'] = aHrWind['DWND'] * aHrWind['dist_coef']
    return aHrWind['WIND'].to_frame()


def DistTriang(aDyTSer: pd.DataFrame):
    def disa(aDaySum):
        i = 0
        while aDaySum > Sums[i]:
            i += 1
            if i > 11:
                warnings.warn("DaySum too big")
        lRndOff = 0.001
        lCarry = 0
        lRatio = aDaySum / Sums[i]
        lDaySum = 0
        aHrVals = [0] * 24
        for j in range(0, 24):
            aHrVals[j] = lRatio * Triang[j][i] + lCarry
            if aHrVals[j] > 0.00001:
                lCarry = aHrVals[j] - (np.round(aHrVals[j] / lRndOff) * lRndOff)
                aHrVals[j] = aHrVals[j] - lCarry
            else:
                aHrVals[j] = 0
            lDaySum += aHrVals[j]
        if lCarry > 0.00001:
            lDaySum = lDaySum - aHrVals[11]
            aHrVals[11] = aHrVals[11] + lCarry
            lDaySum = lDaySum + aHrVals[11]
        if np.abs(aDaySum - lDaySum) > lRndOff:
            warnings.warn("values not distributed properly")
        return aHrVals

    aDyTSer['hourly_value'] = aDyTSer.iloc[:, 0].map(disa)
    aHrTSer = aDyTSer.resample("h").ffill()
    aHrTSer['hour'] = aHrTSer.index.hour
    # aHrTSer['PREC'] = aHrTSer.apply(lambda x: x['hourly_value'][x['hour']], axis=1)
    # parallel_apply
    aHrTSer['PREC'] = aHrTSer.parallel_apply(lambda x: x['hourly_value'][x['hour']], axis=1)
    return aHrTSer['PREC']


def distribute_daily_to_hourly_triangular(df, tolerance=0.5, observation_hour=12):
    """
    使用三角分布将逐日降水量分解为逐小时数据，并将索引设置为日期和小时组合。

    参数:
        df (pd.DataFrame): 包含每日降水数据的 DataFrame，索引为时间 (datetime)，列为 ["prec"]。
        tolerance (float): 容差（未使用，因为总是使用三角分布）。
        observation_hour (int): 观测时刻，用于生成三角分布。

    返回:
        pd.DataFrame: 逐小时降水数据，索引为日期和小时组合，列为 ["prec"]。
    """
    results = []

    for date, row in df.iterrows():
        daily_total = row["prec"]

        # 创建三角分布权重
        hours = np.arange(1, 25)  # 每日24小时
        weights = np.maximum(0, 1 - abs(hours - observation_hour) / observation_hour)
        weights /= weights.sum()  # 标准化为权重

        # 计算逐小时降水量
        hourly_precip = weights * daily_total

        # 构建 DataFrame，使用组合索引
        index = [pd.Timestamp(date) + pd.to_timedelta(h - 1, unit='h') for h in hours]
        distributed_precip = pd.DataFrame({"prec": hourly_precip}, index=index)
        results.append(distributed_precip)

    # 汇总所有分解结果
    distributed_data = pd.concat(results)
    return distributed_data


def PanEvaporationValueComputedByHamon(aTMinTS: pd.DataFrame, aTMaxTS, aDegF: bool, aLatDeg:float, aCTS=None):
    """
    compute Hamon - PET
    :param aTMinTS: Min Air Temperature - daily
    :param aTMaxTS: Max Air Temperature - daily
    :param aDegF: Temperature in Degrees F (True) or C (False)
    :param aLatDeg: Latitude, in degrees
    :param aCTS: Monthly variable coefficients 默认为None 使用内部默认系数
    :return:
    """
    # check latitude
    if MetComputeLatitudeMin > aLatDeg or aLatDeg > MetComputeLatitudeMax:
        raise ValueError(f'Latitude must be between {MetComputeLatitudeMin} and {MetComputeLatitudeMax}')
    aTAVC = (aTMinTS['TMIN'] + aTMaxTS['TMAX']) / 2
    JulDay = 30.5 * (aTMinTS.index.month - 1) + aTMinTS.index.day
    # convert to radians
    LatRdn = aLatDeg * DegreesToRadians
    Phi = LatRdn
    AD = 0.40928 * np.cos(0.0172141 * (172.0 - JulDay))
    SS = np.sin(Phi) * np.sin(AD)
    CS = np.cos(Phi) * np.cos(AD)
    X2 = -SS / CS
    Delt = 7.6394 * (1.5708 - np.atan(X2 / np.sqrt(1.0 - X2 ** 2)))
    SunR = 12.0 - Delt / 2.0
    SUNS = 12.0 + Delt / 2.0
    DYL = (SUNS - SunR) / 12

    #convert temperature to Centigrade if necessary
    if aDegF:
        aTAVC = (aTAVC - 32.0) * (5.0 / 9.0)
    #Hamon equation
    VPSAT = 6.108 * np.exp(17.26939 * aTAVC / (aTAVC + 237.3))
    VDSAT = 216.7 * VPSAT / (aTAVC + 273.3)
    # PET = CTS * DYL * DYL * VDSAT
    if aCTS is None:
        aCTS=defHMonCoeff
    lPanEvap = aCTS[aTMinTS.index.month] * DYL * DYL * VDSAT
    #when the estimated pan evaporation is negative the value is set to zero
    lPanEvap = lPanEvap.clip(lower=0)
    return lPanEvap.to_frame(name="DEVT")


def PanEvaporationValueComputedByPenman(aMinTmp: pd.DataFrame, aMaxTmp: pd.DataFrame, aDewTmp: pd.DataFrame,
                                        aWindSp: pd.DataFrame, aSolRad: pd.DataFrame):
    """
    Compute daily pan evaporation (inches)

    based on the Penman(1948) formula and the method of Kohler, Nordensen, and Fox (1955).
    :param aMinTmp: daily minimum air temperature (degF)
    :param aMaxTmp: daily maximum air temperature (degF)
    :param aDewTmp: dewpoint temperature (degF)
    :param aWindSp: wind movement (miles/day)
    :param aSolRad: solar radiation (langleys/day)
    :return: pan evaporation (inches/day)
    """
    # compute average daily air temperature
    lAirTmp = (aMinTmp['TMIN'] + aMaxTmp['TMAX']) / 2.0

    # net radiation exchange * delta
    # 改进：处理所有可能的无效值（包括NaN、负数、零）
    dsol_data = aSolRad['DSOL'].copy()
    # 处理NaN值
    dsol_data = dsol_data.fillna(0.00001)
    # 处理负数和零值
    dsol_data = dsol_data.clip(lower=0.00001)
    
    lQNDelt = np.exp((lAirTmp - 212.0) * (0.1024 - 0.01066 * np.log(dsol_data))) - 0.0001

    #Vapor pressure deficit between surface and dewpoint temps(Es-Ea) IN of Hg
    lEsMiEa = (6413252.0 * np.exp(-7482.6 / (lAirTmp + 398.36))) - (
            6413252.0 * np.exp(-7482.6 / (aDewTmp['DPTP'] + 398.36)))

    # when vapor pressure deficit turns negative it is set equal to zero
    lEsMiEa = lEsMiEa.clip(lower=0)

    # pan evap * GAMMA, GAMMA = 0.0105 inch Hg/F
    lEaGama = 0.0105 * (lEsMiEa ** 0.88) * (0.37 + 0.0041 * aWindSp['DWND'])

    # Delta = slope of saturation vapor pressure curve at air temperature
    lDelta = 47987800000.0 * np.exp(-7482.6 / (lAirTmp + 398.36)) / ((lAirTmp + 398.36) ** 2)

    #pan evaporation rate in inches per day
    lPanEvap = (lQNDelt + lEaGama) / (lDelta + 0.0105)

    #when the estimated pan evaporation is negative the value is set to zero
    lPanEvap = lPanEvap.clip(lower=0)

    return lPanEvap.to_frame("DEVP")


def DewpointTemperatureByMagnusTetens(aAvgTmp: pd.DataFrame, temp_column: str, aRelHum: pd.DataFrame, rhu_column: str):
    """
        Compute daily dewpoint temperature (°C)
    :param aAvgTmp: Air temperature in degrees Celsius (°C).
    :param temp_column: 温度列名
    :param aRelHum: Relative humidity in percentage (%).
    :param rhu_column: 相对湿度列名
    :return:
    """
    temp_column = validate_data(aAvgTmp, temp_column)
    rhu_column = validate_data(aRelHum, rhu_column)
    a = 17.27
    b = 237.7
    
    # 确保相对湿度数据有效（处理NaN、负数、零值）
    rhu_data = aRelHum[rhu_column].copy()
    rhu_data = rhu_data.fillna(1.0)  # 用1%替代NaN值
    rhu_data = rhu_data.clip(lower=0.1, upper=100.0)  # 限制在0.1-100%范围内
    
    temporary_c = np.log(rhu_data / 100) + ((a * aAvgTmp[temp_column]) / (b + aAvgTmp[temp_column]))
    dptp = (b * temporary_c) / (a - temporary_c)
    return dptp.to_frame(name='DPTP')


def MetDataDailyCloudBySunshine(aInTS: pd.DataFrame, column_name: Union[int, str]):
    """
    云量，根据日照来计算得到
    :param aInTS:
    :param column_name:
    :return:
    """
    column_name = validate_data(aInTS, column_name)
    #日照aInTS[column_name]/24：一天的日照比例
    dclo = 10 * (1 - aInTS[column_name] / 24) ** 0.6
    return dclo.to_frame(name=column_name)


if __name__ == '__main__':
    WDM = wdmutil.WDM()
    wdmpath = r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm"
    TMAX = WDM.read_dsn(wdmpath=wdmpath, dsn=19)
    TMAX = TMAX.rename(columns={"temp1223_DSN_19": "TMAX"})
    print(TMAX.head(5))
    TMIN = WDM.read_dsn(wdmpath=wdmpath, dsn=20)
    TMIN = TMIN.rename(columns={"temp1223_DSN_20": "TMIN"})
    print(TMIN.head(5))
    DEVT = PanEvaporationValueComputedByHamon(TMIN, TMAX, aDegF=True, aLatDeg=19)
    print(DEVT.head(20))

    # prec_df = pd.read_csv("../data/pre.csv")
    # prec_station_data = prec_df[prec_df['f1'] == 59843]
    # prec_station_data.loc[:, ['prec']] = prec_station_data['f10'].astype('float64')
    # # 处理特殊值
    # prec_station_data.loc[:, ['prec']] = prec_station_data['prec'].apply(handle_precipitation)
    # # 构建目标数据
    # prec_station_ts = msfl.fill_missing_values_bymean(prec_station_data, data_col='prec')
    # prec_station_ts.loc[:, ['prec']] = prec_station_ts['prec'] * 0.1
    # target_data = prec_station_ts[['time', 'prec']]
    # target_data.set_index('time', inplace=True)
    # target_data = target_data * 0.0393701
    # result = DistTriang(target_data)
    # result = result.clip(lower=0)
    # result.to_csv("../data/result.csv")
    # print(result.head(24))

    # WDM = wdmutil.WDM()
    # DWND = WDM.read_dsn(wdmpath=r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm",
    #                     dsn=21)
    # DWND = DWND.rename(columns={"temp1223_DSN_21": "DWND"})
    # WIND = DisWnd(DWND)
    # print(WIND.head(26))

    # WDM = wdmutil.WDM()
    # DWND = WDM.read_dsn(wdmpath=r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm",
    #                     dsn=21)
    # DWND = DWND.rename(columns={"temp1223_DSN_21": "DWND"})
    # WIND = DisWnd(DWND)
    # print(WIND.head(26))

    # WDM = wdmutil.WDM()
    # DEVT = WDM.read_dsn(wdmpath=r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm",
    #                     dsn=25)
    # DEVT = DEVT.rename(columns={"temp1223_DSN_25": "DEVT"})
    # PEVT = DisPET(DEVT, 19)
    # print(PEVT.head(20))

    # WDM = wdmutil.WDM()
    # DSOL = WDM.read_dsn(wdmpath=r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm",
    #                     dsn=24)
    # DSOL = DSOL.rename(columns={"temp1223_DSN_24": "DSOL"})
    # SOLR = DisSolar(DSOL, 19)
    # print(SOLR.head(20))

    # WDM = wdmutil.WDM()
    # TMAX = WDM.read_dsn(wdmpath=r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm",
    #                     dsn=19)
    # TMAX = TMAX.rename(columns={"temp1223_DSN_19": "TMAX"})
    # TMIN = WDM.read_dsn(wdmpath=r"G:\1ashuzhongguo\code\PycharmProjects\hspf_climateprocessor\notebook\temp1223.wdm",
    #                     dsn=20)
    # TMIN = TMIN.rename(columns={"temp1223_DSN_20": "TMIN"})
    # print(TMAX.head())
    # print(TMIN.head())
    # lHRTemp = DisTemp(aMnTmpTS=TMIN, aMxTmpTS=TMAX, aObsTime=24)
    # print("result……")
    # print(lHRTemp)
