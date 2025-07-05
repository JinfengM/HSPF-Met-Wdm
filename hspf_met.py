import os.path
from typing import List, Dict

import pandas as pd

from MetUtils import *
from Metcalalg import *
import wdmtoolbox.wdmtoolbox as wdm
from wdmtoolbox import wdmutil
from Metcal import *
from missingfill import *
from pandarallel import pandarallel
pandarallel.initialize(verbose=0)


# ============================逐日数据==================================
def metTmax(inputfile: str, wdmpath: str, data_col: str, stations: List[str], invalid_value: int,
            scale=0.1):
    print(f"处理最大温度数据，数据列: {data_col}")
    temp_df = pd.read_csv(inputfile)
    temp_df.loc[temp_df[data_col] == invalid_value, data_col] = np.nan
    temp_df = fill_missing_values_bymean(temp_df, station_column='f1', data_col=data_col, stations=stations)
    
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        # 修复：使用整数比较
        station_data = temp_df.loc[temp_df['f1'] == int(station), [data_col, 'time']]
        
        if station_data.empty:
            print(f"❌ 站点 {station} 没有找到数据！")
            continue
            
        station_data.set_index('time', inplace=True)
        station_data.loc[:, data_col] = station_data[data_col] * scale
        # 修复：正确处理celsius_to_fahrenheit的返回值
        fahrenheit_data = celsius_to_fahrenheit(station_data, data_col)
        station_data[data_col] = fahrenheit_data.iloc[:, 0].astype('float64')
        dsn = 19 + i * 20
        MetDataDailyTMAX(station_data, wdmpath, str(station), dsn=dsn, column=data_col)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(station_data)}, DSN: {dsn}")
    return temp_df


def metTmin(inputfile: str, wdmpath: str, data_col: str, stations: List[str], invalid_value: int,
            scale=0.1):
    print(f"处理最小温度数据，数据列: {data_col}")
    temp_df = pd.read_csv(inputfile)
    temp_df.loc[temp_df[data_col] == invalid_value, data_col] = np.nan
    temp_df = fill_missing_values_bymean(temp_df, station_column='f1', data_col=data_col, stations=stations)
    # print(temp_df)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        station_data = temp_df.loc[temp_df['f1'] == int(station), [data_col, 'time']]
        
        if station_data.empty:
            print(f"❌ 站点 {station} 没有找到数据！")
            continue
            
        station_data.set_index('time', inplace=True)
        station_data.loc[:, data_col] = station_data[data_col] * scale
        # 修复：正确处理celsius_to_fahrenheit的返回值
        fahrenheit_data = celsius_to_fahrenheit(station_data, data_col)
        station_data[data_col] = fahrenheit_data.iloc[:, 0].astype('float64')
        dsn = 20 + i * 20
        MetDataDailyTMIN(station_data, wdmpath, str(station), dsn=dsn, column=data_col)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(station_data)}, DSN: {dsn}")


def metDailyWind(inputfile: str, wdmpath: str, data_col: str, stations: List[str],
                 invalid_value: int,
                 scale=0.1):
    print(f"处理日风速数据，数据列: {data_col}")
    data_df = pd.read_csv(inputfile)
    data_df.loc[data_df[data_col] == invalid_value, data_col] = np.nan
    data_df = fill_missing_values_bymean(data_df, station_column='f1', data_col=data_col, stations=stations)
    # print(data_df)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        station_data = data_df.loc[data_df['f1'] == int(station), [data_col, 'time']]
        
        if station_data.empty:
            print(f"❌ 站点 {station} 没有找到数据！")
            continue
            
        station_data.set_index('time', inplace=True)
        station_data.loc[:, data_col] = station_data[data_col] * scale
        # 修复：正确处理ms_to_mph的返回值
        mph_data = ms_to_mph(station_data, data_col)
        station_data[data_col] = mph_data.iloc[:, 0].astype('float64')
        # 修复：正确处理windTravelFromWindSpeed的返回值
        wind_travel_data = windTravelFromWindSpeed(station_data, data_col)
        station_data[data_col] = wind_travel_data.iloc[:, 0].astype('float64')
        dsn = 21 + i * 20
        MetDataDailyDWND(station_data, wdmpath, str(station), dsn=dsn, column=data_col)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(station_data)}, DSN: {dsn}")


def metDailyCloud(inputfile: str, wdmpath: str, data_col: str, stations: List[str],
                  invalid_value: int,
                  scale=0.1):
    print(f"处理日云量数据，数据列: {data_col}")
    data_df = pd.read_csv(inputfile)
    data_df.loc[data_df[data_col] == invalid_value, data_col] = np.nan
    data_df = fill_missing_values_bymean(data_df, station_column='f1', data_col=data_col, stations=stations)
    # print(data_df)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        station_data = data_df.loc[data_df['f1'] == int(station), [data_col, 'time']]
        
        if station_data.empty:
            print(f"❌ 站点 {station} 没有找到数据！")
            continue
            
        station_data.set_index('time', inplace=True)
        station_data.loc[:, data_col] = station_data[data_col] * scale
        # 修复：正确处理MetDataDailyCloudBySunshine的返回值
        cloud_data = MetDataDailyCloudBySunshine(station_data, data_col)
        station_data[data_col] = cloud_data.iloc[:, 0].astype('float64')
        dsn = 22 + i * 20
        MetDataDailyDCLO(station_data, wdmpath, str(station), dsn=dsn, column=data_col)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(station_data)}, DSN: {dsn}")


def metDailyDewpointTemperature(atem_file: str, atem_col: str, rhum_file: str, rhum_col: str, wdmpath: str,
                                stations: List[str], invalid_value: int, scale=0.1):
    #加载数据
    print(f"处理日露点温度数据，温度列: {atem_col}, 湿度列: {rhum_col}")
    atem_df = pd.read_csv(atem_file)
    rhum_df = pd.read_csv(rhum_file)
    #无效值处理为NAN
    atem_df.loc[atem_df[atem_col] == invalid_value, atem_col] = np.nan
    rhum_df.loc[rhum_df[rhum_col] == invalid_value, rhum_col] = np.nan
    #空值处理
    atem_df = fill_missing_values_bymean(atem_df, station_column='f1', data_col=atem_col, stations=stations)
    rhum_df = fill_missing_values_bymean(rhum_df, station_column='f1', data_col=rhum_col, stations=stations)
    # print(rhum_df)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        #提取相应站点
        atem_station_data = atem_df.loc[atem_df['f1'] == int(station), [atem_col, 'time']]
        rhum_station_data = rhum_df.loc[rhum_df['f1'] == int(station), [rhum_col, 'time']]
        
        if atem_station_data.empty or rhum_station_data.empty:
            print(f"❌ 站点 {station} 缺少温度或湿度数据！")
            continue
            
        #设置时间为索引
        atem_station_data.set_index('time', inplace=True)
        rhum_station_data.set_index('time', inplace=True)
        #缩放尺度
        atem_station_data.loc[:, atem_col] = atem_station_data[atem_col] * scale
        # 相对湿度scale = 1%
        rhum_station_data.loc[:, rhum_col] = rhum_station_data[rhum_col]
        # 计算露点温度
        dptp_station_data = DewpointTemperatureByMagnusTetens(aAvgTmp=atem_station_data, temp_column=atem_col,
                                                              aRelHum=rhum_station_data, rhu_column=rhum_col)
        # 修复：正确处理celsius_to_fahrenheit的返回值
        fahrenheit_data = celsius_to_fahrenheit(dptp_station_data, 'DPTP')
        dptp_station_data['DPTP'] = fahrenheit_data.iloc[:, 0].astype('float64')
        dsn = 23 + i * 20
        MetDataDailyDPTP(dptp_station_data, wdmpath, str(station), dsn=dsn, column='DPTP')
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(dptp_station_data)}, DSN: {dsn}")


def metDailySolar(inputfile: str, wdmpath: str, data_col: str, stations: List[str], invalid_value: int, scale=0.01):
    print(f"处理日太阳辐射数据，数据列: {data_col}")
    data_df = pd.read_csv(inputfile)
    data_df.loc[data_df[data_col] == invalid_value, data_col] = np.nan
    data_df = fill_missing_values_bymean(data_df, station_column='f1', data_col=data_col, stations=stations)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        station_data = data_df.loc[data_df['f1'] == int(station), [data_col, 'time']]
        
        if station_data.empty:
            print(f"❌ 站点 {station} 没有找到数据！")
            continue
            
        station_data.set_index('time', inplace=True)
        station_data[data_col] = station_data[data_col] * scale
        # 修复：正确处理mjm2_to_Ly的返回值
        ly_data = mjm2_to_Ly(station_data, data_col)
        station_data[data_col] = ly_data.iloc[:, 0].astype('float64')
        dsn = 24 + i * 20
        if station_data.empty:
            raise ValueError(f"{station}站点数据为空")
        MetDataDailyDSOL(station_data, wdmpath, station, dsn=dsn, column=data_col)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(station_data)}, DSN: {dsn}")


def metDailyEvapotranspiration(wdmpath: str, stations: List[str], station_2_latDeg: Dict[str, float],
                               hMonCoeff: List[float] = None):
    print(f"处理日蒸散发数据，基于已有温度数据计算")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, 'TMAX')
    checkStatios(stations, all_dsn_attrs, 'TMIN')
    tmax_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'TMAX')
    tmin_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'TMIN')
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        aLatDeg = station_2_latDeg.get(station)
        tmax_dsn = tmax_LOCN_DSN.get(station)
        tmin_dsn = tmin_LOCN_DSN.get(station)
        
        if not tmax_dsn or not tmin_dsn:
            print(f"❌ 站点 {station} 缺少温度数据DSN！")
            continue
            
        tmin = wdmts.read_dsn(wdmpath, tmin_dsn)
        tmin_source_name = f'{os.path.basename(wdmpath)[:-4]}_DSN_{tmin_dsn}'
        tmin.rename(columns={tmin_source_name: "TMIN"}, inplace=True)
        tmax = wdmts.read_dsn(wdmpath, tmax_dsn)
        tmax_source_name = f'{os.path.basename(wdmpath)[:-4]}_DSN_{tmax_dsn}'
        tmax.rename(columns={tmax_source_name: "TMAX"}, inplace=True)
        devt_df = PanEvaporationValueComputedByHamon(aTMinTS=tmin, aTMaxTS=tmax, aDegF=True, aLatDeg=aLatDeg,
                                                     aCTS=hMonCoeff)
        dsn = 25 + i * 20
        MetDataDailyDEVT(devt_df, wdmpath, station, dsn, "DEVT")
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(devt_df)}, DSN: {dsn}")


def metDailyEvaporation(wdmpath: str, stations: List[str]):
    print(f"处理日蒸发量数据，基于Penman公式计算")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, 'TMAX')
    checkStatios(stations, all_dsn_attrs, 'TMIN')
    checkStatios(stations, all_dsn_attrs, 'DPTP')
    checkStatios(stations, all_dsn_attrs, 'DSOL')
    checkStatios(stations, all_dsn_attrs, 'DWND')
    tmax_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'TMAX')
    tmin_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'TMIN')
    dptp_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'DPTP')
    dsol_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'DSOL')
    dwnd_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, 'DWND')
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        tmax_dsn = tmax_LOCN_DSN.get(station)
        tmin_dsn = tmin_LOCN_DSN.get(station)
        dptp_dsn = dptp_LOCN_DSN.get(station)
        dwnd_dsn = dwnd_LOCN_DSN.get(station)
        dsol_dsn = dsol_LOCN_DSN.get(station)
        
        if not all([tmax_dsn, tmin_dsn, dptp_dsn, dwnd_dsn, dsol_dsn]):
            print(f"❌ 站点 {station} 缺少必要的气象数据DSN！")
            continue
            
        tmax = read_dsn(wdmpath, tmax_dsn, "TMAX")
        tmin = read_dsn(wdmpath, tmin_dsn, "TMIN")
        dptp = read_dsn(wdmpath, dptp_dsn, "DPTP")
        dwnd = read_dsn(wdmpath, dwnd_dsn, "DWND")
        dsol = read_dsn(wdmpath, dsol_dsn, "DSOL")
        devp_df = PanEvaporationValueComputedByPenman(aMinTmp=tmin, aMaxTmp=tmax, aDewTmp=dptp, aWindSp=dwnd,
                                                      aSolRad=dsol)
        dsn = 26 + i * 20
        MetDataDailyDEVP(devp_df, wdmpath, station, dsn, "DEVP")
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(devp_df)}, DSN: {dsn}")


# ============================逐小时数据==================================
def metHourlyPREC(prec_file: str, wdmpath: str, stations: List[str], data_col: str, scale=0.01, method: str = "equal"):
    """
    处理小时降水数据
    
    :param prec_file: 降水数据文件路径
    :param wdmpath: WDM文件路径
    :param stations: 站点列表
    :param data_col: 数据列名
    :param scale: 数据缩放因子
    :param method: 降雨分布方法 - "equal": 均匀分布, "triangular": 三角分布
    """
    print(f"🌧️  处理小时降水数据，数据列: {data_col}, 分布方法: {method}")
    data_df = pd.read_csv(prec_file)
    # 修复：先转换为float类型避免数据类型兼容性警告
    data_df[data_col] = data_df[data_col].astype('float64')
    data_df.loc[:, [data_col]] = data_df[data_col].parallel_apply(prec_special_values)
    data_df = fill_missing_values_bymean(data_df, station_column='f1', data_col=data_col, stations=stations)
    for i, station in enumerate(stations):
        print(f"  🔄 处理站点: {station}")
        
        station_data = data_df.loc[data_df['f1'] == int(station), [data_col, 'time']]
        
        if station_data.empty:
            print(f"    ❌ 站点 {station} 没有找到数据！")
            continue
            
        station_data.set_index('time', inplace=True)
        # 毫米转为英寸
        # 数据 = 原始数据 * 缩放尺度 * 单位转换系数
        station_data = station_data * scale * 0.0393701
        dsn = 11 + i * 20
        #precip字段名称固定的，必须这样写。
        station_data.rename(columns={data_col: 'precip'}, inplace=True)
        MetDataHourlyPREC(aInTS=station_data, wdmpath=wdmpath, location=station, dsn=dsn, method=method)
        print(f"    ✅ 站点 {station} 处理成功，数据行数: {len(station_data)}, DSN: {dsn}")


def metHourlyEVAP(wdmpath: str, stations: List[str], station_2_latDeg: Dict[str, float], tstype:str='DEVP'):
    print(f"处理小时蒸发数据，基于日蒸发数据分解")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype)
    devp_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        devp_dsn = devp_LOCN_DSN.get(station)
        
        if not devp_dsn:
            print(f"❌ 站点 {station} 缺少蒸发数据DSN！")
            continue
            
        devp = read_dsn(wdmpath, devp_dsn, tstype)
        aLatDeg = station_2_latDeg.get(station)
        dsn = 12 + i * 20
        MetDataHourlyEVAP(aInTS=devp, wdmpath=wdmpath, location=station, aLatDeg=aLatDeg, dsn=dsn)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(devp)}, DSN: {dsn}")


def metHourlyATEM(wdmpath: str, stations: List[str], aObsTime:int, ):
    print(f"处理小时温度数据，基于日最大最小温度分解，观测时间: {aObsTime}")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    tstype_tmax = 'TMAX'
    tstype_tmin = 'TMIN'
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype_tmax)
    checkStatios(stations, all_dsn_attrs, tstype_tmin)
    tmax_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype_tmax)
    tmin_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype_tmin)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        tmax_dsn = tmax_LOCN_DSN.get(station)
        tmin_dsn = tmin_LOCN_DSN.get(station)
        
        if not tmax_dsn or not tmin_dsn:
            print(f"❌ 站点 {station} 缺少温度数据DSN！")
            continue
            
        tmax = read_dsn(wdmpath, tmax_dsn, tstype_tmax)
        tmin = read_dsn(wdmpath, tmin_dsn, tstype_tmin)
        temp = pd.concat([tmax, tmin], axis=1)
        dsn = 13 + i * 20
        MetDataHourlyATM(aInTS=temp, aObsTime=aObsTime, wdmpath=wdmpath, location=station, dsn=dsn)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(temp)}, DSN: {dsn}")


def metHourlyWIND(wdmpath: str, stations: List[str], aDCurve:List[float]= None, tstype:str='DWND'):
    print(f"处理小时风速数据，基于日风速数据分解")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype)
    wind_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        wind_dsn = wind_LOCN_DSN.get(station)
        
        if not wind_dsn:
            print(f"❌ 站点 {station} 缺少风速数据DSN！")
            continue
            
        wind = read_dsn(wdmpath, wind_dsn, tstype)
        dsn = 14 + i * 20
        MetDataHourlyWIND(aInTS=wind, wdmpath=wdmpath, location=station, dsn=dsn, aDCurve=aDCurve)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(wind)}, DSN: {dsn}")


def metHourlySOLR(wdmpath: str, stations: List[str], station_2_latDeg: Dict[str, float], tstype:str='DSOL'):
    print(f"处理小时太阳辐射数据，基于日辐射数据分解")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype)
    solar_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        solar_dsn = solar_LOCN_DSN.get(station)
        
        if not solar_dsn:
            print(f"❌ 站点 {station} 缺少太阳辐射数据DSN！")
            continue
            
        aLatDeg = station_2_latDeg.get(station)
        solar = read_dsn(wdmpath, solar_dsn, tstype)
        dsn = 15 + i * 20
        MetDataHourlySOLR(aInTS=solar, wdmpath=wdmpath, location=station, aLatDeg=aLatDeg, dsn=dsn)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(solar)}, DSN: {dsn}")


def metHourlyPEVT(wdmpath: str, stations: List[str], station_2_latDeg: Dict[str, float], tstype:str='DEVT'):
    print(f"处理小时蒸散发数据，基于日蒸散发数据分解")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype)
    devt_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        devt_dsn = devt_LOCN_DSN.get(station)
        
        if not devt_dsn:
            print(f"❌ 站点 {station} 缺少蒸散发数据DSN！")
            continue
            
        devt = read_dsn(wdmpath, devt_dsn, tstype)
        aLatDeg = station_2_latDeg.get(station)
        dsn = 16 + i * 20
        MetDataHourlyPEVT(devt, wdmpath, station, aLatDeg, dsn)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(devt)}, DSN: {dsn}")


def metHourlyDEWP(wdmpath: str, stations: List[str], tstype:str='DPTP'):
    print(f"处理小时露点温度数据，基于日露点温度数据分解")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype)
    dptp_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        dptp_dsn = dptp_LOCN_DSN.get(station)
        
        if not dptp_dsn:
            print(f"❌ 站点 {station} 缺少露点温度数据DSN！")
            continue
            
        dptp = read_dsn(wdmpath, dptp_dsn, tstype)
        dsn = 17 + i * 20
        MetDataHourlyDEWP(dptp, wdmpath, station, dsn)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(dptp)}, DSN: {dsn}")


def metHourlyCLOU(wdmpath: str, stations: List[str], tstype:str='DCLO'):
    print(f"处理小时云量数据，基于日云量数据分解")
    dsns = wdm.listdsns(wdmpath)
    wdmts = wdmutil.WDM()
    all_dsn_attrs = [wdmts.describe_dsn(wdmpath, dsn) for dsn in dsns]
    checkStatios(stations, all_dsn_attrs, tstype)
    dclo_LOCN_DSN = get_stns_dsn(stations, all_dsn_attrs, tstype)
    for i, station in enumerate(stations):
        print(f"处理站点: {station}")
        
        dclo_dsn = dclo_LOCN_DSN.get(station)
        
        if not dclo_dsn:
            print(f"❌ 站点 {station} 缺少云量数据DSN！")
            continue
            
        dclo = read_dsn(wdmpath, dclo_dsn, tstype)
        dsn = 18 + i * 20
        MetDataHourlyCLOU(dclo, wdmpath, station, dsn)
        print(f"✅ 站点 {station} 处理成功，数据行数: {len(dclo)}, DSN: {dsn}")


if __name__ == '__main__':
    #数据信息
    target_stations = ['59843', '59848', '59851', '59854']
    stns_2_letdeg = {'59843': 19.7333, '59848': 19.2333, '59851': 19.7, '59854': 19.3666}
    invalid_value = 32766
    wdmpath = "data/hspf_met0705.wdm"
    tempfile = "data/temp.csv"
    windfile = "data/win.csv"
    ssdfile = "data/ssd.csv"
    rhumfile = "data/rhu.csv"
    radifile = "data/radi.csv"
    precipfile = "data/pre.csv"
    
    # 降雨分布方法配置
    # "equal" = 均匀分布（每小时相等）
    # "triangular" = 三角分布（按小时变化）
    precipitation_method = "equal"  # 用户可根据需要修改此参数

    print("=" * 60)
    print("开始HSPF气象数据处理")
    print(f"目标站点: {target_stations}")
    print(f"输出文件: {wdmpath}")
    print(f"降雨分布方法: {precipitation_method}")
    print("=" * 60)
    
    # ============================逐日数据==================================
    print("\n📅 开始处理日数据...")
    
    # DSN:19.最大温度:TMAX
    metTmax(inputfile=tempfile, data_col='f9', stations=target_stations, invalid_value=invalid_value, wdmpath=wdmpath)

    # DSN:20.最小温度:TMIN
    metTmin(inputfile=tempfile, data_col='f10', stations=target_stations, invalid_value=invalid_value, wdmpath=wdmpath)

    # DSN:21.风速:DWND
    metDailyWind(inputfile=windfile, data_col='f8', stations=target_stations, invalid_value=invalid_value, wdmpath=wdmpath)

    # DSN:22.云量:DCLO
    metDailyCloud(inputfile=ssdfile, data_col='f8', stations=target_stations, invalid_value=invalid_value,wdmpath=wdmpath)
    
    # DSN:23.露点温度:DPTP
    metDailyDewpointTemperature(atem_file=tempfile, atem_col='f8', rhum_file=rhumfile, rhum_col='f8', wdmpath=wdmpath,stations=target_stations, invalid_value=invalid_value, scale=0.1)

    # DSN:24.DSOL太阳辐射:DSOL
    metDailySolar(inputfile=radifile, wdmpath=wdmpath, data_col='f8', stations=target_stations, invalid_value=999998, scale=0.01)

    #DSN:25.DEVT:DEVT
    metDailyEvapotranspiration(wdmpath=wdmpath, station_2_latDeg=stns_2_letdeg, stations=target_stations)

    #DSN:26.DEVP:DEVP
    metDailyEvaporation(wdmpath=wdmpath, stations=target_stations)
    
    print("✅ 日数据处理完成")
    
    # ============================逐小时数据==================================
    print("\n⏰ 开始处理小时数据...")
    
    #print("PREC")
    metHourlyPREC(prec_file=precipfile, wdmpath=wdmpath, stations=target_stations, data_col='f10', scale=0.1, method=precipitation_method)

    #print("EVAP")
    metHourlyEVAP(wdmpath=wdmpath, stations=target_stations, station_2_latDeg=stns_2_letdeg)

    #print("ATEM")
    metHourlyATEM(wdmpath=wdmpath, stations=target_stations, aObsTime=24)

    #print("WIND")
    metHourlyWIND(wdmpath=wdmpath, stations=target_stations)

    #print("SOLR")
    metHourlySOLR(wdmpath=wdmpath, stations=target_stations, station_2_latDeg=stns_2_letdeg)

    #print("PEVT")
    metHourlyPEVT(wdmpath=wdmpath, stations=target_stations, station_2_latDeg=stns_2_letdeg)

    #print("DEWP")
    metHourlyDEWP(wdmpath=wdmpath, stations=target_stations)

    #print("CLOU")
    metHourlyCLOU(wdmpath=wdmpath, stations=target_stations)

    print("✅ 小时数据处理完成")
    
    # 查看生成的DSN
    print("\n📊 WDM文件统计信息:")
    dsns = wdm.listdsns(wdmpath)
    print(f"生成的DSN数量: {len(dsns)}")
    print(f"DSN列表: {sorted(dsns)}")

    # 显示每个DSN的信息
    print("\n📋 DSN详细信息:")
    for dsn in dsns:
        print(f"\nDSN {dsn} 信息:")
        print(wdm.describedsn(wdmpath, dsn))
        
    print("\n" + "=" * 60)
    print("🎉 HSPF气象数据处理完成！")
    print("=" * 60)
