import pandas as pd

from MetSave import *
from Metcalalg import *
from MetUtils import *


# ==================================逐日数据存储====================================
# TMAX (19,39,59,...199) daily maximum temperature
def MetDataDailyTMAX(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 19,
                     column: Union[int, str] = 'TMAX'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("TMAX")
    saveDailyTmax(aInTS, wdmpath, location, dsn)


def MetDataDailyTMIN(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 20,
                     column: Union[int, str] = 'TMIN'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("TMIN")
    saveDailyTmin(aInTS, wdmpath, location, dsn)


def MetDataDailyDWND(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 21,
                     column: Union[int, str] = 'DWND'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DWND")
    saveDailyWIND(aInTS, wdmpath, location, dsn)


def MetDataDailyDCLO(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 22,
                     column: Union[int, str] = 'DCLO'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DCLO")
    saveDailyDCLO(aInTS, wdmpath, location, dsn)


def MetDataDailyDPTP(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 23,
                     column: Union[int, str] = 'DPTP'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DPTP")
    saveDailyDPTP(aInTS, wdmpath, location, dsn)


def MetDataDailyDSOL(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 24,
                     column: Union[int, str] = 'DSOL'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DSOL")
    saveDailyDSOL(aInTS, wdmpath, location, dsn)


def MetDataDailyDEVT(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 25,
                     column: Union[int, str] = 'DEVT'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DEVT")
    saveDailyDEVT(aInTS, wdmpath, location, dsn)


def MetDataDailyDEVP(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 26,
                     column: Union[int, str] = 'DEVP'):
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DEVP")
    saveDailyDEVP(aInTS, wdmpath, location, dsn)

def DistEqual(aDyTSer: pd.DataFrame):
    """
    将日降雨数据均匀分布到24小时
    
    :param aDyTSer: 包含日降雨量的DataFrame，索引为日期
    :return: 逐小时降雨量的Series
    """
    
    def distribute_equally(daily_sum):
        """
        将日降雨量均匀分布到24小时
        
        :param daily_sum: 日降雨总量
        :return: 24个小时的降雨量列表
        """
        # 处理负值或无效值
        if daily_sum < 0 or pd.isna(daily_sum):
            return [0.0] * 24
        
        # 均匀分布：每小时 = 日总量 / 24
        hourly_value = daily_sum / 24.0
        
        # 返回24个相等的小时值
        return [hourly_value] * 24
    
    # 为每一天应用均匀分布函数
    aDyTSer['hourly_values'] = aDyTSer.iloc[:, 0].apply(distribute_equally)
    
    # 创建完整的小时时间索引
    full_index = pd.date_range(
        start=aDyTSer.index.min(),
        end=aDyTSer.index.max() + pd.Timedelta(days=1) - pd.Timedelta(hours=1),
        freq="h"
    )
    
    # 重新索引到小时级别
    aHrTSer = aDyTSer.reindex(full_index, method='ffill')
    
    # 添加小时索引列
    aHrTSer['hour'] = aHrTSer.index.hour
    
    # 提取对应小时的降雨量
    aHrTSer['PREC'] = aHrTSer.parallel_apply(
        lambda row: row['hourly_values'][row['hour']] if row['hourly_values'] is not None else 0.0, 
        axis=1
    )
    
    return aHrTSer['PREC']

def DistTriang(aDyTSer: pd.DataFrame):
    def disa(aDaySum):
        aHrVals = [0] * 24
        aRetCod = 0
        if aDaySum < 0:
            return aHrVals
        i = 0
        while i < len(Sums) and aDaySum > Sums[i]:
            i += 1
        if i >= len(Sums):
            warnings.warn("precipitation too big")
            aHrVals = [-9.8] * 23 + [aDaySum]
            return aHrVals
        lRndOff = 0.001
        lCarry = 0
        lRatio = aDaySum / Sums[i]
        lDaySum = 0
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
        
        # 改进：增加容错机制，减少不必要的警告
        diff = np.abs(aDaySum - lDaySum)
        if diff > lRndOff:
            # 尝试调整最大的小时值来补偿差异
            max_hour_idx = np.argmax(aHrVals)
            aHrVals[max_hour_idx] += (aDaySum - lDaySum)
            lDaySum = sum(aHrVals)
            
            # 重新检查，只有在差异仍然很大时才发出警告
            final_diff = np.abs(aDaySum - lDaySum)
            if final_diff > lRndOff * 10:  # 提高容忍度
                aRetCod = -2
                warnings.warn(f"values not distributed properly: difference={final_diff:.6f}")
        
        if aRetCod != 0:
            aHrVals = [-9.8] * 23 + [aDaySum]
        return aHrVals

    aDyTSer['hourly_value'] = aDyTSer.iloc[:, 0].map(disa)
    # aHrTSer = aDyTSer.resample("h").ffill()
    full_index = pd.date_range(
        start=aDyTSer.index.min(),
        end=aDyTSer.index.max() + pd.Timedelta(days=1) - pd.Timedelta(hours=1),
        freq="h"
    )
    aHrTSer = aDyTSer.reindex(full_index, method='ffill')

    aHrTSer['hour'] = aHrTSer.index.hour
    # aHrTSer['PREC'] = aHrTSer.apply(lambda x: x['hourly_value'][x['hour']], axis=1)
    # parallel_apply
    aHrTSer['PREC'] = aHrTSer.parallel_apply(lambda x: x['hourly_value'][x['hour']], axis=1)
    return aHrTSer['PREC']

# ==================================逐小时数据分解加存储====================================
def MetDataHourlyPREC(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 11, method: str = "equal",
                      cascade_options=None, hourly_data_obs=None, zerodiv="uniform", shift=0, ):
    """
    逐小时降水，根据逐日分解来分解
    :param aInTS: 逐日降水
    :param wdmpath: wdm文件路径
    :param location: 站点名称或站点ID
    :param dsn: 数据序列编号 默认是11
    :param method: 分解方法 - "equal": 均匀分布, "triangular": 三角分布, 其他默认三角分布
    :param cascade_options: cascade object including statistical parameters for the cascade model (暂未实现)
    :param hourly_data_obs: pd.Series observed hourly data of master station (暂未实现)
    :param zerodiv: method to deal with zero division by key "uniform" --> uniform distribution (暂未实现)
    :param shift: shifts the precipitation data by shift (int) steps (eg +7 for 7:00 to 6:00) (暂未实现)
    :return:
    """
    if not isinstance(aInTS, pd.DataFrame):
        raise TypeError(f"输入对象必须是 Pandas DataFrame，但当前类型为: {type(aInTS)}")

    if not isinstance(aInTS.index, pd.DatetimeIndex):
        raise TypeError("索引必须是时间类型 (DatetimeIndex)，但当前索引类型为: {}".format(type(aInTS.index)))

    # 根据method参数选择分布方法
    if method.lower() == "equal":
        print(f"  使用均匀分布方法分解降水数据")
        prec_df = DistEqual(aInTS)
    else:
        print(f"  使用三角分布方法分解降水数据")
        prec_df = DistTriang(aInTS)
    
    saveHourlyPREC(prec_df.to_frame("PREC"), wdmpath, location, dsn)


def MetDataHourlyEVAP(aInTS: pd.DataFrame, wdmpath: str, location: str, aLatDeg: float, dsn: int = 12,
                      column: Union[int, str] = 'DEVP'):
    """
    逐小时蒸发，根据逐日蒸发来分解
    :param aInTS: 包含逐日蒸发的DataFrame
    :param wdmpath: wdm文件路径
    :param location: 站点名或站点ID
    :param aLatDeg: 纬度，单位是十进制角度
    :param dsn: 数据序列号ID
    :param devp_name: 蒸发的列名
    :return:
    """
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DEVP")
    evap_df = DisPET(aInTS, column="DEVP", aLatDeg=aLatDeg)
    saveHourlyEVAP(evap_df, wdmpath=wdmpath, location=location, dsn=dsn)


def MetDataHourlyATM(aInTS: pd.DataFrame, aObsTime: int, wdmpath: str, location: str, dsn: int = 13,
                     tmax_column: Union[str, int] = "TMAX", tmin_column: Union[str, int] = "TMIN"):
    """
    逐小时温度，根据最大和最小温度来分解
    :param aInTS: 含有最大和最小温度，索引为时间
    :param aObsTime: 观察时间
    :param wdmpath: wdm文件路径
    :param location: 站点名称或站点ID
    :param dsn: 数据序列编号
    :param tmax_column: 最大温度名称或索引，默认为列名称TMAX
    :param tmin_column: 最小温度名称或索引，默认为列名称TMIN
    :return:
    """
    tmax_column = validate_data(aInTS, tmax_column)
    tmin_column = validate_data(aInTS, tmin_column)
    tmax_df = aInTS[tmin_column].to_frame(name="TMAX")
    tmin_df = aInTS[tmax_column].to_frame(name="TMIN")
    atem_df = DisTemp(tmin_df, tmax_df, aObsTime)
    saveHourlyATEM(atem_df, wdmpath=wdmpath, location=location, dsn=dsn)


def MetDataHourlyWIND(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 14,
                      column: Union[str, int] = "DWND", aDCurve: List[float] = None):
    """
    逐小时分速数据，根据逐日风速来分解
    :param aInTS: 逐日分速数据
    :param wdmpath: wdm文件路径
    :param location: 站点编号或站点名称
    :param dsn: 数据系列号
    :param column: 风速列名 默认为DWND
    :param aDCurve: 24小时分解系数,默认为None,使用内部的一套系数。
    :return:
    """
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DWND")
    saveHourlyWIND(DisWnd(aInTS), wdmpath=wdmpath, location=location, dsn=dsn)


def MetDataHourlySOLR(aInTS: pd.DataFrame, wdmpath: str, location: str, aLatDeg: float, dsn: int = 15,
                      column: Union[str, int] = "DSOL"):
    """
    逐小时辐射数据，根据逐日辐射数据分解
    :param aInTS: 含有逐日辐射数据DataFrame
    :param wdmpath: wdm文件路径
    :param location: 站点名或站点编号
    :param aLatDeg: 纬度 单位为十进制角度
    :param dsn: 数据系列ID
    :param column: 辐射数据列名
    :return:
    """
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DSOL")
    solr_df = DisSolar(aInTS, aLatDeg=aLatDeg)
    saveHourlySOLR(solr_df, wdmpath=wdmpath, location=location, dsn=dsn)


def MetDataHourlyPEVT(aInTS: pd.DataFrame, wdmpath: str, location: str, aLatDeg: float, dsn: int = 16,
                      column: Union[int, str] = 'DEVT'):
    """
    逐小时蒸散，根据逐日蒸散来分解
    :param aInTS: 包含逐日蒸散的DataFrame
    :param wdmpath: wdm文件路径
    :param location: 站点名或站点ID
    :param aLatDeg: 纬度，单位是十进制角度
    :param dsn: 数据序列号ID
    :param devp_name: 蒸散的列名
    :return:
    """
    column = validate_data(aInTS, column)
    aInTS = aInTS[column].to_frame("DEVT")
    pevt_df = DisPET(aInTS, column="DEVT", aLatDeg=aLatDeg)
    saveHourlyPEVT(pevt_df, wdmpath=wdmpath, location=location, dsn=dsn)


def MetDataHourlyDEWP(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 17,
                      column: Union[int, str] = 'DPTP'):
    """
    逐小时露点温度，根据逐日分解，24小时恒定假设。
    :param aInTS: 逐日的露点温度
    :param wdmpath: wdm文件路径
    :param location: 站点编号或站点名称
    :param dsn: 数据系列号
    :param column: 露点温度列名
    :return:
    """
    column = validate_data(aInTS, column)
    dewp_df = aInTS.resample("h").ffill()
    dewp_df.rename(columns={column: "DEWP"}, inplace=True)
    saveHourlyDEWP(dewp_df, wdmpath=wdmpath, location=location, dsn=dsn)


def MetDataHourlyCLOU(aInTS: pd.DataFrame, wdmpath: str, location: str, dsn: int = 18,
                      column: Union[int, str] = 'DCLO'):
    """
    逐小时云量，根据逐日分解，24小时恒定假设。
    :param aInTS: 逐日的云量
    :param wdmpath: wdm文件路径
    :param location: 站点编号或站点名称
    :param dsn: 数据系列号
    :param column: 云量列名
    :return:
    """
    column = validate_data(aInTS, column)
    dewp_df = aInTS.resample("h").ffill()
    dewp_df.rename(columns={column: "CLOU"}, inplace=True)
    saveHourlyCLOU(dewp_df, wdmpath=wdmpath, location=location, dsn=dsn)





