import os
from typing import Union, List

import numpy as np
import pandas as pd
from wdmtoolbox import wdmutil

def validate_data(aInTS: pd.DataFrame, column: Union[int, str]):
    """
    校验数据，校验数据是否为pandas的DataFrame,索引是否为类型DatetimeIndex，column列是否在数据DataFrame
    :param aInTS: 校验数据
    :param column: 校验数据列
    :return:
    """
    if not isinstance(aInTS, pd.DataFrame):
        raise TypeError(f"输入对象必须是 Pandas DataFrame，但当前类型为: {type(aInTS)}")

    if not isinstance(aInTS.index, pd.DatetimeIndex):
        raise TypeError("索引必须是时间类型 (DatetimeIndex)，但当前索引类型为: {}".format(type(aInTS.index)))
    if isinstance(column, int):
        if not 0 < column < len(aInTS.columns):
            raise ValueError(f"当前列索引 {column} 不在 0-{len(aInTS.columns) - 1} 之间")
        else:
            column = aInTS.columns[column]
    if isinstance(column, str) and column not in aInTS.columns:
        raise ValueError(f"列名 {column} 不在 DataFrame 的列中")
    return column


def celsius_to_fahrenheit(aInTS: pd.DataFrame, column: Union[int, str]):
    """
    将摄氏度转换为华氏度。
    参数:
    celsius (float): 摄氏温度值。
    返回:
    float: 对应的华氏温度值。
    """
    column = validate_data(aInTS, column)
    fahrenheit = (aInTS[column] * 9 / 5) + 32
    return fahrenheit.to_frame()


def mjm2_to_Ly(aInTS: pd.DataFrame, column: Union[int, str]):
    """
       1MJ/m²约等于23.9cal/cm², 1Ly=1cal/cm²
    :param aInTS: 输入数据
    :param column: 需要转换的单位的列名
    :return:
    """
    column = validate_data(aInTS, column)
    conversion_factor = 23.9
    lyhr_df = aInTS[column] * conversion_factor
    return lyhr_df.to_frame()


def ms_to_mph(aInTS: pd.DataFrame, column: Union[int, str]):
    """
    风速单位转换：m/s转mile/hour
    :param aInTS: 风速数据
    :param column: 风速列名
    :return:
    """
    column = validate_data(aInTS, column)
    # 1 m/s = 2.23694 mile/hour
    conversion_factor = 2.23694
    miles_per_hour = aInTS[column] * conversion_factor
    return miles_per_hour.to_frame()


def windTravelFromWindSpeed(aInTS: pd.DataFrame, column: Union[int, str]):
    """
    风速(mile/hour)转风行距离
    :param aInTS: 数据
    :param column: 风速列名
    :return:
    """
    column = validate_data(aInTS, column)
    # 修复：避免inplace警告，使用重新赋值
    clipped_data = aInTS[column].clip(lower=0)
    wind_travel = clipped_data * 24
    return wind_travel.to_frame()


def checkStatios(checkstns: List[str], exist_stns_attr: List[object], tsytpe: str):
    """
    检查目标站点是否已经存在
    :param checkstns: 检查的站点
    :param exist_stns_attr: 已经存在的站点
    :param tsytpe: 检查的时序类型
    :return:
    """
    not_in_attrs = [dsn_attrs for dsn_attrs in exist_stns_attr if dsn_attrs['IDLOCN'] not in checkstns and
                    tsytpe == dsn_attrs['TSTYPE']]
    if not_in_attrs and len(not_in_attrs) > 0:
        no_exist_stations = [attrs['IDLOCN'] for attrs in not_in_attrs]
        all_stations = [attrs['IDLOCN'] for attrs in exist_stns_attr]
        raise ValueError(f"{no_exist_stations}没有在{all_stations}其中")


def get_stns_dsn(target_stns: List[str], exist_stns_attr: List[object], tsytpe: str):
    """
    获取通过站点和时序类型查询dsn
    :param target_stns: 目标站点ID
    :param exist_stns_attr: 已经存在DSN及相应的属性,包括IDLOCN
    :param tsytpe: 时序类型名称
    :return:
    """
    return {dsn_attrs['IDLOCN']: dsn_attrs['DSN'] for dsn_attrs in exist_stns_attr if
            dsn_attrs['IDLOCN'] in target_stns and
            tsytpe == dsn_attrs['TSTYPE']}

def read_dsn(wdmpath:str, dsn:int, tsytpe: str):
    """
    通过dsn获取数据，并把列名转换为时序名称
    :param wdmpath: wdm文件路径
    :param dsn: 数据序列编号
    :param tsytpe: 时序类型名称
    :return:
    """
    wdm = wdmutil.WDM()
    dsn_data = wdm.read_dsn(wdmpath, dsn)
    tmin_source_name = f'{os.path.basename(wdmpath)[:-4]}_DSN_{dsn}'
    dsn_data.rename(columns={tmin_source_name: tsytpe}, inplace=True)
    return dsn_data

def prec_special_values(x):
    if (x == 32766) or (x == 32744):
        return np.nan
    elif x == 32700:
        return 0
    elif x >= 3200:
        return x - 32000
    elif x >= 31000:
        return x - 31000
    elif x >= 30000:
        return x - 30000
    else:
        return x
