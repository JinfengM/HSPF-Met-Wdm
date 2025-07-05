import warnings
import os
from typing import List

import numpy as np
import pandas as pd
from wdmtoolbox import wdmtoolbox as wdm
from pandarallel import pandarallel
pandarallel.initialize(verbose=0)


def miss_fill_mean(
        obs_data: pd.DataFrame,
        station_column: str = "f1",
        year_column: str = "f5",
        month_column: str = "f6",
        day_column: str = "f7",
        data_column: str = "f8",
        station_id: int = 59843,
        invalid_value: int = 32766,
) -> pd.DataFrame:
    """
    这里是简单填充，如果缺测值的前后有值(非连续缺测)，如果是连续缺测则由有值的日期往前填充。
    :param file_path: 文件路径
    :param data_column: 需要填充的数据列
    :param station_id:  站点编号
    :param invalid_value: 无效值
    :param fill_value: 填写值
    :return:
    """
    obs_data.loc[:, ["time"]] = pd.to_datetime(
        obs_data[year_column].astype(str)
        + obs_data[month_column].astype(str).str.zfill(2)
        + obs_data[day_column].astype(str).str.zfill(2)
    )
    miss_datas = obs_data[
        (obs_data[station_column] == station_id)
        & (obs_data[data_column] == invalid_value)
        ]
    if len(miss_datas) == 0:
        warnings.warn("没有缺测值")
        return obs_data
    for miss_data in miss_datas.iterrows():
        miss_time = miss_data[1]["time"]
        day = pd.Timedelta(1, unit="D")
        last_time = miss_time - day
        next_time = miss_time + day
        last_f8 = obs_data.loc[
            (obs_data[station_column] == station_id) & (obs_data["time"] == last_time),
            data_column,
        ].values
        next_f8 = obs_data.loc[
            (obs_data[station_column] == station_id) & (obs_data["time"] == next_time),
            data_column,
        ].values
        obs_data.loc[
            (obs_data[station_column] == station_id) & (obs_data["time"] == miss_time),
            data_column,
        ] = int((last_f8.item() + next_f8.item()) / 2)
    return obs_data


import pandas as pd
import numpy as np


def fill_missing_values_bymean(df,
                               station_column: str = "f1",
                               data_col='f8',
                               year_col='f5',
                               month_col='f6',
                               day_col='f7',
                               stations: List[str] = None):
    """
    根据指定的年、月、日列构建日期列，并按日期升序排序后重构索引。
    针对指定站点的数据，按以下规则填充缺失值：
    1. 连续缺失：后向填充（backward fill）。
    2. 非连续缺失：前后值的均值填充。

    参数：
    df : DataFrame
        输入的数据集。
    station_column : str
        站点列的列名。
    data_col : str
        需要填充缺失值的列名。
    year_col : str
        年份列名称。
    month_col : str
        月份列名称。
    day_col : str
        天列名称。
    stations : list
        需要处理的站点值列表。如果为 None，处理所有站点。
    """
    if stations is not None and not isinstance(stations, list):
        stations = [stations]
    # 创建日期列
    stations = [int(station) for station in stations]
    if not isinstance(df.index, pd.DatetimeIndex):
        df["time"] = pd.to_datetime(
            df[year_col].astype(str)
            + df[month_col].astype(str).str.zfill(2)
            + df[day_col].astype(str).str.zfill(2)
        )

    # 筛选需要处理的站点
    if stations is not None:
        process_df = df[df[station_column].isin(stations)]
    else:
        process_df = df
    # 按站点分组并处理
    def process_group(group):
        group = group.sort_values(by='time').reset_index(drop=True)

        # 缺失值处理逻辑
        is_nan = group[data_col].isna().tolist()
        result = []
        count = 0

        for i in range(len(group)):
            if is_nan[i]:
                count += 1
            else:
                if count > 1:  # 连续缺失
                    result.extend(['backward_fill'] * count)
                elif count == 1:  # 非连续缺失
                    result.append('mean_fill')
                result.append('valid')  # 有效数据
                count = 0

        # 处理最后一段缺失
        if count > 1:
            result.extend(['backward_fill'] * count)
        elif count == 1:
            result.append('mean_fill')

        # 应用填充逻辑
        method = pd.Series(result, index=group.index)

        # 连续缺失用后向填充
        group.loc[method == 'backward_fill', data_col] = group[data_col].bfill()

        # 非连续缺失用前后值均值填充
        for idx in group[method == 'mean_fill'].index:
            prev_val = group.loc[idx - 1, data_col] if idx - 1 in group.index else np.nan
            next_val = group.loc[idx + 1, data_col] if idx + 1 in group.index else np.nan

            if pd.notna(prev_val) and pd.notna(next_val):
                group.loc[idx, data_col] = (prev_val + next_val) / 2  # 取前后均值
            elif pd.notna(prev_val):
                group.loc[idx, data_col] = prev_val  # 只有前值
            elif pd.notna(next_val):
                group.loc[idx, data_col] = next_val  # 只有后值

        return group

    # 按站点分组处理
    processed_df = process_df.groupby(station_column, group_keys=False).parallel_apply(process_group)
    # 合并处理后的数据和未处理的数据
    if stations is not None:
        unprocessed_df = df[~df[station_column].isin(stations)]
        df = pd.concat([processed_df, unprocessed_df]).sort_index()
    else:
        df = processed_df

    # 返回结果
    return df

def fill_missing_values(df,
                        data_col='f8',
                        year_col='f5',
                        month_col='f6',
                        day_col='f7',
                        method='linear'
                        ):
    """
    通过判断缺测值是否为连续缺测，如果连续缺测则由有值一天往前填充，如果不是则通过线性插值来实现插值。
    """
    # 创建日期列
    # print(year_col, month_col, day_col)
    df.loc[:, ["time"]] = pd.to_datetime(
        df[year_col].astype(str)
        + df[month_col].astype(str).str.zfill(2)
        + df[day_col].astype(str).str.zfill(2)
    )

    # 按日期升序排序并重置索引
    df = df.sort_values(by='time').reset_index(drop=True)
    is_nan = df[data_col].isna().tolist()
    result = []
    count = 0

    # 遍历数据列，判断连续缺失的长度
    for i in range(len(df)):
        if is_nan[i]:
            count += 1
        else:
            if count > 1:  # 连续缺失长度大于 1
                result.extend(['backward_fill'] * count)
            else:
                result.extend(['linear_interpolate'] * count)
            result.append('valid')  # 有效数据
            count = 0

    # 处理最后一段缺失情况
    if count > 1:
        result.extend(['backward_fill'] * count)
    elif count == 1:
        result.append('linear_interpolate')

    # 应用填充逻辑
    fill_method = pd.Series(result, index=df.index)
    df.loc[:, data_col] = np.where(fill_method == 'backward_fill', df[data_col].bfill(), df[data_col])  # 后向填充
    df.loc[:, data_col] = np.where(fill_method == 'linear_interpolate', df[data_col].interpolate(method=method),
                                   df[data_col])  # 线性插值

    return df
