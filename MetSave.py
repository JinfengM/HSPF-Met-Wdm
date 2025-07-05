import os
import tempfile
from typing import Union
import pandas as pd
import numpy as np
import wdmtoolbox as wdm
from wdmtoolbox import wdmutil


def SaveDataToWdm(aDyTSer: pd.DataFrame, column_name: str, dsn: int, wdmpath: str, location: str, tcode: int,
                  scenario: str = 'OBSERVED', description: str = None):
    """
    保存逐日数据到WDM文件 - 修复了pandas 2.x兼容性问题
    :param aDyTSer: 逐日数据
    :param columnname: 逐日数据列名，列名参考Basins参考文档
    :param dsn: id编号，参考Basins参考文档
    :param wdmpath: WDM的文件路径
    :param location: 数据位置
    :param scenario: 场景
    :param description: 数据描述
    :return:
    """
    print("wdmpath:", wdmpath)
    if not os.path.exists(wdmpath):
        wdm.createnewwdm(wdmpath, overwrite=True)
    
    # 验证数据格式
    if len(aDyTSer.columns) != 1:
        raise ValueError(f"数据必须只包含一列，当前有{len(aDyTSer.columns)}列")
    
    # 确保索引是DatetimeIndex
    if not isinstance(aDyTSer.index, pd.DatetimeIndex):
        raise TypeError("索引必须是时间类型 (DatetimeIndex)，但当前索引类型为: {}".format(type(aDyTSer.index)))
    
    # 清理数据 - 移除NaN和无穷大值
    data_to_save = aDyTSer.copy()
    
    # 检查并处理数值列中的无穷大值
    numeric_cols = data_to_save.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        # 替换无穷大值为NaN
        data_to_save = data_to_save.replace([np.inf, -np.inf], np.nan)
    
    # 移除包含NaN的行
    data_to_save = data_to_save.dropna()
    
    if data_to_save.empty:
        print("错误：清理后的数据为空")
        print("原始数据统计信息:")
        print(aDyTSer.describe())
        raise ValueError("清理后的数据为空，无法保存到WDM文件")
    
    # 重命名列为指定的column_name
    data_to_save.columns = [column_name]
    
    if dsn in wdm.listdsns(wdmpath):
        wdm.deletedsn(wdmpath, dsn=dsn)
    
    wdm.createnewdsn(wdmpath,
                     dsn=dsn,
                     tstype=column_name,
                     tcode=tcode,
                     scenario=scenario,
                     location=location,
                     constituent=column_name,
                     description=description)
    
    try:
        # 尝试直接使用wdmtoolbox
        wdm.csvtowdm(
            wdmpath=wdmpath,
            dsn=dsn,
            columns=[column_name],
            input_ts=data_to_save,
        )
    except Exception as e:
        # 如果直接调用失败，尝试通过临时CSV文件的方式
        print(f"直接保存失败，尝试通过临时文件: {e}")
        try:
            # 创建临时CSV文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                # 保存为CSV格式，包含时间索引
                data_to_save.to_csv(temp_file.name, index_label='datetime')
                temp_csv_path = temp_file.name
            
            # 使用临时CSV文件
            wdm.csvtowdm(
                wdmpath=wdmpath,
                dsn=dsn,
                columns=[column_name],
                input_ts=temp_csv_path,
            )
            
            # 清理临时文件
            os.unlink(temp_csv_path)
            print("通过临时文件成功保存数据")
            
        except Exception as e2:
            # 如果临时文件方法也失败，抛出原始错误
            raise Exception(f"保存到WDM失败。直接保存错误: {e}。临时文件保存错误: {e2}")


def saveData(aDyTSer: pd.DataFrame, column_name: str, wdmpath: str, scenario: str, location: str, tcode: int,
             description: str, dsn_range: range, dsn=None):
    # 分配 DSN
    if dsn is None:
        available_dsns = set(dsn_range) - set(wdm.listdsns(wdmpath))
        if available_dsns:
            dsn = min(available_dsns)
        else:
            raise ValueError("没有可用的 DSN 分配")
    else:
        if dsn not in dsn_range:
            raise ValueError(f"当前DSN编号{dsn}不是有效编号，编号要是{dsn_range}其中之一")
    # 保存数据
    SaveDataToWdm(
        aDyTSer=aDyTSer,
        column_name=column_name,
        dsn=dsn,
        wdmpath=wdmpath,
        location=location,
        tcode=tcode,
        scenario=scenario,
        description=description
    )


# --------------------------保存逐日数据-------------------------------
def saveDailyTmax(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="TMAX",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily maximum temperature",
        dsn_range=range(19, 200, 20),
        dsn=dsn
    )


def saveDailyTmin(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="TMIN",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily minimum temperature",
        dsn_range=range(20, 201, 20),
        dsn=dsn
    )


def saveDailyWIND(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DWND",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily windspeed",
        dsn_range=range(21, 202, 20),
        dsn=dsn
    )


def saveDailyDCLO(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DCLO",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily cloud cover",
        dsn_range=range(22, 203, 20),
        dsn=dsn
    )


def saveDailyDPTP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DPTP",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily dewpoint temperature",
        dsn_range=range(23, 204, 20),
        dsn=dsn
    )


def saveDailyDPTP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DPTP",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily dewpoint temperature",
        dsn_range=range(23, 204, 20),
        dsn=dsn
    )


def saveDailyDSOL(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DSOL",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily solar radiation",
        dsn_range=range(24, 205, 20),
        dsn=dsn
    )


def saveDailyDEVT(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DEVT",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily evapotranspiration",
        dsn_range=range(25, 206, 20),
        dsn=dsn
    )


def saveDailyDEVP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DEVP",
        wdmpath=wdmpath,
        scenario='OBSERVED',
        location=location,
        tcode=4,
        description="daily evaporation",
        dsn_range=range(26, 207, 20),
        dsn=dsn
    )


# --------------------------保存逐小时数据-------------------------------

def saveHourlyPREC(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="PREC",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="Hourly Precipitation disaggregated from Daily",
        dsn_range=range(11, 192, 20),
        dsn=dsn
    )


def saveHourlyEVAP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="EVAP",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly evaporation",
        dsn_range=range(12, 193, 20),
        dsn=dsn
    )


def saveHourlyATEM(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="ATEM",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly temperature",
        dsn_range=range(13, 194, 20),
        dsn=dsn
    )


def saveHourlyWIND(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="WIND",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly windspeed",
        dsn_range=range(14, 195, 20),
        dsn=dsn
    )


def saveHourlySOLR(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="SOLR",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly solar radiation",
        dsn_range=range(15, 196, 20),
        dsn=dsn
    )


def saveHourlyPEVT(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="PEVT",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly potential evapotranspiration",
        dsn_range=range(16, 197, 20),
        dsn=dsn
    )


def saveHourlyDEWP(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="DEWP",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly dewpoint temperature",
        dsn_range=range(17, 198, 20),
        dsn=dsn
    )


def saveHourlyCLOU(aDyTSer: pd.DataFrame, wdmpath: str, location: str, dsn=None):
    saveData(
        aDyTSer=aDyTSer,
        column_name="CLOU",
        wdmpath=wdmpath,
        scenario='COMPUTED',
        location=location,
        tcode=3,
        description="hourly cloud cover",
        dsn_range=range(18, 199, 20),
        dsn=dsn
    )
