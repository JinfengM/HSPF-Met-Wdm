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

from hspf_met import *

pandarallel.initialize(verbose=0)

if __name__ == '__main__':
    #数据信息
    target_stations = ['59843', '59848', '59851', '59854']
    stns_2_letdeg = {'59843': 19.7333, '59848': 19.2333, '59851': 19.7, '59854': 19.3666}
    invalid_value = 32766
    wdmpath = "data/hspf_met_simple_test.wdm"
    tempfile = "data/temp.csv"
    windfile = "data/win.csv"
    ssdfile = "data/ssd.csv"
    rhumfile = "data/rhu.csv"
    radifile = "data/radi.csv"
    precipfile = "data/pre.csv"

    print("开始测试修复后的程序（仅日数据）...")
    
    try:
        # ============================逐日数据==================================
        # DSN:19.最大温度
        print("处理最大温度数据...")
        temp_df = metTmax(inputfile=tempfile, data_col='f10', stations=target_stations, invalid_value=invalid_value, wdmpath=wdmpath)
        print("✓ 最大温度处理成功")

        # DSN:20.最小温度
        print("处理最小温度数据...")
        metTmin(inputfile=tempfile, data_col='f10', stations=target_stations, invalid_value=invalid_value, wdmpath=wdmpath)
        print("✓ 最小温度处理成功")

        # DSN:21.风速
        print("处理风速数据...")
        metDailyWind(inputfile=windfile, data_col='f8', stations=target_stations, invalid_value=invalid_value, wdmpath=wdmpath)
        print("✓ 风速数据处理成功")

        # DSN:22.云量
        print("处理云量数据...")
        metDailyCloud(inputfile=ssdfile, data_col='f8', stations=target_stations, invalid_value=invalid_value,
                      wdmpath=wdmpath)
        print("✓ 云量数据处理成功")
        
        # DSN:23.露点温度
        print("处理露点温度数据...")
        metDailyDewpointTemperature(atem_file=tempfile, atem_col='f8', rhum_file=rhumfile, rhum_col='f8', wdmpath=wdmpath,
                                    stations=target_stations, invalid_value=invalid_value, scale=0.1)
        print("✓ 露点温度数据处理成功")
        
        # DSN:24.DSOL太阳辐射
        print("处理太阳辐射数据...")
        metDailySolar(inputfile=radifile, wdmpath=wdmpath, data_col='f8', stations=target_stations, invalid_value=999998, scale=0.01)
        print("✓ 太阳辐射数据处理成功")

        # DSN:25.DEVT
        print("处理蒸散数据...")
        metDailyEvapotranspiration(wdmpath=wdmpath, station_2_latDeg=stns_2_letdeg, stations=target_stations)
        print("✓ 蒸散数据处理成功")

        # DSN:26.DEVP
        print("处理蒸发数据...")
        metDailyEvaporation(wdmpath=wdmpath, stations=target_stations)
        print("✓ 蒸发数据处理成功")

        print("\n🎉 所有日数据测试通过！程序修复成功！")
        print(f"生成的WDM文件: {wdmpath}")
        
        # 查看生成的DSN
        dsns = wdm.listdsns(wdmpath)
        print(f"生成的DSN数量: {len(dsns)}")
        print(f"DSN列表: {sorted(dsns)}")
        
        # 显示每个DSN的信息
        wdmts = wdmutil.WDM()
        print("\nDSN详细信息:")
        for dsn in sorted(dsns):
            attrs = wdmts.describe_dsn(wdmpath, dsn)
            print(f"DSN {dsn}: {attrs['TSTYPE']} - 站点 {attrs['IDLOCN']}")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc() 