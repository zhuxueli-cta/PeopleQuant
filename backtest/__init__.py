#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
# 每调用一次 os.path.dirname() 就向上一层
libs_dir = os.path.dirname(current_path)  # 上层目录
peoplequant_dir = os.path.dirname(libs_dir)      # 再上层：PeopleQuant 目录
parent_dir = os.path.dirname(peoplequant_dir)    # 再上层：目标父目录
# 将根目录添加到 sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from peoplequant.backtest.backtest import BackTest
from peoplequant.backtest import draw_line
