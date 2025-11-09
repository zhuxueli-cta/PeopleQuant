#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)  # 插入到最前面，优先搜索
