#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_path, "../../"))
# 将根目录添加到 sys.path
if root_path not in sys.path:
    sys.path.append(root_path)
from trade_mdforopenctp import PeopleQuantApi 
import time as tm
import zhuchannel
import asyncio

import traceback
import types
import polars
from datetime import datetime,time,date,timedelta
import copy
from typing import Dict, List, Optional, Tuple, Any
import json
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QAbstractScrollArea,
                             QHBoxLayout, QTableWidget, QTableWidgetItem, QHeaderView,
                             QPushButton, QComboBox, QMessageBox, QSplitter)
from PySide6.QtCore import Qt, Signal, QTimer, QSettings
os.environ["QUAMASH_QTIMPL"] = "PySide6"
import quamash  #专为 Qt 与 asyncio 整合设计的库,在PySide6之后导入

class ArbitrageTable(QTableWidget):
    def __init__(self, headers, table_id, data_dir, parent=None):
        """
        :param headers: 表格列名列表
        :param table_id: 表格唯一标识（如"first_upper"、"second_lower"，用于区分不同表格的配置）
        :param data_dir: 数据保存目录
        """
        super().__init__(0, len(headers), parent)
        self.task_data = {}
        self.task_thread = {}
        self.headers = headers
        self.table_id = table_id  # 唯一标识，避免不同表格的配置冲突
        #print(self.headers)
        self.data_dir = data_dir  # 列宽数据保存目录
        self.verticalHeader().setVisible(False)
        self.setHorizontalHeaderLabels(self.headers)  # 关键代码，将列名显示到表格
        
        # 启用水平滚动条（解决列隐藏问题）
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContentsOnFirstShow)
        
        # 列宽配置：最小宽度（按列名定制）
        self.min_widths = {
            "多腿": 80, "空腿": 80, "触发条件": 120, "份数": 60,
            "买价差": 80, "卖价差": 80, "已成份数": 80, "成交价差": 80,
            "持仓价差": 100, "止损价差": 80, "止盈价差": 80, "浮盈": 80, "启用": 60
        }
        
        # 初始化列宽和信号
        self._init_columns()
        # 加载保存的列宽（启动时恢复）
        self.load_column_widths()

    def _init_columns(self):
        """初始化列宽调节功能"""
        # 允许手动拉伸列宽
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        # 首次显示时按最小宽度初始化
        for i in range(self.columnCount()):
            col_name = self.headers[i]
            self.setColumnWidth(i, self.min_widths.get(col_name, 60))
        # 监听列宽变化：记录调整后的宽度
        self.horizontalHeader().sectionResized.connect(self._on_column_resized)

    def _on_column_resized(self, col_idx, old_width, new_width):
        """列宽变化时，确保不小于最小宽度并保存"""
        col_name = self.headers[col_idx]
        min_w = self.min_widths.get(col_name, 60)
        # 确保列宽不小于最小宽度
        if new_width < min_w:
            self.setColumnWidth(col_idx, min_w)
            new_width = min_w
        # 保存调整后的列宽
        self.save_column_widths()

    def _get_config_path(self):
        """获取列宽配置文件路径"""
        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        # 文件名格式：{table_id}_column_widths.json
        return os.path.join(self.data_dir, f"{self.table_id}_column_widths.json")

    def save_column_widths(self):
        """保存当前列宽到本地文件"""
        try:
            # 记录每列的索引和宽度（按索引保存，避免列名重复问题）
            width_data = {
                "table_id": self.table_id,
                "widths": [self.columnWidth(i) for i in range(self.columnCount())]
            }
            # 写入JSON文件
            with open(self._get_config_path(), 'w', encoding='utf-8') as f:
                json.dump(width_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存列宽失败：{e}")

    def load_column_widths(self):
        """从本地文件加载列宽并应用"""
        try:
            config_path = self._get_config_path()
            if not os.path.exists(config_path):
                return  # 无配置文件则不处理
            # 读取JSON数据
            with open(config_path, 'r', encoding='utf-8') as f:
                width_data = json.load(f)
            # 验证表格标识是否匹配（避免加载其他表格的配置）
            if width_data.get("table_id") != self.table_id:
                return
            # 应用列宽（确保索引和数量匹配）
            saved_widths = width_data.get("widths", [])
            for i in range(min(len(saved_widths), self.columnCount())):
                # 确保加载的宽度不小于最小宽度
                col_name = self.headers[i]
                min_w = self.min_widths.get(col_name, 60)
                self.setColumnWidth(i, max(saved_widths[i], min_w))
        except Exception as e:
            print(f"加载列宽失败：{e}")

    def add_empty_row(self):
        """添加空行"""
        row = self.rowCount()
        self.insertRow(row)
        for col in range(self.columnCount()):
            item = QTableWidgetItem("")
            item.setFlags(item.flags() | Qt.ItemIsEditable)
            self.setItem(row, col, item)
        return row
        
    def get_selected_row(self):
        """获取选中的行"""
        selected = self.selectedItems()
        if not selected:
            return -1
        return selected[0].row()
        
    def delete_selected_row(self):
        """删除选中行"""
        row = self.get_selected_row()
        if row >= 0:
            if hasattr(self, "task_data") :
                data = self.get_row_data(row)
                if data[0]+data[1] in self.task_data: self.task_data.pop(data[0]+data[1])
            self.removeRow(row)
            return True
        return False
        
    def clear_all(self):
        """清空表格"""
        if hasattr(self, "task_data") :
            for row in range(self.rowCount()):
                data = self.get_row_data(row)
                if data[0]+data[1] in self.task_data: self.task_data.pop(data[0]+data[1])
        self.setRowCount(0)
        
    def get_row_data(self, row):
        """获取行数据"""
        if 0 <= row < self.rowCount():
            return [self.item(row, col).text() for col in range(self.columnCount())]
        return None
        
    def set_row_data(self, row, data):
        """设置行数据"""
        for col, value in enumerate(data):
            if col < self.columnCount():
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.setItem(row, col, item)

class ButtonGroup(QWidget):
    """按钮组组件"""
    def __init__(self, button_names, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.buttons = {}
        
        for name in button_names:
            btn = QPushButton(name)
            self.buttons[name] = btn
            layout.addWidget(btn)
        
        layout.addStretch()

class FirstWindow(QMainWindow):
    """第一个窗口"""
    closed = Signal()
    
    def __init__(self,pqapi:PeopleQuantApi, loop, data_path, config_dir, parent=None):
        super().__init__(parent)
        self.loop = loop
        self.pqapi = pqapi
        self.data_path = data_path
        self.config_dir = config_dir  # 列宽配置保存目录
        #self.task_data = {}
        self.setWindowTitle("套利策略设置")
        self.init_ui()
        self.load_data()
        
    def init_ui(self):
        # 上部分表格：指定唯一标识 table_id="first_upper"
        upper_headers = ["多腿", "空腿", "触发条件", "份数"]
        self.upper_table = ArbitrageTable(
            headers=upper_headers,
            table_id="first_upper",  # 唯一标识
            data_dir=self.config_dir  # 配置保存目录
        )

        # 上部分按钮
        upper_buttons = ["增加", "删除", "清空", "启用"]
        self.upper_btn_group = ButtonGroup(upper_buttons)
        
        
        # 上部分布局
        upper_layout = QHBoxLayout()
        # 添加表格时设置拉伸因子为1（表示表格区域随窗口放大而拉伸）
        upper_layout.addWidget(self.upper_table, stretch=1)
        # 按钮组固定宽度，不拉伸（设置stretch=0，或通过setFixedWidth固定宽度）
        self.upper_btn_group.setFixedWidth(100)  # 固定按钮组宽度（可选，避免按钮被拉伸）
        upper_layout.addWidget(self.upper_btn_group, stretch=0)
        
        upper_container = QWidget()
        upper_container.setLayout(upper_layout)
        
        # 下部分表格
        lower_headers = ["多腿", "空腿", "触发条件", "剩余份数", 
                        "对价差", "最新价差", "成交价差", "启用"]
        self.lower_table = ArbitrageTable(
            headers=lower_headers,
            table_id="first_window_lower",  # 唯一标识
            data_dir=self.config_dir  # 列宽配置保存目录
        )
        #self.lower_table.task_data = self.task_data
        # 下部分按钮
        lower_buttons = ["启用", "暂停", "删除", "清空"]
        self.lower_btn_group = ButtonGroup(lower_buttons)
        
        # 下部分布局
        lower_layout = QHBoxLayout()
        lower_layout.addWidget(self.lower_table, stretch=1)  # 表格拉伸
        self.lower_btn_group.setFixedWidth(100)  # 按钮组固定宽度
        lower_layout.addWidget(self.lower_btn_group, stretch=0)
        
        lower_container = QWidget()
        lower_container.setLayout(lower_layout)
        
        # 主布局
        main_layout = QVBoxLayout()
        main_layout.addWidget(upper_container)
        main_layout.addWidget(lower_container)
        
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
        
        # 连接信号槽
        self.connect_signals()
        
    def connect_signals(self):
        # 上部分按钮
        self.upper_btn_group.buttons["增加"].clicked.connect(
            self.upper_table.add_empty_row)
        self.upper_btn_group.buttons["删除"].clicked.connect(
            self.upper_table.delete_selected_row)
        self.upper_btn_group.buttons["清空"].clicked.connect(
            self.upper_table.clear_all)
        self.upper_btn_group.buttons["启用"].clicked.connect(
            self.move_to_lower_table)
            
        # 下部分按钮
        self.lower_btn_group.buttons["启用"].clicked.connect(
            lambda: self.update_status("启用"))
        self.lower_btn_group.buttons["暂停"].clicked.connect(
            lambda: self.update_status("暂停"))
        self.lower_btn_group.buttons["删除"].clicked.connect(
            self.lower_table.delete_selected_row)
        self.lower_btn_group.buttons["清空"].clicked.connect(
            self.lower_table.clear_all)
    
    def move_to_lower_table(self):
        """将上部分选中行移动到下部分"""
        row = self.upper_table.get_selected_row()
        if row >= 0:
            data = self.upper_table.get_row_data(row)
            if not all(data) or data[0]+data[1] in self.lower_table.task_data: return
            # 下部分表格额外字段填充默认值
            data.extend(["", "", "", "未启用"])
            self.lower_table.task_data[data[0]+data[1]] = data
            new_row = self.lower_table.rowCount()
            self.lower_table.insertRow(new_row)
            self.lower_table.set_row_data(new_row, data)
            task = self.loop.create_task(self.cta(self.lower_table.task_data[data[0]+data[1]],self.pqapi))
    
    def update_status(self, status):
        """更新状态列"""
        row = self.lower_table.get_selected_row()
        if row >= 0 and self.lower_table.columnCount() >= 8:
            item = QTableWidgetItem(status)
            self.lower_table.setItem(row, 7, item)
            data = self.lower_table.get_row_data(row)
            self.lower_table.task_data[data[0]+data[1]][:] = data
    
    def load_data(self):
        """加载数据"""
        try:
            if os.path.exists(self.data_path):
                with open(self.data_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 加载上部分表格数据
                    for row_data in data.get('upper_table', []):
                        row = self.upper_table.rowCount()
                        self.upper_table.insertRow(row)
                        self.upper_table.set_row_data(row, row_data)
                    # 加载下部分表格数据
                    for row_data in data.get('lower_table', []):
                        if not row_data or not all(row_data): continue
                        if row_data:
                            if row_data[0] and row_data[1] and row_data[0]+row_data[1] not in self.lower_table.task_data: 
                                self.lower_table.task_data[row_data[0]+row_data[1]] = row_data
                        row = self.lower_table.rowCount()
                        self.lower_table.insertRow(row)
                        self.lower_table.set_row_data(row, row_data)
                        task = self.loop.create_task(self.cta(row_data,self.pqapi))
        except Exception as e:
            print(f"加载数据失败: {e}")
    
    def save_data(self):
        """保存数据"""
        try:
            upper_data = []
            for row in range(self.upper_table.rowCount()):
                upper_data.append(self.upper_table.get_row_data(row))
                
            lower_data = []
            for row in range(self.lower_table.rowCount()):
                lower_data.append(self.lower_table.get_row_data(row))
                
            data = {
                'upper_table': upper_data,
                'lower_table': lower_data
            }
            
            with open(self.data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存数据失败: {e}")
    
    def closeEvent(self, event):
        self.save_data()
        self.closed.emit()
        event.accept()
    

    async def cta(self,data:List[str],pqapi:PeopleQuantApi):
        lower_headers = ["多腿", "空腿", "触发条件", "剩余份数", 
                        "对价差", "最新价差", "成交价差", "启用"]
        long_quote,long_position,short_quote,short_position = {},{},{},{}
        long_lots,short_lots,long_symbol_info,short_symbol_info = {},{},{},{}
        for i in data[0].split('+') :
            if '*' in i:
                lot,symbol = i.split('*')
                long_lots[symbol] = int(lot)  
            else: 
                symbol = i
                long_lots[i] = 1
            long_quote[symbol] = await self.loop.create_task(pqapi.async_wrapper(self.loop,pqapi.get_quote,symbol ))
            long_position[symbol] = pqapi.get_position(symbol)
            long_symbol_info[symbol] = pqapi.get_symbol_info(symbol)
        for i in data[1].split('+') :
            if '*' in i:
                lot,symbol = i.split('*')
                short_lots[symbol] = int(lot)  
            else: 
                symbol = i
                short_lots[i] = 1
            short_quote[symbol] = await self.loop.create_task(pqapi.async_wrapper(self.loop,pqapi.get_quote,symbol ))
            short_position[symbol] = pqapi.get_position(symbol)
            short_symbol_info[symbol] = pqapi.get_symbol_info(symbol)
        UpdateTimes = {q.ctp_datetime for q in long_quote.values()} | {q.ctp_datetime for q in short_quote.values()}
        while True:
            if data[0] + data[1] not in self.lower_table.task_data: return
            if UpdateTimes != ({q.ctp_datetime for q in long_quote.values()} | {q.ctp_datetime for q in short_quote.values()}):
                UpdateTimes = {q.ctp_datetime for q in long_quote.values()} | {q.ctp_datetime for q in short_quote.values()}
                long_turnover = sum([long_lots[s]*q.AskPrice1*long_symbol_info[s].VolumeMultiple for s,q in long_quote.items()])
                short_turnover = sum([short_lots[s]*q.BidPrice1*short_symbol_info[s].VolumeMultiple for s,q in short_quote.items()])
                buy_turnover = long_turnover - short_turnover
                last_turnover = sum([long_lots[s]*q.LastPrice*long_symbol_info[s].VolumeMultiple for s,q in long_quote.items()]) - sum([short_lots[s]*q.LastPrice*short_symbol_info[s].VolumeMultiple for s,q in short_quote.items()])
                position_turnover = ""
                if eval(str(buy_turnover)+data[2]) and data[0] + data[1] in self.lower_table.task_data and self.lower_table.task_data[data[0]+data[1]][-1] == "启用":
                    if int(data[3]) > 0:
                        task = []
                        for symbol,quote in long_quote.items():
                            price = quote["UpperLimitPrice"]
                            lot = long_lots[symbol] * int(data[3])
                            task.append(self.loop.create_task(pqapi.OpenClose(self.loop,symbol,"kaiduo",lot,price)))
                        for symbol,quote in short_quote.items():
                            price = quote["LowerLimitPrice"]
                            lot = short_lots[symbol] * int(data[3])
                            task.append(self.loop.create_task(pqapi.OpenClose(self.loop,symbol,"kaikong",lot,price)))    
                        rs = await asyncio.gather(*task)
                        long_turnover,short_turnover = 0,0
                        for r in rs:
                            if r['symbol'] in long_quote:
                                long_turnover += r['shoushu']*r['junjia']*long_symbol_info[r['symbol']].VolumeMultiple 
                            elif r['symbol'] in short_quote:
                                short_turnover += r['shoushu']*r['junjia']*short_symbol_info[r['symbol']].VolumeMultiple 
                        position_turnover = long_turnover - short_turnover
                        for row in range(self.lower_table.rowCount()):
                            row_data = self.lower_table.get_row_data(row)
                            if row_data[0] == data[0] and row_data[1] == data[1]:
                                self.lower_table.item(row, 4).setText(str(buy_turnover))  # 对价差
                                self.lower_table.item(row, 5).setText(str(last_turnover))  # 最新价差
                                self.lower_table.item(row, 3).setText(str(0))  # 成交完
                                self.lower_table.item(row, 6).setText(str(position_turnover))  # 持仓价差
                                data[3] = 0
                if int(data[3]):
                    for row in range(self.lower_table.rowCount()):
                        row_data = self.lower_table.get_row_data(row)
                        if row_data[0] == data[0] and row_data[1] == data[1]:
                            self.lower_table.item(row, 4).setText(str(buy_turnover))  # 买价差
                            self.lower_table.item(row, 5).setText(str(last_turnover))  # 最新价差
            await asyncio.sleep(0)

class SecondWindow(QMainWindow):
    """第二个窗口"""
    closed = Signal()
    
    def __init__(self,pqapi:PeopleQuantApi,loop, data_path, config_dir, parent=None):
        super().__init__(parent)
        self.pqapi = pqapi
        self.loop = loop
        self.data_path = data_path
        self.config_dir = config_dir  # 列宽配置保存目录
        self.setWindowTitle("持仓管理")
        self.init_ui()
        self.load_data()
        
    def init_ui(self):
        # 上部分表格
        upper_headers = ["多腿", "空腿", "持仓价差", "份数", "止损价差", "止盈价差"]
        self.upper_table = ArbitrageTable(
            headers=upper_headers,
            table_id="second_upper",  # 唯一标识
            data_dir=self.config_dir  # 配置保存目录
        )
        
        # 上部分按钮
        upper_buttons = ["增加", "删除", "清空", "启用"]
        self.upper_btn_group = ButtonGroup(upper_buttons)
        
        # 上部分布局
        upper_layout = QHBoxLayout()
        # 添加表格时设置拉伸因子为1（表示表格区域随窗口放大而拉伸）
        upper_layout.addWidget(self.upper_table, stretch=1)
        # 按钮组固定宽度，不拉伸（设置stretch=0，或通过setFixedWidth固定宽度）
        self.upper_btn_group.setFixedWidth(100)  # 固定按钮组宽度（可选，避免按钮被拉伸）
        upper_layout.addWidget(self.upper_btn_group, stretch=0)
        
        upper_container = QWidget()
        upper_container.setLayout(upper_layout)
        
        # 下部分表格
        lower_headers = ["多腿", "空腿", "持仓价差", "剩余份数", 
                        "止损价差", "止盈价差", "卖价差", "最新价差", "浮盈", "启用"]
        self.lower_table = ArbitrageTable(
            headers=lower_headers,
            table_id="second_window_lower",  # 唯一标识
            data_dir=self.config_dir  # 列宽配置保存目录
        )
        
        # 下部分按钮
        lower_buttons = ["启用", "暂停", "删除", "清空"]
        self.lower_btn_group = ButtonGroup(lower_buttons)
        
        # 下部分布局
        lower_layout = QHBoxLayout()
        lower_layout.addWidget(self.lower_table, stretch=1)  # 表格拉伸
        self.lower_btn_group.setFixedWidth(100)  # 按钮组固定宽度
        lower_layout.addWidget(self.lower_btn_group, stretch=0)
        
        lower_container = QWidget()
        lower_container.setLayout(lower_layout)
        
        # 主布局
        main_layout = QVBoxLayout()
        main_layout.addWidget(upper_container)
        main_layout.addWidget(lower_container)
        
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
        
        # 连接信号槽
        self.connect_signals()
        
    def connect_signals(self):
        # 上部分按钮
        self.upper_btn_group.buttons["增加"].clicked.connect(
            self.upper_table.add_empty_row)
        self.upper_btn_group.buttons["删除"].clicked.connect(
            self.upper_table.delete_selected_row)
        self.upper_btn_group.buttons["清空"].clicked.connect(
            self.upper_table.clear_all)
        self.upper_btn_group.buttons["启用"].clicked.connect(
            self.move_to_lower_table)
            
        # 下部分按钮
        self.lower_btn_group.buttons["启用"].clicked.connect(
            lambda: self.update_status("启用"))
        self.lower_btn_group.buttons["暂停"].clicked.connect(
            lambda: self.update_status("暂停"))
        self.lower_btn_group.buttons["删除"].clicked.connect(
            self.lower_table.delete_selected_row)
        self.lower_btn_group.buttons["清空"].clicked.connect(
            self.lower_table.clear_all)
    
    def move_to_lower_table(self):
        """将上部分选中行移动到下部分"""
        row = self.upper_table.get_selected_row()
        if row >= 0:
            data = self.upper_table.get_row_data(row)
            if not all(data) or data[0]+data[1] in self.lower_table.task_data: return
            # 下部分表格额外字段填充默认值
            data.extend([ "", "", "", "未启用"])
            self.lower_table.task_data[data[0]+data[1]] = data
            new_row = self.lower_table.rowCount()
            self.lower_table.insertRow(new_row)
            self.lower_table.set_row_data(new_row, data)
            task = self.loop.create_task(self.cta(self.lower_table.task_data[data[0]+data[1]],self.pqapi))
    
    def update_status(self, status):
        """更新状态列"""
        row = self.lower_table.get_selected_row()
        if row >= 0 and self.lower_table.columnCount() > 9:
            item = QTableWidgetItem(status)
            self.lower_table.setItem(row, 9, item)
            data = self.lower_table.get_row_data(row)
            self.lower_table.task_data[data[0]+data[1]][:] = data
    
    def load_data(self):
        """加载数据"""
        try:
            if os.path.exists(self.data_path):
                with open(self.data_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 加载上部分表格数据
                    for row_data in data.get('upper_table', []):
                        row = self.upper_table.rowCount()
                        self.upper_table.insertRow(row)
                        self.upper_table.set_row_data(row, row_data)
                    # 加载下部分表格数据
                    for row_data in data.get('lower_table', []):
                        if not row_data or not all(row_data): continue
                        if row_data:
                            if row_data[0] and row_data[1] and row_data[0]+row_data[1] not in self.lower_table.task_data: 
                                self.lower_table.task_data[row_data[0]+row_data[1]] = row_data 
                        row = self.lower_table.rowCount()
                        self.lower_table.insertRow(row)
                        self.lower_table.set_row_data(row, row_data)
                        task = self.loop.create_task(self.cta(row_data,self.pqapi))
        except Exception as e:
            print(f"加载数据失败: {e}")
    
    def save_data(self):
        """保存数据"""
        try:
            upper_data = []
            for row in range(self.upper_table.rowCount()):
                upper_data.append(self.upper_table.get_row_data(row))
                
            lower_data = []
            for row in range(self.lower_table.rowCount()):
                lower_data.append(self.lower_table.get_row_data(row))
                
            data = {
                'upper_table': upper_data,
                'lower_table': lower_data
            }
            
            with open(self.data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存数据失败: {e}")
    
    def closeEvent(self, event):
        self.save_data()
        self.closed.emit()
        event.accept()
    
    async def cta(self,data:List[str],pqapi:PeopleQuantApi):
        lower_headers = ["多腿", "空腿", "持仓价差", "剩余份数", 
                        "止损价差", "止盈价差", "卖价差", "最新价差", "浮盈", "启用"]
        long_quote,long_position,short_quote,short_position = {},{},{},{}
        long_lots,short_lots,long_symbol_info,short_symbol_info = {},{},{},{}
        for i in data[0].split('+') :
            if '*' in i:
                lot,symbol = i.split('*')
                long_lots[symbol] = int(lot)  
            else: 
                symbol = i
                long_lots[i] = 1
            long_quote[symbol] = await self.loop.create_task(pqapi.async_wrapper(self.loop,pqapi.get_quote,symbol ))
            long_position[symbol] = pqapi.get_position(symbol)
            long_symbol_info[symbol] = pqapi.get_symbol_info(symbol)
        for i in data[1].split('+') :
            if '*' in i:
                lot,symbol = i.split('*')
                short_lots[symbol] = int(lot)  
            else: 
                symbol = i
                short_lots[i] = 1
            short_quote[symbol] = await self.loop.create_task(pqapi.async_wrapper(self.loop,pqapi.get_quote,symbol ))
            short_position[symbol] = pqapi.get_position(symbol)
            short_symbol_info[symbol] = pqapi.get_symbol_info(symbol)
        UpdateTimes = {q.ctp_datetime for q in long_quote.values()} | {q.ctp_datetime for q in short_quote.values()}
        while True:
            if data[0] + data[1] not in self.lower_table.task_data: return
            if UpdateTimes != ({q.ctp_datetime for q in long_quote.values()} | {q.ctp_datetime for q in short_quote.values()}):
                UpdateTimes = {q.ctp_datetime for q in long_quote.values()} | {q.ctp_datetime for q in short_quote.values()}
                long_turnover = sum([long_lots[s]*q.LastPrice*long_symbol_info[s].VolumeMultiple for s,q in long_quote.items()])
                short_turnover = sum([short_lots[s]*q.LastPrice*short_symbol_info[s].VolumeMultiple for s,q in short_quote.items()])
                last_turnover = long_turnover - short_turnover
                sell_turnover = sum([long_lots[s]*q.BidPrice1*long_symbol_info[s].VolumeMultiple for s,q in long_quote.items()]) - sum([short_lots[s]*q.AskPrice1*short_symbol_info[s].VolumeMultiple for s,q in short_quote.items()])
                position_turnover = ""
                if (sell_turnover <= float(data[4]) or sell_turnover >= float(data[5])) and data[0] + data[1] in self.lower_table.task_data and self.lower_table.task_data[data[0]+data[1]][-1] == "启用":
                    if int(data[3]):
                        task = []
                        for symbol,quote in long_quote.items():
                            price = quote["LowerLimitPrice"]
                            lot = long_lots[symbol] * int(data[3])
                            task.append(self.loop.create_task(pqapi.OpenClose(self.loop,symbol,"pingduo",lot,price)))
                        for symbol,quote in short_quote.items():
                            price = quote["UpperLimitPrice"]
                            lot = short_lots[symbol] * int(data[3])
                            task.append(self.loop.create_task(pqapi.OpenClose(self.loop,symbol,"pingkong",lot,price)))    
                        rs = await asyncio.gather(*task)
                        long_turnover,short_turnover = 0,0
                        for r in rs:
                            if r['symbol'] in long_quote:
                                long_turnover += r['shoushu']*r['junjia']*long_symbol_info[r['symbol']].VolumeMultiple 
                            elif r['symbol'] in short_quote:
                                short_turnover += r['shoushu']*r['junjia']*short_symbol_info[r['symbol']].VolumeMultiple 
                        position_turnover = long_turnover - short_turnover
                        for row in range(self.lower_table.rowCount()):
                            row_data = self.lower_table.get_row_data(row)
                            if row_data[0] == data[0] and row_data[1] == data[1]:
                                self.lower_table.item(row, 7).setText(str(last_turnover))  # 卖价差
                                self.lower_table.item(row, 6).setText(str(sell_turnover))  # 最新价差
                                self.lower_table.item(row, 3).setText(str(0))  # 剩余份数
                                self.lower_table.item(row, 8).setText(str(position_turnover-float(data[2])))  # 平仓盈亏
                                data[3] = 0
                                
                if int(data[3]):
                    for row in range(self.lower_table.rowCount()):
                        row_data = self.lower_table.get_row_data(row)
                        if row_data[0] == data[0] and row_data[1] == data[1]:
                            self.lower_table.item(row, 7).setText(str(last_turnover))  # 买价差
                            self.lower_table.item(row, 6).setText(str(sell_turnover))  # 卖价差
                            self.lower_table.item(row, 8).setText(str(sell_turnover-float(data[2])))  # 卖价差
            await asyncio.sleep(0)

class ArbitrageApp:
    """套利应用主控制器"""
    def __init__(self,pqapi):
        self.pqapi = pqapi
        self.app = QApplication(sys.argv)
        self.loop = quamash.QEventLoop(self.app)  # 核心：共享事件循环
        asyncio.set_event_loop(self.loop)  # 设置为全局默认循环
        
        # 数据保存路径（原有表格数据）
        self.data_dir = os.path.expanduser("~/.arbitrage_data")
        os.makedirs(self.data_dir, exist_ok=True)
        #print(self.data_dir)
        self.first_window_data = os.path.join(self.data_dir, "first_window.json")
        self.second_window_data = os.path.join(self.data_dir, "second_window.json")
        
        # 新增：列宽配置保存目录
        self.config_dir = os.path.expanduser("~/.arbitrage_config")
        os.makedirs(self.config_dir, exist_ok=True)
        
        # 创建窗口时传入配置目录
        self.first_window = FirstWindow(self.pqapi,self.loop,
            self.first_window_data, 
            self.config_dir  # 传入列宽配置目录
        )
        self.second_window = SecondWindow(self.pqapi,self.loop,
            self.second_window_data, 
            self.config_dir  # 传入列宽配置目录
        )
        
        # 窗口联动关闭
        self.first_window.closed.connect(self.close_all)
        self.second_window.closed.connect(self.close_all)
        
        
    def show_windows(self):
        """显示窗口"""
        self.first_window.resize(1000, 600)
        self.second_window.resize(1000, 600)
        self.first_window.show()
        self.second_window.show()
        
    def close_all(self):
        """关闭所有窗口"""
        self.first_window.save_data()
        self.second_window.save_data()
        self.first_window.close()
        self.second_window.close()
        self.loop.stop()  # 停止整合后的事件循环
        self.app.quit()

        
    def run(self):
        """启动应用：运行整合后的事件循环"""
        with self.loop:  # 自动管理循环生命周期
            return self.loop.run_forever()  # 同时处理 Qt 和 asyncio 事件


envs = {
    "7x24": {
        "td": "tcp://182.254.243.31:40001",
        "md": "tcp://182.254.243.31:40011",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "电信1": {
        "td": "tcp://182.254.243.31:30001",
        "md": "tcp://182.254.243.31:30011",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "电信2": {
        "td": "tcp://182.254.243.31:30002",
        "md": "tcp://182.254.243.31:30012",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "移动": {
        "td": "tcp://182.254.243.31:30003",
        "md": "tcp://182.254.243.31:30013",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
}
TradeFrontAddr="tcp://180.168.146.187:10101"   #交易前置地址
MdFrontAddr="tcp://101.230.209.178:53313"      #行情前置地址
TradeFrontAddr = envs["电信2"]["td"]
MdFrontAddr = envs["电信2"]["md"]
TradeFrontAddr = envs["7x24"]["td"]
MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID="9999"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

if __name__ == "__main__":
    pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
    app = ArbitrageApp(pqapi)
    app.show_windows()
    sys.exit(app.run())
