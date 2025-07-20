# PeopleQuant
上期CTP的Python版本，Small Beautiful Python SDK
综合交易平台（Comprehensive Transaction Platform， CTP）是专门为期货公司开发的一套期货经纪业务管理系统，由交易、风险控制和结算三大系统组成。
本项目借鉴openctp编译的Python版CTP接口
CTP原生接口文件列表：
ThostFtdcTraderApi.h  C++头文件，包含交易相关的指令，如报单。
ThostFtdcMdApi.h C++头文件，包含获取行情相关的指令。
ThostFtdcUserApiStruct.h 包含了所有用到的数据结构。
ThostFtdcUserApiDataType.h 包含了所有用到的数据类型。
thosttraderapi.dll 交易部分的动态链接库和静态链接库。
thosttraderapi.lib
thostmduserapi.dll 行情部分的动态链接库和静态链接库。
thostmduserapi.lib
error.dtd 包含所有可能的错误信息。
error.xml
上述接口文件的Python版由如下文件组成：
_thostmduserapi.pyd
_thosttraderapi.pyd
thostmduserapi.py
thostmduserapi_se.dll
thosttraderapi.py
thosttraderapi_se.dll
