# PeopleQuant
上期CTP的Python版本，Small Beautiful Python SDK

综合交易平台（Comprehensive Transaction Platform， CTP）是专门为期货公司开发的一套期货经纪业务管理系统，由交易、风险控制和结算三大系统组成。
本项目借鉴openctp编译的Python版CTP接口。

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

thostmduserapi.py包含了行情接口，thosttraderapi.py包含了交易接口，无论是行情接口和交易接口，都包含两个类：Api和Spi。
Api：包含主动发起请求和订阅的接口函数，例如：下单、查询持仓、订阅行情等。
Spi：包含所有的响应和回报函数，用于接收CTP发送，或交易所发送再由CTP转发的信息，利润：下单回报、成交回报、行情推送。

行情接口推送的没有历史数据，只有实时行情，行情的主要作用是计算持仓的盈亏。
交易接口用于查询持仓、查询合约、下单、收取查询到的持仓数据等，因此交易接口的作用是发出指令，并收取包含了业务数据的回报。
在什么条件下需要发出指令呢？可以是行情触发的，如根据行情计算的持仓浮盈达到了止损止盈，可以是基本面消息事件触发的，也可能是其他因素触发的，即便是行情触发的也不一定来自CTP的行情接口，可以是其他来源的行情。
因此，在设计框架时，可设计为三个层面，交易接口作为CTP底层，用于接收上层的信号，行情接口平行于交易接口，可用于计算持仓盈亏，但不是必须的；中间层为策略层，做仓位管理、向CTP底层发送指令、从CTP底层收取业务数据、记录交易结果以作绩效分析；最上层是数据分析层，例如技术分析、机器学习、因子挖掘等，还可以做终端界面。
CTP底层接收指令和从交易所收取业务数据回报，要求运行速度很快，不应有复杂的运算；策略层做仓位计算、数据整理、下发指令，只做简单的计算，也要求速度较快；数据分析层运算量最大，较大的运算可能需要较高的硬件配置，三个层面不能相互影响运行速度，所以应分处三个进程中，后续的Python版本可以并行运行多线程，因此后续可以将三个层面运行在三个线程中。
框架的层次结构图如下：
<img width="649" height="789" alt="3FFC60F5F890D0FDBC2B8E0B2F937DE9" src="https://github.com/user-attachments/assets/5c9bdc43-a3b7-483f-a884-f1939872470b" />
<br>本项目的开发需要一定的时间，并需要持续的迭代和完善，喜欢本项目的话可以先加星关注，一起见证项目的成长，并为您的交易带来便利。


