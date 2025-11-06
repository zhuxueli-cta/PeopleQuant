# PeopleQuant
CTP接口的Python版，跨平台的交易接口，精而美的Python SDK。  
CTP接口是连接交易柜台的门户，本项目力图解决交易前的风控、交易后的数据反馈、各种交易场景下的数据查询，解决交易上的“最后一公里问题”，并能嵌入其他交易平台，借助其他平台的优势，以及结合Python支持的科学计算、机器学习、AI等现代技术，开发各种高效策略。

```
ThostFtdcTraderApi.h C++头文件，包含交易相关的指令，如报单。
ThostFtdcMdApi.h C++头文件，包含获取行情相关的指令。
ThostFtdcUserApiStruct.h 包含了所有用到的数据结构。
ThostFtdcUserApiDataType.h 包含了所有用到的数据类型。
thosttraderapi_se.dll 交易部分的动态链接库。
thosttraderapi_se.lib 交易部分的静态链接库。
thostmduserapi_se.dll 行情部分的动态链接库。
thostmduserapi_se.lib行情部分的静态链接库。
error.dtd 包含所有可能的错误信息。
error.xml包含所有可能的错误信息
```

本项目基于openctp编译的Python版CTP接口开发。  
CTP原生接口的交易接口和行情接口编译成Python版接口后形成了如下文件列表：  
```
_thostmduserapi.pyd
_thosttraderapi.pyd
thostmduserapi.py
thostmduserapi_se.dll
thosttraderapi.py
thosttraderapi_se.dll
``` 
本项目采用openctp项目中编译好的Python接口。接口地址：[openctp接口文件](http://www.openctp.cn/CTPAPI-Python.html)  

框架的层次结构图如下：  
<img width="649" height="789" alt="3FFC60F5F890D0FDBC2B8E0B2F937DE9" src="https://github.com/user-attachments/assets/5c9bdc43-a3b7-483f-a884-f1939872470b" />

CTP框架的设计思路：  
一、 流控  
    客户端主动发起的指令，如果频率过高，可能对CTP柜台造成压力，所以CTP对指令设了流量控制，流控分为4类：  
  1、 查询流：  
      在途1笔，在途的含义是指令发出到响应收完之前（bIsLast为False），只有响应收完（bIsLast为True），才可以发送下一笔查询。但1秒只能查询1次，时间上也做了频率流控。  
  2、 报单/撤单流：  
      报单/撤单设置频率流控，一般是1秒6笔，报单和撤单分开算，且不同合约分开算。  
  3、 FTD报文流控：  
      报文流控为综合流控，包括所有的指令（登录、查询、报单等），一般是1秒6笔。  
  4、 会话数流控：  
      一个登录连接称为一个会话session，一般会话数为6，即只允许账户同时登录6个会话，包括手机、电脑端的程序。同时登录账户数不超过6，超过次数的登录会被拒绝。  
  
    查询流和FTD报文流控是会话session级别的，表示一个会话的最大流量，可以通过增加登录会话突破流控，但又会受最大会话数限制。  
    报单/撤单流控是投资者级别的，对所有的会话的报撤单做汇总流控，只能通过分账户突破该流控。根据最新的程序化交易管理规定，“在1个交易日内出现10次（含）以上1秒钟内10笔（含）以上报撤单的，具有实际控制关系的账户按照一个账户管理”，可能要被认定高频交易，除了需要报备，还可能取消手续费返还。  
    查询流在CTP本端流控，超过流控时会被直接拒绝，不会通过网络发出，其他指令则是发向柜台后被流控，指令发向柜台后如果超过了流控，会进入排队，不会被直接拒绝，但若频率过高总是超过柜台流控，可能会被期货公司拒绝接入，所以最好也在本地做好这些流控。  
    接口参数中默认流控为：_FTDmaxsize={"Session":6, "ReqQry":1,"OrderInsert":6,"OrderAction":6}  
    在CTP6.7.9及以上版本中可以查询到FTD和查询流控，后台会根据查询到的数据更新Session和ReqQry  

二、 持仓、资金  
    策略层若没有其他持仓数据的接口来源，只能通过CTP底层读取，则需要跟随行情实时计算，跟随委托单、成交单实时计算。  

三、 事前风控  
    策略层不能拿到持仓数据做风控，就需要在CTP底层做风控，对报单参数检查，价格是最小变动价位整数，对可用资金检查，等等。  

四、 事后反馈  
    结果记录日志，并向策略层反馈。  

五、 多线程/多进程  
    CTP底层、策略层、数据分析层采用多线程或多进程设计，各层分工，避免相互阻塞。  

六、 业务数据及业务函数阻塞逻辑  
    PeopleQuant中的业务数据为持仓、委托单、成交单、账户资金、行情快照，均为字典格式，CTP推送业务数据时，后台会完成数据的更新，字典是可变数据类型，业务函数返回的数据会自动跟着更新，因此无需重复使用业务函数获取数据。  
    PeopleQuant中的业务函数设计为阻塞模式，即调用业务函数向CTP查询数据时，需要等待结果反馈，若查询成功，返回值会得到业务数据，若查询失败，则返回None或缺省值。  
    调用业务函数时，首先会从内存中查询是否已存在数据，例如合约的行情已经订阅，就直接从内存中取，无需重复订阅，持仓数据已在内存中更新，就直接返回相应持仓，无需重复向CTP查询持仓，这会大大增加执行效率，因为向CTP查询有1秒1次的查询流控，频繁查询会造成CTP更新滞后。  
    调用下单函数时，也会得到下单成功发出或失败的结果，下单成功发出时会返回委托单数据，下单失败会返回None，下单发到交易柜台再从柜台收到回报需要时间，这会增加执行耗时，对高频交易有影响，但及时获取到下单成败的结果，可以避免持续错误下单。  
    下单发向交易柜台之前，也会在本端对价格、可用手数、可用资金做下事前风控判断，避免错误的参数发向柜台。  

目前本项目完成了CTP底层的基础开发，实现了交易所需的基本功能，可嵌入到其他交易平台中，利用其他平台计算的交易信号，调用本接口交易。  
项目文件：  

```
_thostmduserapi.pyd
_thosttraderapi.pyd
thostmduserapi.py
thostmduserapi_se.dll
thosttraderapi.py
thosttraderapi_se.dll
__init__.py    #包标识
ctpapi.cp38-win_amd64.pyd   #ctp接口编译文件
trade_mdforopenctp.py      #实际应用接口文件
zhuchannel.py              #接口所需的队列、线程类
zhustruct.py               #业务数据结构和K线生成类
example.py                 #策略示例
6.7.11_API接口说明.chm     #CTP接口说明
```

本项目推出两个Python版：旧版Python3.8，支持win7以上系统；最新版Python3.13，支持Win10以上系统。  
对应两个CTP版本：6.7.2；最新版6.7.11。  

#### 安装教程

1.  可从本项目仓库中下载所需版本。  
2.  从本项目下载ctpapi.cp38-win_amd64.pyd(ctpapi.cp313-win_amd64.pyd)、trade_mdforopenctp.py、zhuchannel.py、example.py，CTP接口文件从openctp官网下载。  
下载的所有文件放在一个文件夹即可
#### 使用说明

1.  字段说明：  
    CTP接口中已有的字段保持原字段使用，CTP中的字段一般是大写字母开头（如InstrumentID表示合约代码），项目中新增的字段，例如持仓浮动盈亏（float_profit_long），采用小写字母开头，以做区分。  
    CTP接口的字段含义可从API接口说明.chm和.h头文件中查询。  

2.  业务数据说明：  
    持仓、行情、账户资金、委托单、成交单为字典格式，CTP底层推送数据时自动更新这些业务数据，使用时无需重复调用查询函数查询。  
    
3.  实例化： 
 
```
if __name__ == '__main__':
    pqapi = PeopleQuantApi(BrokerID,UserID,PassWord,AppID, AuthCode,TradeFrontAddr,MdFrontAddr,s) #实例化
    account = pqapi.get_account()  #查询账户资金
    position1 = pqapi.get_position("FG601")  #查询持仓
    quote1 = pqapi.get_quote("FG601")    #查询行情
    position2 = pqapi.get_position("rb2601")  #查询持仓
    quote2 = pqapi.get_quote("rb2601")  #查询行情
    #print(position)
    #print(quote)
    #margin1 = p.get_symbol_marginrate("IH2509")  #查询保证金率
    margin2 = p.get_symbol_marginrate("TF2512")  #查询保证金率
    #print(margin1,"IH2509")
    print(margin2,"TF2512")
    UpdateTime = 0  #行情推送时间
    
    while True:
        #业务数据自动更新，无需重复调用查询函数查询
        if UpdateTime != quote1["UpdateTime"]:  #行情时间变动（UpdateTime字段精确到秒值）
            print(quote1["InstrumentID"],quote1["LastPrice"],quote1["UpdateTime"],)
            UpdateTime = quote1["UpdateTime"]
            print(position1 )
            print(quote2["InstrumentID"],quote2["LastPrice"],quote2["UpdateTime"],)
            print(position2 )
            print(account)
```

4.  接口说明

```
get_account #查询账户资金
get_position #查询账户持仓
get_quote    #订阅行情
get_symbol_trade  #查询合约的成交单
get_symbol_order  #查询合约的委托单，活动单、结束单，开仓单、平仓单
get_id_order      #根据委托单编号查询委托单
get_trade_of_order  #查询委托单对应的成交单
get_order_risk      #查询订单风控，报单次数、撤单次数、开仓成交手数、自成交数、信息量、报单成交比
get_symbol_info    #查询合约属性，合约乘数、最小跳、到期日…
query_symbol_option  #查询合约对应的期权，按不同到期日分类，例如同一白糖合约有不同到期日的期权
get_option          #根据标的价格查询某档位的期权
open_close        #实用的开平仓下单及结果处理
"更多功能敬请期待……"
```


