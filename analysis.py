#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import configparser
import math
import copy
import logging
import logging.config
from mongo_util import Mongo

THRESHOLD = None # 参数：阈值
ALL_DATA_CACHE = {}
MONGO = None # mongo连接
VOLUME = [] # 指定个股历史交易量数据
CLOSE = [] # 指定个股历史成交价数据

def handle_time(func):
    ''' 修饰器:计算函数运行所花的时间 '''
    def wrapper(*args, **kwargs):
        ''' wrapper '''
        s_time = time.time()
        _func = func(*args, **kwargs)
        e_time = time.time()
        logging.info('use time : %f', (e_time - s_time))
        return _func
    return wrapper

def html_single_stock(symbol):
    ''' 返回html格式化后的指定个股 \
        历史出现的地量日期、涨跌幅的信息
        @symbol: 指定个股代码
    '''
    _html = []
    _html.append('<h3>********************************* \
                        *********************Result****************** \
                        *****************************************</h3>')
    _html.append('<p>&nbsp&nbsp&nbsp地量日期&nbsp&nbsp&nbsp后五天涨跌幅 \
                    &nbsp&nbsp&nbsp&nbsp第一天 &nbsp&nbsp&nbsp 第二天 &nbsp&nbsp&nbsp \
                    第三天 &nbsp&nbsp&nbsp 第四天 &nbsp&nbsp&nbsp 第五天 &nbsp&nbsp&nbsp 10天总涨幅<p>')
    _diliang_list = sorted(analysis_volume(symbol))
    global CLOSE, VOLUME
    volume = copy.deepcopy(VOLUME)
    close = copy.deepcopy(CLOSE)
    for each in _diliang_list:
        _temp = '<p>&nbsp&nbsp&nbsp{}&nbsp&nbsp&nbsp&nbsp|&nbsp&nbsp \
                &nbsp&nbsp&nbsp&nbsp&nbsp'.format(volume[each][0])
        _temp_price = []
        _temp_price.append(close[each][1])
        _count = 1
        _step = 1
        try:
            # 计算出地量日期的涨跌幅
            while _count < 11:
                if len(close)-1 < each+_step:
                    break
                if close[each+_step][1] > 0:
                    _temp_price.append(close[each+_step][1])
                    _count += 1
                _step += 1
            if len(_temp_price) < 2:
                continue
            if _temp_price[0] == 0:
                _temp += '&nbsp&nbsp&nbsp/&nbsp&nbsp&nbsp'
            else:
                _temp += '{:>16.2f}%&nbsp&nbsp&nbsp&nbsp'.format( \
                    (_temp_price[1]-_temp_price[0])/_temp_price[0]*100)
            # 展示出现地量后的个股后5个交易日的涨跌幅
            for _day in range(2, 6):
                if (_day+1) > len(_temp_price):
                    _temp += '&nbsp&nbsp&nbsp/&nbsp&nbsp&nbsp'
                else:
                    _temp += '{:>16.2f}%&nbsp&nbsp&nbsp&nbsp'.format( \
                        (_temp_price[_day]-_temp_price[_day-1])/_temp_price[_day-1]*100)
            # 计算出现地量后10个交易日的总涨跌幅
            if _temp_price[0] == 0:
                _temp += '{:>16.2f}%&nbsp&nbsp&nbsp&nbsp<p>'.format( \
                    (_temp_price[-1] - _temp_price[1])/_temp_price[1]*100)
            else:
                _temp += '{:>16.2f}%&nbsp&nbsp&nbsp&nbsp<p>'.format( \
                    (_temp_price[-1] - close[each][1])/close[each][1]*100)
            _html.append(_temp)
        except Exception as err:
            logging.info('HTML格式化打印出错: %s', str(err))
            exit(-1)
    return ''.join(_html)

def html_dl_stocks(stocks=None):
    ''' 将数据库里保存的地量股按html的格式展示 '''
    _html = '<h3>&nbsp&nbsp出现的地量股&nbsp&nbsp</h3>'
    _html += '<h5>创业板</h5><p>'
    if 'sc' in stocks and len(stocks['sc']) > 0:
        _html += '&nbsp&nbsp&nbsp'.join(stocks['sc'].split(','))
    else:
        _html += '昨天没有地量股'
    _html += '</p><br>'

    _html += '<h5>深</h5><p>'
    if 'sz' in stocks and len(stocks['sz']) > 0:
        _html += '&nbsp&nbsp&nbsp'.join(stocks['sz'].split(','))
    else:
        _html += '昨天没有地量股'
    _html += '</p><br>'

    _html += '<h5>沪</h5><p>'
    if 'sh' in stocks and len(stocks['sh']) > 0:
        _html += '&nbsp&nbsp&nbsp'.join(stocks['sh'].split(','))
    else:
        _html += '昨天没有地量股'
    _html += '</p><br>'
    return _html

def get_format_stocks():
    ''' 返回json格式化（code、name、goodsId[用于调取haogu个股数据接口]）的个股数据 '''
    global MONGO
    diliang_coll = MONGO.get_collection('diliang')
    symbol_coll = MONGO.get_collection('symbol')
    _stocks = {}
    for _stype in ['sh', 'sz', 'sc']:
        _temp = []
        _bk = diliang_coll.find_one({'type':_stype})['symbol']
        if len(_bk) < 2: # 如果没有地量
            _stocks.update({_stype:_temp})
            continue
        for _scode in _bk.split(','):
            _symbol = symbol_coll.find_one({'symbol':str(int(_scode))})
            _temp.append({
                'code': '{:0>6}'.format(_scode),
                'name': _symbol['name'],
                'goodsId': _symbol['secuId']
            })
        _stocks.update({_stype:_temp})
    return _stocks

def get_stocks():
    ''' 从数据库中提取计算好的地量股 '''
    global MONGO
    diliang_coll = MONGO.get_collection('diliang')
    _stocks = {
        'sh': diliang_coll.find_one({'type':'sh'})['symbol'],
        'sz': diliang_coll.find_one({'type':'sz'})['symbol'],
        'sc': diliang_coll.find_one({'type':'sc'})['symbol'],
    }
    return _stocks

def update_dl_stocks(duration=None):
    ''' 遍历数据库中所有个股，通过分析计算，将得出的地量股储存在mongo数据库，
        每个板块的地量股按英文逗号分隔，储存为一个字符串，
        其中sh->沪、sz->深、sc->创业板
    '''
    if duration is None:
        is_all = 1
    elif duration == 'lastweek':
        is_all = 2
    stock_sc = []
    stock_sz = []
    stock_sh = []
    for symbol in range(300001, 300550): # 创业板个股代码
        if analysis_volume(symbol, date=1, is_all=1) != None:
            stock_sc.append(str(symbol))
            logging.info('stock %d success.', symbol)
    print('-----1--------')
    for symbol in range(600001, 602000): # 沪股个股代码
        if analysis_volume(symbol, date=1, is_all=1) != None:
            stock_sh.append(str(symbol))
            logging.info('stock %d success.', symbol)
    print('------2-------')
    for symbol in range(2001, 2756): # 深股个股代码
        if analysis_volume(symbol, date=1, is_all=1) != None:
            stock_sz.append('{:0>6}'.format(symbol))
            logging.info('stock %d success.', symbol)
    print('-------3------')
    for symbol in range(1, 1000): # 深股个股代码
        if analysis_volume(symbol, date=1, is_all=1) != None:
            stock_sz.append('{:0>6}'.format(symbol))
            logging.info('stock %d success.', symbol)
    global MONGO
    diliang_coll = MONGO.get_collection('diliang')
    diliang_coll.update_one({'type':'sh'}, {'$set':{'symbol':','.join(stock_sh)}})
    diliang_coll.update_one({'type':'sz'}, {'$set':{'symbol':','.join(stock_sz)}})
    diliang_coll.update_one({'type':'sc'}, {'$set':{'symbol':','.join(stock_sc)}})

def analysis_volume(symbol,date=None, is_all=None):
    '''is_all为None表示分析个股，不为None则分析全部，其中1代表分析最近一天，2代表最近一周'''
    volume, close = get_data(symbol, flag=date)
    global VOLUME, CLOSE
    VOLUME = copy.deepcopy(volume)
    CLOSE = copy.deepcopy(close)
    diliang_date = [] # 地量交易日
   # diliang_date.extend(list(zd_diliang()))  # 震荡式地量
    diliang_date.extend(list(xdfh_diliang()))  # 下跌放量翻红式地量
    diliang_date.extend(list(xd_diliang()))  # 下跌式地量
    diliang_date = list(set(diliang_date))
    # 判断是否分析全部
    if is_all != None:
        if len(diliang_date) == 0:
            return None
        if is_all == 1:
            _deadline = volume[-1][0]
        elif is_all == 2:
            if len(volume) >= 7:
                _deadline = volume[-7][0]
            else:
                _deadline = volume[0][0]
        for each in diliang_date:
            if volume[each][0] == _deadline:
                print('{0} - {1}'.format(symbol, volume[each][0]))
                return symbol
        return None
    return diliang_date

def zd_diliang():
    ''' 分析震荡式下跌行情'''
    _dl_dates = set() # 保存出现地量的日期序号【列表下标】
    global VOLUME
    volume = copy.deepcopy(VOLUME)
    _period = []
    _flag = 0
    _avg = 0
    for index, value in enumerate(volume):
        try:
            _volume = value[1]
            if _volume == 0:
                _period = []
                _flag = _avg = 0
                continue
            if len(_period) < 2:
                _period.append(_volume)
            else:
                _res = _check_volume(_volume, _period) # 检测交易量是否平稳
                _flag += _res
                if _flag < 3:
                    _avg = (_avg * len(_period) + _volume)/(len(_period)+1)
                    _period.append(_volume)
                else:
                    if len(_period) > 5: # 如果连续5个交易日的交易量很平稳
                        if _avg == 0:
                            continue
                        # 从平稳期后20天检测是否出现地量
                        if len(volume) - index > 24:
                            _res = _extract_diliang(_avg, volume[index+1:index+25])
                        else:
                            _res = _extract_diliang(_avg, volume[index+1:])
                        if _res != None:
                            _dl_dates.add(_res+index+1)
                    _period = []
                    _flag = _avg = 0
        except Exception as err:
            logging.error('flag: %d ', str(err))
    return _dl_dates

def xdfh_diliang():
    ''' 下跌过程放量翻红行情
    '''
    _dl_dates = set()
    global VOLUME, CLOSE
    volume = copy.deepcopy(VOLUME)
    close = copy.deepcopy(CLOSE)
    period = []
    price_rate = [] # 股价盈亏率
    _avg = 0 # 平均值volume
    _range = 0 # 极差
    _min = 0 # 最高值
    _max = 0 # 最低值
    _pre_price = 0 # 前一天股价
    for index, value in enumerate(volume):
        #print('{} day: volum:{}, close:{}'.format(index,value[1],close[index][1]))
        if int(value[1]) == 0: # 如果出现成交量为0 则是停牌
            period = []
            price_rate = []
            _avg = _range = _min = _max = _pre_price = 0
            continue
        _price = close[index][1]
        if len(period) == 0:
            _rate = 0
        else:
            _rate = (_price - _pre_price) / _pre_price # 计算涨跌幅
        if _rate < -0.12: # 过滤高送转分红[涨跌幅10%阈值]
            period = [value[1]]
            price_rate = [0]
            _avg = _min = _max = value[1]
            _range = 0
            _pre_price = _price
            continue
        if len(period) > 2 and _price_trend(price_rate) is True:
            # 判断股价是否是上升趋势，如果是则抛弃，从队首开始直至趋势趋于下跌或者平稳
            # print('----------------------------------------------------------')
            # print(period)
            # print(['{:.2f}%'.format(x*100) for x in price_rate])
            # print('{:.2f}%'.format(sum(price_rate)/len(price_rate)*100))
            # print('\r\n')
            # if value[0] > 20160401 and value[0] < 20160606:
            #     print('x:period:{}'.format(period))
            price_rate = price_rate[1:]
            period = period[1:]
            period.append(value[1])
            price_rate.append(_rate)
            _avg, _range, _min, _max = cal_index2(period)
            _pre_price = _price
            continue
        if len(period) > 4:
            price_rate.append(_rate)
            _pre_price = _price
            _avg, _range, _min, _max = cal_index(_avg, len(period), _min, _max, value[1])
            period.append(value[1])
            # if value[0] > 20160401 and value[0] < 20160606:
            #     print('y:period:{}'.format(period))
            if check_diliang(period, value[1]) is True:
               # print('------------------++++++++++++++++++++++-------------')
                if value[0] > 20170301:
                    print(period)
                    print(['{:.2f}%'.format(x*100) for x in price_rate])
                # 从平稳期后25天检测是否出现地量
                if len(volume) - index > 5:
                    _res = _extract_diliang2(value[1], \
                     volume[index+1:index+5], close[index+1:index+5])
                else:
                    if len(volume) - index < 2:
                        continue
                    _res = _extract_diliang2(value[1], volume[index+1:], close[index+1:])
                if _res != None:
                   # print('-----------{}'.format(volume[_res+index+1][0]))
                    _dl_dates.add(_res+index+1)
                price_rate = []
                period = []
                #print('avg:{}, min:{}, max:{}'.format(_avg,_min,_max))
                _avg = _range = _min = _max = 0
        else:
            # 处理股价为0的情况
            if _pre_price == 0:
                price_rate.append(0)
            else:
                price_rate.append(_rate)
            _pre_price = _price
            _avg, _range, _min, _max = cal_index(_avg, len(period), _min, _max, value[1])
            period.append(value[1])
    return _dl_dates

def xd_diliang():
    ''' 下跌式直接地量行情:
        在交易量连续下跌的趋势下出现出现一个大的下跌幅度，
        当跌幅超过30%时，视为地量
    '''
    _dl_dates = set()
    global VOLUME
    volume = copy.deepcopy(VOLUME)
    _section = []
    for index, value in enumerate(volume):
        _vol = value[1]
        if _vol == 0: # 如果出现交易量为0，则停牌
            _section = []
            continue
        if len(_section) == 0:
            _section.append(_vol)
            continue
        _rate = (_vol- _section[-1]) / _section[-1]
        if _rate > - 0.2:
            _section = [_vol]
        else:
            if len(_section) < 3:
                _section.append(_vol)
            else:
                if _rate < -0.25: # 下跌地量
                    _dl_dates.add(index)
                    _section = []
                else:
                    if len(_section) > 4:
                        _section = _section[1:]
                    else:
                        _section.append(_vol)
    return _dl_dates

def _process_xd(_input, _vol):
    ''' 依次处理交易量
        @_input: 符合模型的下跌交易日区间
        @_vol: 用于比较、处理的交易量
    '''
    if len(_input) == 0:
        _input.add(_vol)
        return _input
    return None

def _extract_diliang(_avg, _input):
    '''提取趋势平稳后的地量
        分析趋势平稳后的N个连续交易日的交易量，最低量_min[1],
        如果_min[1]比平均量低50%，则认为是地量
        @_avg: 前几天趋势平稳的平均交易量
        @_input: 后20天的交易量
        返回地量日期的下标
    '''
    _counter = 0
    _stock = []
    _temp = {}
    for index, value in enumerate(_input):
        if value[1] == 0: # 过滤停牌交易日
            continue
        _temp.update({index: value[1]})
    if len(_temp) == 0:
        return None
    _stock = sorted(_temp.items(), key=lambda x: x[1])
    _min = _stock[0]
    if (_min[1] - _avg) / _avg < -0.5:
        return _min[0]
    else:
        return None

def _extract_diliang2(_now, _input, _close):
    '''提取下跌式地量'''
    _res_list = []
    _temp = {}
    _high = 0
    _index = 0
    for index, value in enumerate(_input):
        if value[1] == 0:
            continue
        if (value[1] / _now) > 1.4: # 出现地量前的增量交易
            if index > 0 and _close[index][1] < _close[index-1][1]: # 放量下跌则放弃
                continue
            #print('放量2：{}'.format(value))
            _high = value[1]
  #          print(_high)
            for iidex, vvalue in enumerate(_input[index+1:]):
                if iidex == 0:
                    _temp.update({index+1: vvalue[1]})
                    continue
                if _close[index+iidex+1][1] < _close[index+iidex][1]:
                    _temp.update({index+iidex+1: vvalue[1]})
            break
    if len(_temp) == 0:
        return None
    _res_list = sorted(_temp.items(), key=lambda x: x[1])
    _volume_index = _res_list[0][0]
    #print('_volume_index：{}'.format(_volume_index))
    _price_min = 10000000
    _price_index = 0
    #print('_close:{}'.format(_close))
    for index, value in enumerate(_close):
        if value[1] == 0:
            continue
        if value[1] < _price_min:
            _price_min = value[1]
            _price_index = index
 #   print('_price_index: {}'.format(_price_min))
    # 放量后的最低量不一定是股价最低点
    if _volume_index != _price_index:
        _index = _price_index
    if (_input[_index][1] - _high) / _high < -0.3:
        return _index
    else:
        return None

def _check_volume(_volume, _period):
    ''' 检测成交量是不是处于平稳趋势 '''
    _max = max(_period)
    if ((_volume - _max) / _max) > 0.3:
        return 3
    _avg = sum(_period) / len(_period)
    _ratio = abs((_volume-_avg)/_avg)
    if _ratio < 0.2:
        return 0
    elif _ratio < 0.3:
        return 1
    elif _ratio < 0.4:
        return 2
    else:
        return 3
    # _sd = _calculate_standard_deviation(_avg, _period)
    # if abs(_volume - _avg) < _sd:
    #     return True
    # else:
    #     return False

def _calculate_standard_deviation(_avg, _input):
    '''计算标准差'''
    _temp = 0
    _len = len(_input)
    for each in _input:
        _temp += (each - _avg) ** 2
    return int(math.sqrt(_temp/_len))

def _price_trend(price_rate):
    ''' 计算分析价格趋势
        @price_rate: 用于计算分析的涨跌幅列表
    '''
    if price_rate[0] == 0:
        price_rate = price_rate[1:]
    _len = len(price_rate)
    if _len <= 5: # 5天内的涨跌幅均值 > -0.008, 则表示是上升趋势
        if sum(price_rate) / _len > -0.008:
            return True
    elif _len <= 10: # 10天内的涨跌幅均值 > -0.0035, 则表示是上升趋势
        if sum(price_rate) / _len > -0.0035:
            return True
    else: # 大于10天的交易日涨跌幅均值 > -0.0025, 则表示是上升趋势
        if sum(price_rate) / _len > -0.0025:
            return True
    return False

def cal_index(avg, size, _min, _max, item):
    ''' 计算一些统计指标 '''
    if size == 0:
        _min = item
    if item > _max:
        _max = item
    elif item < _min:
        _min = item
    _range = _max - _min
    avg = (avg*size + item) / (size+1)
    return avg, _range, _min, _max

def cal_index2(_input):
    ''' 计算一些统计指标, 最大最小值、均值、极差值 '''
    _min = min(_input)
    _max = max(_input)
    _range = _max - _min
    _avg = sum(_input) / len(_input)
    return _avg, _range, _min, _max

def check_diliang(period, volume):
    ''' 检查是否地量出现
        交易量 / (period前三交易量的均值) 大于 预设阈值THRESHOLD
        @priod: 前N天交易量列表
        @volume: 当前交易量
    '''
    _avg = sum(sorted(period)[-3:]) / 3 # 取交易量前三, 计算其平均值
    global THRESHOLD
    if (_avg - volume)/_avg > THRESHOLD:
        return True
    else:
        return False

def _init_data_cache():
    ''' 设置全局MONGO实例，避免大量的数据库连接开销 '''
    global MONGO
    MONGO = Mongo('127.0.0.1', 27028, 'stock', 'xiaohe', 'stock123)#)^')

def get_data(symbol, flag):
    ''' 获取历史交易数据（成交量、收盘价）'''
    if flag is None:
        _mongo = Mongo('127.0.0.1', 27028, 'stock', 'xiaohe', 'stock123)#)^')
    else:
        global MONGO
        _mongo = MONGO
    vl_coll = _mongo.get_collection('volume')
    # 按照要求获取前2个月或者前2年的历史数据
    if flag != None:
        start_date = get_start_date(month=2)
    else:
        start_date = get_start_date(year=2)
    vol_cur = vl_coll.find({'symbol': symbol, 'date':{'$gt': start_date}})
    volume = {} # 交易量
    close = {} # 股价
    for each in vol_cur:
        volume.update({each['date']:each['volume']})
        close.update({each['date']:each['close']})
    volume = sorted(volume.items(), key=lambda d: d[0])
    close = sorted(close.items(), key=lambda d: d[0])
    return volume, close

def get_start_date(year=None, month=None, day=None):
    ''' 获取用于计算的数据的起始日期 '''
    localtime = time.localtime(time.time())
    _year = localtime.tm_year
    _month = localtime.tm_mon
    _day = localtime.tm_mday
    if year != None:
        _year -= year
    if month != None:
        if _month > 2:
            _month -= month
        else:
            _year -= 1
            _month = 12 + month - 2
    if day != None:
        _day -= day
    return int('{}{:0>2}{:0>2}'.format(_year, _month, _day))
def _logging_conf():
    ''' 日志文件配置 '''
    logging.basicConfig(level=logging.INFO, \
                        format='%(asctime)s [line:%(lineno)d] %(levelname)s %(message)s', \
                        datefmt='%Y-%m-%d %H:%M:%S', \
                        filename='logging.log', \
                        filemode='w')

def _get_conf():
    ''' 获取配置参数 '''
    cfpr = configparser.ConfigParser()
    cfpr.read('conf.cfg')
    global THRESHOLD
    THRESHOLD = float(cfpr.get('volume', 'threshold'))

_get_conf()
_logging_conf()
_init_data_cache()

if __name__ == '__main__':
    print(html_single_stock(analysis_volume(300506)))
  #  update_dl_stocks()
