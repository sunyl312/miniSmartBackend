import datetime
import pytz


def currenttime():
    beijing_tz = pytz.timezone('Asia/Shanghai')
    current_time_utc = datetime.datetime.utcnow()
    # 将时间戳转换为本地时间的struct_time对象
    current_time_beijing = current_time_utc.replace(
        tzinfo=pytz.utc).astimezone(beijing_tz)

    # 使用strftime()方法将struct_time对象格式化为指定的时间字符串
    current_time_str = current_time_beijing.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前的年月日时分秒
    return current_time_str
