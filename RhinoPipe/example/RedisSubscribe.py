import asyncio

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoLogger.RhinoLoggerObject.RhinoLoggerEnum import LoggerLevel
from RhinoLogger.RhinoLoggerObject.RhinoLoggerObject import LoggerConfig
from RhinoObject.Base.BaseEnum import DealDataType
from RhinoObject.Base.BaseEnum import RedisDataType
from RhinoObject.Rhino.RhinoEnum import RhinoDataType
from RhinoObject.Rhino.RhinoObject import RedisConfig, RhinoConfig
from RhinoPipe.RhinoPipe import RhinoPipe

logger_config = LoggerConfig(
    cmdlevel=LoggerLevel.DEBUG.value,
    filename='RhinoCollectlogs/log.log'
)
rhino_collect_config = RhinoConfig(
    collect_type=DealDataType.REDIS.value,
    redis_config=RedisConfig(
        port=6379,
        password="",
        DataType=RedisDataType.GET.value,
        is_subscribe=True,
    )
)


def start():
    logger = RhinoLogger.get_instance(logger_config)
    p = RhinoPipe.get_instance(logger, rhino_collect_config)(
        [RhinoDataType.RHINOTRADE.value, RhinoDataType.RHINODEPTH.value])
    for item in p.listen():  # 监听状态：有消息发布了就拿过来
        print(item)


if __name__ == '__main__':
    start()
