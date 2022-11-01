import asyncio
import redis

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoLogger.RhinoLoggerObject.RhinoLoggerEnum import LoggerLevel
from RhinoLogger.RhinoLoggerObject.RhinoLoggerObject import LoggerConfig
from RhinoObject.Base.BaseEnum import DealDataType
from RhinoObject.Base.BaseEnum import RedisDataType
from RhinoObject.Rhino.RhinoEnum import RhinoDataType
from RhinoObject.Rhino.RhinoObject import RedisConfig, RhinoConfig
from RhinoPipe.RhinoPipe import RhinoPipe
from RhinoSign.RhinoSign import RhinoSign

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
        is_async=True,
    )
)


async def start():
    logger = RhinoLogger.get_instance(logger_config)
    p = await RhinoPipe.get_instance(logger, rhino_collect_config)(
        [RhinoDataType.RHINOTRADE.value, RhinoDataType.RHINODEPTH.value])
    while 1:
        data = await p.get_message()
        print(data)


if __name__ == '__main__':
    asyncio.run(start())
