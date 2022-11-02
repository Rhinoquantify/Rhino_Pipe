"""
懒加载
"""

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import DealDataType
from RhinoObject.Base.BaseEnum import RedisDataType
from RhinoObject.Rhino.RhinoObject import RhinoConfig

from RhinoPipe.RhinoAsyncRedis.RhinoAsyncRedis import RhinoAsyncRedis
from RhinoPipe.RhinoRedis.RhinoRedis import RhinoRedis


class RhinoPipe:

    @classmethod
    def get_instance(cls, logger: RhinoLogger, rhino_collect_config: RhinoConfig):
        if rhino_collect_config is None:
            return None
        instance = None
        if rhino_collect_config.collect_type == DealDataType.REDIS.value:
            if rhino_collect_config.redis_config.is_async:
                if rhino_collect_config.redis_config.DataType == RedisDataType.SET.value:
                    if not rhino_collect_config.redis_config.is_subscribe:
                        instance = RhinoAsyncRedis.get_instance(logger, rhino_collect_config).set_data
                    else:
                        instance = RhinoAsyncRedis.get_instance(logger, rhino_collect_config).set_channel_data
                else:
                    if not rhino_collect_config.redis_config.is_subscribe:
                        instance = RhinoAsyncRedis.get_instance(logger, rhino_collect_config).get_data
                    else:
                        instance = RhinoAsyncRedis.get_instance(logger, rhino_collect_config).get_channel_data
            else:
                if rhino_collect_config.redis_config.DataType == RedisDataType.SET.value:
                    if not rhino_collect_config.redis_config.is_subscribe:
                        instance = RhinoRedis.get_instance(logger, rhino_collect_config).set_data
                    else:
                        pass
                else:
                    if not rhino_collect_config.redis_config.is_subscribe:
                        instance = RhinoRedis.get_instance(logger, rhino_collect_config).get_data
                    else:
                        instance = RhinoRedis.get_instance(logger, rhino_collect_config).get_channel_data
        return instance
