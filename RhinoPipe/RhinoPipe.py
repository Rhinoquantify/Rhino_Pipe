"""
懒加载
"""

from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Base.BaseEnum import DealDataType
from RhinoObject.Rhino.RhinoObject import RhinoConfig

from RhinoPipe.RhinoRedis.RhinoSetGetRedis import RhinoSetGetRedis


class RhinoPipe:

    @classmethod
    def get_instance(cls, logger: RhinoLogger, rhino_collect_config: RhinoConfig):
        if rhino_collect_config is None:
            return None
        instance = None
        if rhino_collect_config.collect_type == DealDataType.REDIS.value:
            instance = RhinoSetGetRedis.get_instance(logger, rhino_collect_config)
        return instance
