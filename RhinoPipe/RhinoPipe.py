"""
懒加载
"""

from RhinoCollect.CollectObject.RhinoCollectObject import RhinoCollectConfig
from RhinoObject.Base.BaseEnum import DealDataType

from RhinoPipe.RhinoRedis.RhinoRedis import RhinoRedis
from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger


class RhinoPipe:

    @classmethod
    def get_instance(cls, logger: RhinoLogger, rhino_collect_config: RhinoCollectConfig):
        instance = None
        if rhino_collect_config.collect_type == DealDataType.REDIS.value:
            instance = RhinoRedis.get_instance(logger, rhino_collect_config)
        return instance
