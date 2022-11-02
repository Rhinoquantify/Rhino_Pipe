import pickle
import traceback
from typing import Any, NoReturn, List

import redis
from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Rhino.RhinoEnum import RhinoDataType
from RhinoObject.Rhino.RhinoObject import RhinoConfig


class RhinoRedis:
    _redis = None

    def __init__(self, logger: RhinoLogger, rhino_config: RhinoConfig) -> NoReturn:
        self.logger = logger
        self.rhino_redis = None
        self.init(rhino_config)

    @classmethod
    def get_instance(cls, logger: RhinoLogger, rhino_config: RhinoConfig):
        if cls._redis is None:
            cls._redis = RhinoRedis(logger, rhino_config)
        return cls._redis

    def init(self, rhino_config: RhinoConfig) -> NoReturn:
        try:
            self.rhino_redis = redis.StrictRedis(
                host=rhino_config.redis_config.host,
                port=rhino_config.redis_config.port,
                password=rhino_config.redis_config.password,
                db=rhino_config.redis_config.db_name
            )
            self.logger.info("Redis 初始化成功")
        except Exception as e:
            self.logger.error(f"Redis 初始化失败")
            self.logger.error(traceback.format_exc())

    def set_data(self, data: Any) -> NoReturn:
        try:
            key = data.key
            value = pickle.dumps(data)
            self.rhino_redis.set(key, value)
        except Exception as e:
            self.logger.error(traceback.format_exc())

    def get_data(self, key: str) -> Any:
        try:
            value = self.rhino_redis.get(key)
            return pickle.loads(value)
        except Exception as e:
            self.logger.error(traceback.format_exc())

    def get_channel_data(self, channels: List):
        try:
            public_channel = self.rhino_redis.pubsub()
            public_channel.subscribe(channels)
            return public_channel
        except Exception as e:
            self.logger.error(f"Redis 订阅失败 " + channels)
            self.logger.error(traceback.format_exc())
