import pickle
from typing import Any, NoReturn

import aredis
from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
import traceback
from RhinoObject.Rhino.RhinoObject import RhinoConfig


class RhinoSetGetRedis:
    _redis = None

    def __init__(self, logger: RhinoLogger, rhino_config: RhinoConfig) -> NoReturn:
        self.logger = logger
        self.rhino_redis = None
        self.init(rhino_config)

    @classmethod
    def get_instance(cls, logger: RhinoLogger, rhino_config: RhinoConfig):
        if cls._redis is None:
            cls._redis = RhinoSetGetRedis(logger, rhino_config)
        return cls._redis

    def init(self, rhino_config: RhinoConfig) -> NoReturn:
        try:
            self.rhino_redis = aredis.StrictRedis(
                host=rhino_config.redis_config.host,
                port=rhino_config.redis_config.port,
                password=rhino_config.redis_config.password,
                db=rhino_config.redis_config.db_name
            )
            self.logger.info("Redis 初始化成功")
        except Exception as e:
            self.logger.error(f"Redis 初始化失败")
            self.logger.error(traceback.format_exc())

    async def set_data(self, data: Any) -> NoReturn:
        try:
            key = data.key  # redis 的 key 值
            value = pickle.dumps(data)
            await self.rhino_redis.set(key, value)
            self.logger.debug("Redis 存储 " + data.__str__())
        except Exception as e:
            self.logger.error(f"Redis 存储失败 " + data.__str__())
            self.logger.error(traceback.format_exc())

    async def get_data(self, key) -> Any:
        try:
            value = await self.rhino_redis.get(key)
            return pickle.loads(value)
        except Exception as e:
            self.logger.error(f"Redis 获取失败 " + key)
            self.logger.error(traceback.format_exc())