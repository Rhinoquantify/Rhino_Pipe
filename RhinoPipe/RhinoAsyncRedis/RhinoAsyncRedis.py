import pickle
import time
import traceback
from typing import Any, NoReturn, List

import aredis
from RhinoLogger.RhinoLogger.RhinoLogger import RhinoLogger
from RhinoObject.Rhino.RhinoEnum import RhinoDataType
from RhinoObject.Rhino.RhinoObject import RhinoConfig


class RhinoAsyncRedis:
    _redis = None

    def __init__(self, logger: RhinoLogger, rhino_config: RhinoConfig) -> NoReturn:
        self.logger = logger
        self.rhino_redis = None
        self.init(rhino_config)

    @classmethod
    def get_new_instance(cls, logger: RhinoLogger, rhino_config: RhinoConfig):
        return RhinoAsyncRedis(logger, rhino_config)

    @classmethod
    def get_instance(cls, logger: RhinoLogger, rhino_config: RhinoConfig):
        if cls._redis is None:
            cls._redis = RhinoAsyncRedis(logger, rhino_config)
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
            data.store_time = int(time.time() * 1000)
            value = pickle.dumps(data)
            await self.rhino_redis.set(key, value)
            # self.logger.debug("Redis 存储 " + data.__str__())
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

    async def set_channel_data(self, data):
        try:
            key = data.key  # redis 的 key 值
            data.store_time = int(time.time() * 1000)
            if RhinoDataType.RHINODEPTH.value in key:
                await self.rhino_redis.publish(RhinoDataType.RHINODEPTH.value, str(data.__dict__))
            elif RhinoDataType.RHINOTRADE.value in key:
                await self.rhino_redis.publish(RhinoDataType.RHINOTRADE.value, str(data.__dict__))
            elif RhinoDataType.WEBSOCKETSTART.value == key:
                await self.rhino_redis.publish(RhinoDataType.WEBSOCKETSTART.value, str(data.__dict__))
            elif RhinoDataType.WEBSOCKETBREAK.value == key:
                await self.rhino_redis.publish(RhinoDataType.WEBSOCKETBREAK.value, str(data.__dict__))
            elif RhinoDataType.RHINOKLINE.value in key:
                await self.rhino_redis.publish(RhinoDataType.RHINOKLINE.value, str(data.__dict__))
            elif RhinoDataType.RHINOTICKER.value in key:
                await self.rhino_redis.publish(RhinoDataType.RHINOTICKER.value, str(data.__dict__))
            # self.logger.debug("Redis channel 存储 " + data.__str__())
        except Exception as e:
            self.logger.error(f"Redis channel 存储失败 " + data.__str__())
            self.logger.error(traceback.format_exc())

    async def get_channel_data(self, channels: List):
        try:
            public_channel = self.rhino_redis.pubsub()
            await public_channel.subscribe(channels)
            return public_channel
        except Exception as e:
            self.logger.error(f"Redis 订阅失败 " + channels)
            self.logger.error(traceback.format_exc())
