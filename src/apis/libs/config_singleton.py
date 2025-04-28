from libs.shared.config import Config
# from flask import current_app as app

"""
This is wrapper around the Config lib. The purpose is to implement a caching strategy to
improve API performance. This implementation could be moved to the shared Config class, but
all the code that references it would need to be refactored, therefore the wrapper.

jc - 10/15/2020
"""


class ConfigSingleton:
    me = None

    @classmethod
    def config_instance(cls):
        if cls.me is None:
            # app.logger.debug('Create new ConfigSingleton')
            cls.me = Config()
        return cls.me

    def get_config(self, env):
        try:
            c = ConfigSingleton.config_instance()
            # app.logger.debug('Checking cache for config')
            cached = c.get_cache(env)
            if cached:
                # app.logger.debug('Found config in cache')
                return cached

            # app.logger.debug('Pulling config from repo')
            config = c.get_config(env)
            # app.logger.debug('Caching config')
            c.set_cache(env, config)
            return config
        except Exception as e:
            raise e
