from flask import Flask


class FlaskApp(object):
    """
        We need a way to access Flask configuration globally
    """
    _app = None

    def __init__(self):
        raise Exception('call instance()')

    @classmethod
    def app(cls):
        if cls._app is None:
            cls._app = Flask(__name__, template_folder='../templates')
            # more init opration here
        return cls._app
