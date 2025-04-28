import json
from libs.email_helper import EmailHelper
from flask import Response
import logging
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError
from flask_jwt_extended.exceptions import NoAuthorizationError

ignored_response_codes = [400, 401, 404, 405, 410, 422]
log = logging.getLogger('ResponseHelper')


def response_to_json(response):
    return json.loads(response.get_data().decode("utf-8"))


def handle_exception(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (ExpiredSignatureError, NoAuthorizationError) as e:
            return ResponseHelper.abort(401, msg=str(e))
        except InvalidSignatureError as e:
            return ResponseHelper.abort(422, msg=str(e))
        except Exception as e:
            return ResponseHelper.abort(500, msg=str(e))

    return wrapper


class ResponseHelper:

    def __init__():
        raise Exception('Do not instantiate response_helper')

    @classmethod
    def abort(cls, *args, **kwargs):
        """
        flask_restful is loaded during server startup making it difficult to mock APIs.
        """
        message = kwargs.get('msg', None)
        code = args[0]
        if message is None:
            kwargs = {'msg': 'Abort() missing msg argument'}
            code = 500
        if code not in ignored_response_codes:
            EmailHelper.report_issue(message, subject='Recommendation API')

        dumps = json.dumps(kwargs)
        log.error(f'abort({code}) - {dumps}')

        return Response(dumps, args[0], headers={'Content-Type': 'application/json'})
