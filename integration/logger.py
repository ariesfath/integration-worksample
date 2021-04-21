import logging


default_logger = logging.getLogger()


class JobLogger:
    """Helper class to help log messages from job execution"""
    @classmethod
    def debug(cls, message):
        print(message)
        default_logger.debug(message)

    @classmethod
    def error(cls, message):
        default_logger.error(message)
