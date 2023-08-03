""" Structured LOGging

This is currently not in use, because we are using async messaging, and `logging` does not support async.

By adding a ```meta``` field to the log message `extra`, one can add structured data to your logs.
This structure will be interpreted by the StructuredLogHandler.
"""

import logging


class ChainingHandler(logging.Handler):
    def __init__(self, next_handler: logging.Handler):
        super().__init__()
        self.next_handler = next_handler

    def emit(self, record):
        if self.should_handle(record):
            self.actual_handle(record)
        else:
            self.next_handler.handle(record)

    def should_handle(self, record):
        """Determine whether this handler should handle the record"""
        raise NotImplementedError()

    def actual_handle(self, record):
        """Actually handle the record"""
        raise NotImplementedError()


class StructuredLogHandler(logging.Handler):
    """Structured log handler

    This handler interprets a ```meta``` field
    """
    def emit(self, record):
        """Emit a record.

        Interpret a ```meta``` field from the log message.
        """
        try:
            meta = record.meta
        except AttributeError:
            pass



# Create a custom logger
logger = logging.getLogger(__name__)
logger.addHandler(StructuredLogHandler())
logger.error('This is a log message', extra={'meta': {'key': 'value'}})
