import logging

logger = logging.getLogger("ECommercePipeline")

def test_logger_created():
    assert logger is not None

def test_logger_name():
    assert logger.name == "ECommercePipeline"