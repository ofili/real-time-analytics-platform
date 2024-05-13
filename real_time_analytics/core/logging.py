import logging
import ecs_logging

from real_time_analytics.core.config import Settings

logger = logging.getLogger(Settings.spark_app_name)
logger.setLevel(logging.DEBUG)

# Add an ECS formatter to the Handler
handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())
file_handler = logging.FileHandler("./app.log")
file_handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)
