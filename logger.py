import asyncio
import logging


class FormatterWithTaskName(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        task = asyncio.current_task()
        if task is not None and hasattr(task, "get_name"):
            record.task_name = f"[{task.get_name()}] "
        else:
            record.task_name = ""
        return super().format(record)


logger = logging.getLogger(name="Jobs Postings crawler")

handler = logging.StreamHandler()
handler.setFormatter(
    FormatterWithTaskName("[%(asctime)s] %(levelname)s %(task_name)s%(message)s")
)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
