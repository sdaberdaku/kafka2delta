import logging
import time
from datetime import datetime

from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger(__name__)


class BatchProcessingListener(StreamingQueryListener):
    def __init__(self):
        super().__init__()
        self.error_message = None
        self.last_batch_timestamp = None
        self.total_rows_processed = 0
        self.checkpoint_rows_processed = 0

    def onQueryStarted(self, event):
        logger.info(f"Query started: {event.id}")
        self.error_message = None

    def onQueryProgress(self, event):
        self.last_batch_timestamp = datetime.now()
        num_rows = event.progress.numInputRows
        self.total_rows_processed += num_rows
        self.checkpoint_rows_processed += num_rows

    def onQueryTerminated(self, event):
        if event.exception:
            self.error_message = str(event.exception)
            logger.error(f"Query terminated with error: {event.exception}")
        else:
            logger.info("Query terminated normally")

    def wait_for_snapshot_to_complete(self, idle_seconds=5, timeout_seconds=120):
        """Wait for the query to become idle, indicating snapshot completion"""
        start_time = time.time()
        logger.info(f"Waiting for query to become idle for {idle_seconds} seconds...")

        while (time.time() - start_time) < timeout_seconds:
            if self.error_message:
                logger.error(f"Query encountered error while waiting: {self.error_message}")
                return False

            if self.last_batch_timestamp:
                idle_time = (datetime.now() - self.last_batch_timestamp).total_seconds()
                if idle_time >= idle_seconds:
                    logger.info(f"Query has been idle for {idle_seconds} seconds - initial snapshot complete")
                    return True
            else:
                logger.info("Waiting for first batch to be processed...")

            time.sleep(1)

        logger.warning(f"Timeout after {timeout_seconds} seconds waiting for idle state")
        return False

    def set_checkpoint(self):
        """Mark the beginning of a new operation to track its rows"""
        self.checkpoint_rows_processed = 0

    def wait_for_rows(self, expected_rows_count, timeout_seconds=240):
        """Wait for specific number of rows since the last checkpoint"""
        start_time = time.time()
        logger.info(f"Waiting for {expected_rows_count} rows since last marker...")

        while self.checkpoint_rows_processed < expected_rows_count:
            if self.error_message:
                logger.error(f"Error while waiting for rows: {self.error_message}")
                return False

            if (time.time() - start_time) > timeout_seconds:
                logger.warning(f"Timeout waiting for {expected_rows_count} rows. "
                               f"Got {self.checkpoint_rows_processed} rows")
                return False

            time.sleep(1)

        logger.info(f"Successfully processed {self.checkpoint_rows_processed} rows since marker")
        return True
