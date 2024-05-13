import time
from asyncio import sleep
from functools import wraps

from real_time_analytics.core.logging import logger


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for handling function retries.

    This class implements the circuit breaker design pattern. It tracks the number of failures for a function and
    can trip the circuit if the failure count exceeds a threshold. When tripped, the circuit breaker prevents
    further executions of the function for a specified reset timeout period. This helps prevent cascading failures
    and allows for recovery.

    Attributes:
        fail_count (int): The current number of consecutive failures.
        open (bool): Indicates if the circuit breaker is currently open.
        last_fail_time (float, optional): The timestamp of the last failure.
        fail_threshold (int): The number of failures required to trip the circuit breaker.
        reset_timeout (float): The time (in seconds) to wait before resetting the circuit breaker.
    """

    def __init__(self, fail_threshold=5, reset_timeout=10):
        self.fail_count = 0
        self.open = False
        self.last_fail_time = None
        self.fail_threshold = fail_threshold
        self.reset_timeout = reset_timeout

    def check(self):
        """
        Checks if the circuit breaker is open or closed.

        This method checks if the circuit breaker is currently open. If it's open, it also verifies if the
        reset timeout period has elapsed. If the timeout has passed, the circuit breaker is reset to closed.

        Returns:
            bool: True if the circuit breaker is closed, False otherwise.
        """

        if self.open:
            current_time = time.monotonic()
            elapsed_time = current_time - self.last_fail_time
            if elapsed_time >= self.reset_timeout:
                self.reset()
        return not self.open

    def trip(self):
        """
        Trips the circuit breaker, marking it as open.

        This method sets the circuit breaker state to open, records the current time as the last failure time,
        and logs an informational message about the circuit breaker being opened.
        """

        self.open = True
        self.last_fail_time = time.monotonic()
        logger.info("Circuit breaker opened due to repeated failures.")

    def reset(self):
        """
        Resets the circuit breaker to closed state.

        This method resets the circuit breaker's state to closed, clears the failure count, and logs an informational
        message about the circuit breaker being reset.
        """

        self.open = False
        self.fail_count = 0
        logger.info("Circuit breaker reset after timeout.")


def retry(
    max_retries=3, backoff_factor=2, exceptions=(Exception,), circuit_breaker=None
):
    """
    Retry decorator with exponential backoff and optional circuit breaker.

    This decorator retries the wrapped function up to a specified maximum number of attempts with exponential
    backoff between retries. Optionally, it can also integrate with a circuit breaker object to prevent excessive
    retries in case of continuous failures.

    Args:
        max_retries (int, optional): The maximum number of retries allowed. Defaults to 3.
        backoff_factor (float, optional): The factor by which to multiply the wait time between retries.
            Defaults to 2 (exponential backoff).
        exceptions (tuple, optional): A tuple of exception types to retry on. Defaults to (Exception,).
        circuit_breaker (CircuitBreaker, optional): A CircuitBreaker object to use for tripping and resetting.
            Defaults to None.

    Returns:
        callable: A decorator function that can be applied to other functions.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if circuit_breaker and not circuit_breaker.check():
                logger.info("Circuit breaker is open, skipping retry.")
                raise exceptions("Circuit breaker is open")

            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    logger.error(
                        f"Error in attempt {attempt}/{max_retries}: {e.__class__.__name__}"
                    )
                    if circuit_breaker:
                        circuit_breaker.fail_count += 1
                        if circuit_breaker.fail_count >= circuit_breaker.fail_threshold:
                            circuit_breaker.trip()
                    delay = backoff_factor**attempt
                    await sleep(delay)
            if circuit_breaker:
                circuit_breaker.trip()
            raise exceptions(f"Failed after {max_retries} attempts")

        return wrapper

    return decorator


circuit_breaker = CircuitBreaker(fail_threshold=5, reset_timeout=10)
