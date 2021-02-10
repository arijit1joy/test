"""
    This file contains the functions that are common for the entire framework.
"""


# This is a decorator function to gracefully handle any unhandled exceptions
def exception_handler(decorated_function):
    def inner_function(*args, **kwargs):
        try:
            return decorated_function(*args, **kwargs)  # Attempt to execute the function and catch any uncaught errors
        except Exception as regression_test_exception:  # noqa
            import traceback
            import time
            import sys
            import json

            # Get the exception information and the traceback print for proper formatting
            exception_type, exception_value, exception_traceback = sys.exc_info()
            traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)

            # Format the error information for readability
            error_message = {
                "errorType": exception_type.__name__,
                "errorMessage": str(exception_value),
                "stackTrace": f"\n{traceback_string}"
            }  # This could also be converted into a string using json.dumps

            # This will be printed to the user to provide the function's name and the error information
            print(f"An error occurred in the function: '{decorated_function.__module__}.{decorated_function.__name__}'."
                  f"\n\nError Information: {error_message}")

            raise regression_test_exception

    return inner_function


# This is a decorator function to set delays before or after a function executes
@exception_handler
def set_delay(time_in_secs, wait_before=False):
    def function_wrapper(decorated_function):
        def inner_function(*args, **kwargs):
            import time

            delay_notification = f"Delaying the process for {time_in_secs} seconds . . ."

            # If "wait_before" is set to "True", delay before the function executes else delay after its execution
            if wait_before:
                print(delay_notification)
                time.sleep(time_in_secs)  # Delay for the specified time
                return decorated_function(*args, **kwargs)  # Execute decorated function

            function_execution_response = decorated_function(*args, **kwargs)  # Execute decorated function

            print(delay_notification)
            time.sleep(time_in_secs)  # Delay for the specified time

            return function_execution_response
        return inner_function
    return function_wrapper


@exception_handler
def get_formatted_date(date_format):
    import datetime
    return datetime.datetime.now().strftime(date_format)
