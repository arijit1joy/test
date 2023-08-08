from unittest.mock import MagicMock


class CDAModuleMockingContext:

    def __init__(self, sys_in_context):
        self._sys_in_context = sys_in_context
        self._existing_mocked_modules = {}
        self._new_mocked_modules = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # Reset the sys.modules dictionary back to what it was before

        for mocked_module_path in self._existing_mocked_modules:
            self._sys_in_context.modules[mocked_module_path] = self._existing_mocked_modules[mocked_module_path]

        for mocked_module_path in self._new_mocked_modules:
            del self._sys_in_context.modules[mocked_module_path]

    def mock_module(self, module_path: str, mock_object: MagicMock = MagicMock()):
        """
        This function replaces the 'sys.modules[module_path] = MagicMock_object' executions.
        If the mock_object is not provided, a new MagicMock object is instantiated and is used to replace the module

        :param module_path: The path to the module to be mocked with dot ('.') notation e.g. 'utilities.utility'
        :param mock_object: A MagicMock object that will be used to mock the module located at the 'module_path' within
        the current context
        :return: None
        """
        if module_path in self._sys_in_context.modules:
            self._existing_mocked_modules[module_path] = self._sys_in_context.modules[module_path]
        else:
            self._new_mocked_modules[module_path] = True

        self._sys_in_context.modules[module_path] = mock_object
