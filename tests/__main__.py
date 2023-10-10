"""Main entry point for all unit tests."""


import sys
import unittest

if __name__ == "__main__":
    test_suite = unittest.defaultTestLoader.discover(".")
    test_runner = unittest.TextTestRunner(resultclass=unittest.TextTestResult)
    result = test_runner.run(test_suite)
    sys.exit(not result.wasSuccessful())
