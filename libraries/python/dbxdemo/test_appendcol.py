import pytest

from dbxdemo.spark import get_spark
from dbxdemo.appendcol import with_status

class TestAppendCol(object):

    def test_with_status(self):
        source_data = [
           