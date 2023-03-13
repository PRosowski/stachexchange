import unittest
from mock import patch

import dag_definition.stackexchange_pr_ingest_dag as module_main


class TestGetListOf7zFiles(unittest.TestCase):
    @patch('stackexchange_pr_ingest_dag.get')
    def test_func(self, get_content_mock):
        get_content_mock.return_value = '<files><file name="writers.stackexchange.com.7z" source="original"><filecount>8</filecount></file><file name="dsfd.json" source="original"><format>Unknown</format></file></files>'
        self.assertEqual(module_main.get_list_of_7z_files("ss"), ["writers.stackexchange.com.7z"])
        get_content_mock.assert_called_once()



