import pathlib
import unittest

import pandas as pd

from belling import common
import yaml


class TestCommon(unittest.TestCase):

    def setUp(self) -> None:
        data_file = pathlib.Path(".") / "data" / "data.yaml"
        with data_file.open("r") as f:
            data = yaml.unsafe_load(f)
        self.data_frame = pd.DataFrame(data)

    def test_common(self):
        df = common.combine_data(["filePath", "key"], self.data_frame)
        print(df)

    def test_filename_feature(self):
        df = common.filename_feature(self.data_frame)
        print(df)


if __name__ == '__main__':
    unittest.main()
