import pathlib
import unittest

import pandas as pd

from belling import common
from belling import feature
import yaml


class TestCommon(unittest.TestCase):

    def setUp(self) -> None:
        data_file = pathlib.Path(".") / "data" / "data.yaml"
        with data_file.open("r") as f:
            data = yaml.unsafe_load(f)
        self.data_frame = pd.DataFrame(data)
        self.repo_info = ["base/new/", "base/d/"]

    def test_concat(self):
        df = pd.concat([self.data_frame, self.data_frame])
        print(df)

    def test_common(self):
        feature_name, df = common.combine_data(["filePath", "key"], self.data_frame)
        print(df)

    def test_filename_feature(self):
        df = common.filename_feature(self.data_frame)
        print(df)

    def test_repo_info(self):
        df = feature.add_repo_info(self.data_frame, self.repo_info)
        print(df)

    def test_get_commit_id(self):
        file = pathlib.Path(__file__).absolute()
        res = common.get_commit_id(file, file.parent, 8)
        self.assertNotEqual("", res)

        res = common.get_commit_id(file, file.parent, 100)
        self.assertEqual("", res)

        res = common.get_commit_id(file.parent / "nofound.py", file.parent, 8)
        self.assertEqual("", res)


if __name__ == '__main__':
    unittest.main()
