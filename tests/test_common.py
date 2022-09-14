import asyncio
import pathlib
import unittest

import pandas as pd

from belling import common
from belling import feature
import yaml

from belling.common import create_repo_info
from belling.feature import add_git_info


class TestCommon(unittest.TestCase):

    def setUp(self) -> None:
        data_file = pathlib.Path(".") / "data" / "data.yaml"
        repo_info_file = pathlib.Path(".") / "data" / "repo_info.yaml"
        with data_file.open("r") as f:
            data = yaml.unsafe_load(f)
        data = self.create_data_yaml()
        self.data_frame = pd.DataFrame(data)

        with repo_info_file.open("r") as f:
            self.repo_info = [v["repo_path"] for v in yaml.unsafe_load(f)]

    def create_data_yaml(self):
        root_dir = pathlib.Path(".").parent
        data = []
        for i in root_dir.glob("**/*"):
            if i.is_dir():
                continue
            tmp = {
                "filePath": str(i.absolute()),
                "key": i.parent.name,
                "line_num": 1,
                "repo_info": str(i.parent.parent),
                "id": 1
            }
            data.append(tmp)
        return data

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

    def test_group_by_map(self):
        data = asyncio.run(common.group_by_map(self.data_frame, "key", print))

    def test_add_git_info(self):
        df = self.data_frame
        df["repo_info"] = r"E:\python_project\belling"
        df["line_num"] = "1"
        res = add_git_info(self.data_frame, "None")
        print(res)
        res.to_csv("data.csv")

    def test_create_repo_info(self):
        root = pathlib.Path(".").absolute().parent.parent
        res = create_repo_info(root, pathlib.Path(".") / "data" / "repo_info.yaml")


if __name__ == '__main__':
    unittest.main()
