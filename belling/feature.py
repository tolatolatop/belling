#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 2:56
# @Author  : tolatolatop
# @File    : feature.py
import pandas as pd

from belling.common import filename_feature, combine_data


def create_total_data_frame(df: pd.DataFrame, task_info, repo_info, repo_path):
    """
    task信息

    :param df:
    :param task_info:
    :param repo_info:
    :param repo_path:
    :return:
    """
    add_task_info(df, task_info)
    add_repo_info(df, repo_info)
    add_unique_label(df)
    add_git_info(df, repo_path)
    return


def add_git_info(df, repo_path):
    pass


def add_unique_label(df):
    unique_label = ["fileName", "key"]
    name, _ = combine_data(unique_label, df)
    df["unqiue_label"] = df[name]


def add_task_info(df, task_info):
    df["task_id"] = task_info["id"]
    df["task_name"] = task_info["name"]
    filename_feature(df)


def add_repo_info(df: pd.DataFrame, repo_info):
    regex = "(" + "|".join(repo_info) + ").*"
    df["repo_info"] = df["filePath"].replace({regex: r"&\1"}, regex=True)
    # 当初始非&则作为未命中处理
    return df
