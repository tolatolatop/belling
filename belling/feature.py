#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 2:56
# @Author  : tolatolatop
# @File    : feature.py
import asyncio

import pandas as pd

from belling.common import filename_feature, combine_data, get_commit_id, group_by_map, get_commit_msg


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
    asyncio.run(group_by_map(df, "commit_id", "repo_info", add_commit_id))
    asyncio.run(group_by_map(df, "commit_msg", "repo_info", add_commit_msg))
    return df


async def get_commit_id_from_row_data(row_data):
    file_path = row_data["filePath"]
    repo_path = row_data["repo_info"].replace("&", "")
    line_num = row_data["line_num"]
    commit_id = await get_commit_id(file_path, repo_path, line_num)
    return commit_id


async def get_commit_msg_from_branch(df: pd.DataFrame):
    commit_id = df.iloc[0]["commit_id"]
    repo_path = df.iloc[0]["repo_info"]
    commit_msg = await get_commit_msg(repo_path, commit_id)
    df["commit_msg"] = commit_msg
    return df["commit_msg"]


async def add_commit_msg(df: pd.DataFrame):
    await group_by_map(df, "commit_msg", "commit_id", get_commit_msg_from_branch)
    return df["commit_msg"]


async def add_commit_id(df: pd.DataFrame):
    res = []
    for i, data in df.iterrows():
        commit_id = await get_commit_id_from_row_data(data)
        res.append(commit_id)
    return res


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
