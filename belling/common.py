#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 2:14
# @Author  : tolatolatop
# @File    : common.py
import asyncio
import pathlib
import subprocess as sp

import pandas as pd
import yaml


async def shell(cmd, cwd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
        shell=True
    )

    stdout, stderr = await proc.communicate()

    return proc.returncode, stdout, stderr


def combine_data(columns, df: pd.DataFrame):
    name = '&'.join(columns)
    df[name] = df[columns].apply(lambda x: '&'.join(x), axis=1)
    return name, df


def filename_feature(df: pd.DataFrame):
    df["fileName"] = df['filePath'].str.replace("(.*/)*", "", regex=True)
    return df


async def get_commit_id(file_path, repo_path, line_num):
    cmd = ['git', 'blame', f'-L{line_num},{line_num}', '-l', file_path]
    _, stdout, stderr = await shell(" ".join(cmd), repo_path)
    if stderr == b'':
        return stdout.decode().split()[0]
    else:
        return ""


async def get_commit_msg(repo_path, commit_id):
    cmd = ['git', 'show', commit_id, "--pretty=%B", "-s"]
    _, stdout, stderr = await shell(" ".join(cmd), repo_path)
    if stderr == b'':
        return stdout.decode().split()[0]
    else:
        return ""


async def group_by_map(df: pd.DataFrame, label, by, func):
    group_df = [x for _, x in df.groupby(by)]
    result = await asyncio.gather(*(func(sub_df) for sub_df in group_df))
    for new_data, sub_df in zip(result, group_df):
        df.loc[sub_df.index, label] = new_data
    return df


def get_git_info(root: pathlib.Path):
    res = {
        "repo_path": str(root.parent)
    }
    return res


def find_all_git_repo(root: pathlib.Path):
    repo_dir_list = (repo_dir for repo_dir in root.glob("**/.git") if repo_dir.is_dir())
    res = [get_git_info(repo_dir) for repo_dir in repo_dir_list]
    return res


def create_repo_info(root: pathlib.Path, repo_info_file_path):
    res = find_all_git_repo(root)
    with repo_info_file_path.open("w") as f:
        yaml.safe_dump(res, f)
