#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 2:14
# @Author  : tolatolatop
# @File    : common.py
import asyncio
import subprocess as sp

import pandas as pd


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
