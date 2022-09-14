#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 2:14
# @Author  : tolatolatop
# @File    : common.py
import subprocess as sp

import pandas as pd


def combine_data(columns, df: pd.DataFrame):
    name = '&'.join(columns)
    df[name] = df[columns].apply(lambda x: '&'.join(x), axis=1)
    return name, df


def filename_feature(df: pd.DataFrame):
    df["fileName"] = df['filePath'].str.replace("(.*/)*", "", regex=True)
    return df


def get_commit_id(file_path, repo_path, line_num):
    cmd = ['git', 'blame', f'-L{line_num},{line_num}', '-l', file_path]
    p = sp.Popen(cmd, cwd=repo_path, stderr=sp.PIPE, stdout=sp.PIPE)
    stdout, stderr = p.communicate()
    if stderr == b'':
        return stdout.decode().split()[0]
    else:
        return ""
