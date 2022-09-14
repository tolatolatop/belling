#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 2:14
# @Author  : tolatolatop
# @File    : common.py

import pandas as pd


def combine_data(columns, df: pd.DataFrame):
    name = '+'.join(columns)
    df[name] = df[columns].apply(lambda x: '&'.join(x), axis=1)
    return df


def filename_feature(df: pd.DataFrame):
    df["fileName"] = df['filePath'].str.replace("(.*/)*", "", regex=True)
    return df
