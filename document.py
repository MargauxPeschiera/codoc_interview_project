#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 17:43:04 2020

@author: margauxpeschiera
"""

import re
import os
import datefinder
import docx2txt

from typing import NamedTuple, Optional, Iterable
from tika import parser

from datetime import datetime, date


FileInfo = NamedTuple(
    "FileInfo",
    [("ipp", int), ("id_doc", int)],
)

Doc = NamedTuple(
    "Doc",
    [
        ("patient_num", int),
        ("doc_num", int),
        ("doc_date", Optional[date]),
        ("update_date", date),
        ("doc_origine_code", str),
        ("display_text", str),
        ("author", Optional[str]),
    ],
)


_REGEX_FILENAME = re.compile(r"(?P<ipp>\d*)_(?P<doc>\d*)\.(pdf|docx)")
_REGEX_EXTENSION = re.compile(r"\w*\.(?P<type>pdf|docx|xlsx)")


def get_info_from_filename(file_name: str) -> FileInfo:
    rexres = _REGEX_FILENAME.match(file_name)
    if rexres is None:
        raise Exception(f"Can not parse {file_name}")
    ipp = rexres.groupdict()["ipp"]
    docid = rexres.groupdict()["doc"]

    return FileInfo(ipp, docid)


def convert_docx_to_ascii(file: str) -> str:
    text = docx2txt.process(file)
    return text.replace('"', "")


def convert_pdf_to_ascii(file_name: str) -> str:

    raw = parser.from_file(file_name)
    return raw["content"].replace('"', "")


def get_extension(file_name: str) -> str:
    rexres = _REGEX_EXTENSION.match(file_name)
    if rexres is None:
        raise Exception(f"Can not parse {file_name}")
    return rexres.groupdict()["type"]


def search_date(text: str) -> date:
    matches = list(datefinder.find_dates(text))
    matches = [dat.date() for dat in matches]
    limit_date = datetime(2000, 1, 1).date()
    if len(matches) >= 2:
        if matches[0] > limit_date:
            return matches[0]
        else:
            return matches[1]
    elif len(matches) == 1:
        return matches[0]
    else:
        return None


def search_author(text: str) -> str:
    text = text.lower()
    matches = ["".join(x) for x in re.findall(r"(?i)dr\s[a-z]+\s?[a-z]+", text)]
    if len(matches) > 0:
        return matches[-1]
    else:
        return None


def parse_all_files(directory_path: str) -> Iterable[Doc]:
    import database as db

    entries = os.scandir(directory_path)
    for entry in entries:
        file_name = entry.name
        entension = get_extension(file_name)
        if entension == "pdf":
            file_info = get_info_from_filename(file_name)
            text = convert_pdf_to_ascii(directory_path + file_name)

            yield Doc(
                patient_num=db.get_patient_num_from_ipp(file_info.ipp),
                doc_num=file_info.id_doc,
                doc_date=search_date(text),
                update_date=date.today(),
                doc_origine_code="DOSSIER_PATIENT",
                display_text=text,
                author=search_author(text),
            )
        elif entension == "docx":
            file_info = get_info_from_filename(file_name)
            text = convert_docx_to_ascii(directory_path + file_name)

            yield Doc(
                patient_num=db.get_patient_num_from_ipp(file_info.ipp.lstrip("0")),
                doc_num=file_info.id_doc,
                doc_date=search_date(text),
                update_date=date.today(),
                doc_origine_code="RADIOLOGIE_SOFTWARE",
                display_text=text,
                author=search_author(text),
            )
