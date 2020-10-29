#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 09:51:47 2020

@author: margauxpeschiera
"""

import pandas as pd

from typing import NamedTuple, Optional, Iterable
from datetime import date, datetime


UPLOAD_ID = 1

Patient = NamedTuple(
    "Patient",
    [
        ("patient_num", int),
        ("nom", str),
        ("prenom", str),
        ("date_naissance", date),
        ("sexe", str),
        ("nom_jeune_fille", Optional[str]),
        ("adresse", str),
        ("tel", str),
        ("code_postale", str),
        ("ville", str),
        ("pays", str),
        ("date_mort", Optional[date]),
        ("code_mort", bool),
        ("upload_id", int),
    ],
)

PatientHistorique = NamedTuple(
    "PatientHistorique",
    [
        ("patient_num", int),
        ("hospital_patient_id", int),
        ("origin_patient_id", str),  # fichier excel pour tous
        ("upload_id", int),
        ("master_patient_id", bool),
    ],
)


def parse_date_or_none(xldate: str) -> Optional[date]:
    if pd.isna(xldate):
        return None
    else:
        date_time = datetime.strptime(xldate, "%d/%m/%Y")
        return date_time.date()


def parse_str_or_none_strip(str_value: str) -> Optional[str]:
    if pd.isna(str_value):
        return None
    else:
        return str(str_value.strip())


def get_list_index_duplicated(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["NOM", "PRENOM", "DATE_NAISSANCE"]]
    df = df[df.duplicated(["NOM", "PRENOM", "DATE_NAISSANCE"], keep=False)]
    list_index = (
        df.groupby(df.columns.values.tolist()).apply(lambda x: tuple(x.index)).tolist()
    )

    return list_index


def add_id_column_to_df(df: pd.DataFrame) -> pd.DataFrame:
    df["ID"] = range(0, len(df))
    df_duplicate = get_list_index_duplicated(df)

    for i, j in df_duplicate:
        df.at[j, "ID"] = df.at[i, "ID"]

    return df


def get_df_patient_table(df: pd.DataFrame) -> pd.DataFrame:
    df = add_id_column_to_df(df)
    df_dupli = df.duplicated(["NOM", "PRENOM", "DATE_NAISSANCE"], keep="first")
    # Keep only the patient that are not duplicated
    df = df[df_dupli == False]

    return df


def get_df_hist_table(df: pd.DataFrame) -> pd.DataFrame:
    df = add_id_column_to_df(df)
    df_dupli = df.duplicated(["NOM", "PRENOM", "DATE_NAISSANCE"], keep="first")
    # keep all the rows but put the master column
    df["master_patient_id"] = -df_dupli
    return df


def read_excel_file_patient(path: str, sheet_name: str) -> Iterable[Patient]:

    global UPLOAD_ID

    df = pd.read_excel(path, sheet_name)

    header = df.columns.ravel()
    assert [h for h in header] == [
        "NOM",
        "PRENOM",
        "DATE_NAISSANCE",
        "SEXE",
        "NOM_JEUNE_FILLE",
        "HOSPITAL_PATIENT_ID",
        "ADRESSE",
        "TEL",
        "CP",
        "VILLE",
        "PAYS",
        "DATE_MORT",
    ]

    df = get_df_patient_table(df)
    for index, row in df.iterrows():

        date_mort = parse_date_or_none(row["DATE_MORT"])
        yield (
            Patient(
                patient_num=row["ID"],
                nom=str(row["NOM"]),
                prenom=str(row["PRENOM"]),
                date_naissance=parse_date_or_none(str(row["DATE_NAISSANCE"])),
                sexe=str(row["SEXE"]),
                nom_jeune_fille=parse_str_or_none_strip(row["NOM_JEUNE_FILLE"]),
                adresse=str(row["ADRESSE"]),
                tel=str(row["TEL"]),
                code_postale=str(row["CP"]),
                ville=str(row["VILLE"]),
                pays=str(row["PAYS"]),
                date_mort=date_mort,
                code_mort=1 if date_mort else 0,
                upload_id=UPLOAD_ID,
            )
        )


def read_excel_file_patient_hist(path: str, sheet_name: str) -> Iterable[Patient]:

    global UPLOAD_ID

    df = pd.read_excel(path, sheet_name)

    header = df.columns.ravel()
    assert [h for h in header] == [
        "NOM",
        "PRENOM",
        "DATE_NAISSANCE",
        "SEXE",
        "NOM_JEUNE_FILLE",
        "HOSPITAL_PATIENT_ID",
        "ADRESSE",
        "TEL",
        "CP",
        "VILLE",
        "PAYS",
        "DATE_MORT",
    ]

    df = get_df_hist_table(df)
    for index, row in df.iterrows():

        yield (
            PatientHistorique(
                patient_num=row["ID"],
                hospital_patient_id=int(row["HOSPITAL_PATIENT_ID"]),
                origin_patient_id=path,
                upload_id=UPLOAD_ID,
                master_patient_id=int(row["master_patient_id"]),
            )
        )
