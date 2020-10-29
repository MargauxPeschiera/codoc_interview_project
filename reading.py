#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 09:51:47 2020

@author: margauxpeschiera
"""

import pandas as pd

from typing import NamedTuple, Optional, Iterable
from datetime import date, datetime


# The upload_id is 1 for this insertion, it will change for other insertions.
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
    """
    Function to parse a string into a date format or None if the string is empty.

    Parameters
    ----------
    xldate : str
        String to parse into a date fromat.

    Returns
    -------
    Optional[date]
        Return the date correpsonding to xldate or None.

    """
    if pd.isna(xldate):
        return None
    else:
        date_time = datetime.strptime(xldate, "%d/%m/%Y")
        return date_time.date()


def parse_str_or_none_strip(str_value: str) -> Optional[str]:
    """
    Function to check if a string is present or if it's empty.

    Parameters
    ----------
    str_value : str
        String to parse into a date fromat.

    Returns
    -------
    Optional[str]
        Return the string correpsonding to str_value or None.

    """

    if pd.isna(str_value):
        return None
    else:
        return str(str_value.strip())


def get_list_index_duplicated(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to get a list containing tuples. The tuples represent the index of rows that are duplicated.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe from which we want to get the index of the duplicated rows.

    Returns
    -------
    list_index : TYPE
        List containing the index of duplicated rows.

    """
    df = df[["NOM", "PRENOM", "DATE_NAISSANCE"]]
    df = df[df.duplicated(["NOM", "PRENOM", "DATE_NAISSANCE"], keep=False)]
    list_index = (
        df.groupby(df.columns.values.tolist()).apply(lambda x: tuple(x.index)).tolist()
    )

    return list_index


def add_id_column_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add an "ID" column to the dataframe, it will put the same ID for duplicated rows.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe we will add the "ID" column too.

    Returns
    -------
    df : TYPE
        Dataframe with the ID column.

    """
    df["ID"] = range(0, len(df))
    df_duplicate = get_list_index_duplicated(df)

    for i, j in df_duplicate:
        df.at[j, "ID"] = df.at[i, "ID"]

    return df


def get_df_patient_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get a dataframe corresponding to the rows we will insert into the patient table (remove the duplicated rows).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to transform into a dataframe with ID column (and removing the duplicates).

    Returns
    -------
    df : TYPE
        Dataframe with ID column and without duplicates.

    """
    df = add_id_column_to_df(df)
    df_dupli = df.duplicated(["NOM", "PRENOM", "DATE_NAISSANCE"], keep="first")
    # Remove the duplicated rows
    df = df[df_dupli == False]

    return df


def get_df_hist_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get a dataframe corresponding to the rows we will insert into the patient_ipphist table.
    We want to keep the duplicates and add a master column with true/false.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to trasnform.

    Returns
    -------
    df : TYPE
        Dataframe with ID column and master_patient_id column

    """
    df = add_id_column_to_df(df)
    df_dupli = df.duplicated(["NOM", "PRENOM", "DATE_NAISSANCE"], keep="first")
    # keep all the rows but put the master column with true/false
    df["master_patient_id"] = -df_dupli
    return df


def read_excel_file_patient(path: str, sheet_name: str) -> Iterable[Patient]:
    """
    Process the excel file for patient table, returning an Iterable of Patient tuple.

    Parameters
    ----------
    path : str
        Path to the file to process.
    sheet_name : str
        Name of the sheet to process into the excel file.

    Yields
    ------
    Iterable[Patient]
        Iterable containing Patient tuple and ready to be insert into Patient table.

    """

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
    """
     Process the excel file for patient_ipphist table, returning an Iterable of PatientHistorique tuple.

    Parameters
    ----------
    path : str
        Path to the file to process.
    sheet_name : str
        Name of the sheet to process into the excel file.

    Yields
    ------
    Iterable[Patient]
        Iterable containing PatientHistorique tuple and ready to be insert into Patient_ipphist table.

    """

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
