#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 12:34:21 2020

@author: margauxpeschiera
"""
import sqlite3
from tqdm import tqdm
from typing import Iterable

from reading import (
    read_excel_file_patient,
    read_excel_file_patient_hist,
    Patient,
    PatientHistorique,
)

import document


def get_connection(db: str):
    try:
        connection = sqlite3.connect(db)
    except sqlite3.Error as error:
        print(error)
    return connection


def delete_rows_db(connection):

    cursor = connection.cursor()
    cursor.execute("DELETE FROM DWH_PATIENT")
    cursor.execute("DELETE FROM DWH_PATIENT_IPPHIST")
    cursor.execute("DELETE FROM DWH_DOCUMENT")
    connection.commit()


def insert_into_dwh_patient_ipphist(
    connection, patients: Iterable[PatientHistorique]
) -> int:
    cursor = connection.cursor()
    format_str = """
        INSERT INTO DWH_PATIENT_IPPHIST(
            PATIENT_NUM, 
            HOSPITAL_PATIENT_ID, 
            ORIGIN_PATIENT_ID, 
            MASTER_PATIENT_ID, 
            UPLOAD_ID
            ) 
        VALUES ("{patient_num}",
                "{hpi}", 
                "{opi}", 
                "{mpi}", 
                "{upload_id}"
                
                );"""
    for p in patients:
        sql_command = format_str.format(
            patient_num=p.patient_num,
            hpi=p.hospital_patient_id,
            opi=p.origin_patient_id,
            mpi=p.master_patient_id,
            upload_id=p.upload_id,
        )

        cursor.execute(sql_command)

    connection.commit()

    return cursor.lastrowid


def insert_into_dwh_patient(connection, patients: Iterable[Patient]) -> int:
    cursor = connection.cursor()

    format_str = """
        INSERT INTO DWH_PATIENT(
            PATIENT_NUM, 
            LASTNAME, 
            FIRSTNAME, 
            BIRTH_DATE, 
            SEX, 
            MAIDEN_NAME, 
            RESIDENCE_ADDRESS, 
            PHONE_NUMBER, 
            ZIP_CODE, 
            RESIDENCE_CITY, 
            DEATH_DATE, 
            RESIDENCE_COUNTRY, 
            DEATH_CODE, 
            UPLOAD_ID
            ) 
        VALUES ("{patient_num}",
                "{last_name}", 
                "{first_name}", 
                "{birth_date}", 
                "{sex}",
                "{nom_jf}",
                "{adresse}",
                "{tel}",
                "{zipcode}",
                "{ville}",
                "{date_mort}",
                "{pays}",
                "{death_code}",
                "{upload_id}"
                
                );"""

    for p in patients:
        sql_command = format_str.format(
            patient_num=p.patient_num,
            last_name=p.nom,
            first_name=p.prenom,
            birth_date=p.date_naissance,
            sex=p.sexe,
            nom_jf=p.nom_jeune_fille,
            adresse=p.adresse,
            tel=p.tel,
            zipcode=p.code_postale,
            ville=p.ville,
            date_mort=p.date_mort,
            pays=p.pays,
            death_code=p.code_mort,
            upload_id=p.upload_id,
        )

        cursor.execute(sql_command)

    connection.commit()

    return cursor.lastrowid


def insert_document(connection, documents: Iterable[document.Doc]) -> int:
    cursor = connection.cursor()

    format_str = """
        INSERT INTO DWH_DOCUMENT(
            PATIENT_NUM, 
            DOCUMENT_NUM, 
            DOCUMENT_DATE, 
            UPDATE_DATE, 
            DOCUMENT_ORIGIN_CODE,
            DISPLAYED_TEXT, 
            AUTHOR
            ) 
        VALUES ("{patient_num}",
                "{doc_num}", 
                "{doc_date}", 
                "{update_date}", 
                "{doc_ori_code}", 
                "{text}",
                "{author}"
                
                );"""
    for d in documents:
        sql_command = format_str.format(
            patient_num=d.patient_num,
            doc_num=d.doc_num,
            doc_date=d.doc_date,
            update_date=d.update_date,
            doc_ori_code=d.doc_origine_code,
            text=d.display_text,
            author=d.author,
        )
        cursor.execute(sql_command)

    connection.commit()
    return cursor.lastrowid


def get_patient_num_from_ipp(ipp: int) -> int:
    """
    Get patient_num from DWH_PATIENT_IPPHIST table knowing the ipp.

    Parameters
    ----------
    ipp : int
        Ipp of the patient that we want to get the patient_num.

    Returns
    -------
    int
        Patient_num.

    """
    connection = get_connection("drwh.db")

    cursor = connection.cursor()
    format_str = """SELECT PATIENT_NUM FROM DWH_PATIENT_IPPHIST WHERE HOSPITAL_PATIENT_ID="{ipp}" AND MASTER_PATIENT_ID=1"""
    sql_command = format_str.format(ipp=ipp)
    cursor.execute(sql_command)
    row = cursor.fetchone()

    connection.close()
    return row[0]


if __name__ == "__main__":

    connection = get_connection("drwh.db")

    delete_rows_db(connection)

    patients = read_excel_file_patient(
        "fichiers source/export_patient.xlsx", "Export Worksheet"
    )

    for i in tqdm(
        range(insert_into_dwh_patient(connection, patients)), desc="Inserting patient"
    ):
        pass

    patients = read_excel_file_patient_hist(
        "fichiers source/export_patient.xlsx", "Export Worksheet"
    )

    for i in tqdm(
        range(insert_into_dwh_patient_ipphist(connection, patients)),
        desc="Inserting patient ipphist",
    ):
        pass

    docs = document.parse_all_files("fichiers source/")

    for i in tqdm(range(insert_document(connection, docs)), desc="Inserting document"):
        pass

    connection.close()
