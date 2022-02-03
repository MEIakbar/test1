from math import floor, ceil
from datetime import datetime

import numpy as np
import pandas as pd
import os
import re


def dttot_extract_NIK(s):
    """
    extract NIK from Deskripsi
    """
    try:
        result = re.search('NIK(.*) ', s)
        return result.group(1)
    except:
        return np.nan

def dttot_extract_paspor(s):
    "extract paspor from Deskripsi"
    try:
        result = re.search('paspor(.*) ', s)
        return result.group(1)
    except:
        return np.nan

def all_get_list_value(s):
    return [s]

def no_data_deletation(x):
    return list(filter(('No Data').__ne__, x))

def UN_extract_orderdict_cols(df, col):
    list_value = []
    for x in df[col]:
        try:
            value = x.split(",")[-1].split("'")[1]
        except:
            value = "No Data"
        list_value.append(value)
    return list_value


def UN_extract_NATIONALITY(df, col):
    list_value = []
    for x in df[col]:
        try:
            value = x.split(",")[-1].split('"')[1]
        except:
            value = "No Data"
        list_value.append(value)
    return list_value

def UN_date_prepro(s):
    try:
        result = datetime.strptime(s, '%Y-%m-%d').strftime("%d/%m/%Y")
        return result
    except:
        return s

# def extract_OPEC_Nama(s):
#     return s.split(")")[0]
def extract_OPEC_Nama(s):
    if ")" in s:
        return s.split(")")[0]
    else:
        return s.split(";")[0]

def extract_OPEC_NIK(s):
    """
    extract NIK from Deskripsi
    """
    try:
        result = re.search('National ID No. (.*);', s)
        return result.group(1)
    except:
        return np.nan

def extract_OPEC_DOB(s):
    """
    extract DOB from Deskripsi
    """
    try:
        result = re.search('DOB (.*);', s)
        return result.group(1)
    except:
        return np.nan

def extract_OPEC_POB(s):
    """
    extract POB from Deskripsi
    """
    try:
        result = re.search('POB (.*);', s)
        return result.group(1)
    except:
        return np.nan

def create_OPEC_name_list(s):
    try:
        list_string = s.split("a.k.a.")
        result = [''.join(reversed(s.split(','))) for s in list_string]
        return result
    except:
        return [s]

def jaro_distance(s1, s2):
#     print("************")
#     print(s2)
    # lower case all the character
    s1 = s1.lower()
    s2 = s2.lower()

    s1 = s1.strip()
    s2 = s2.strip()

    # If the s are equal
    if (s1 == s2):
        return 1.0

    # Length of two s
    len1 = len(s1)
    len2 = len(s2)

    # Maximum distance upto which matching
    # is allowed
    max_dist = floor(max(len1, len2) / 2) - 1

    # Count of matches
    match = 0

    # Hash for matches
    hash_s1 = [0] * len(s1)
    hash_s2 = [0] * len(s2)

    # Traverse through the first
    for i in range(len1):

        # Check if there is any matches
        for j in range(max(0, i - max_dist),
                       min(len2, i + max_dist + 1)):

            # If there is a match
            if (s1[i] == s2[j] and hash_s2[j] == 0):
                hash_s1[i] = 1
                hash_s2[j] = 1
                match += 1
                break

    # If there is no match
    if (match == 0):
        return 0.0

    # Number of transpositions
    t = 0
    point = 0

    # Count number of occurrences
    # where two characters match but
    # there is a third matched character
    # in between the indices
    for i in range(len1):
        if (hash_s1[i]):

            # Find the next matched character
            # in second
            while (hash_s2[point] == 0):
                point += 1

            if (s1[i] != s2[point]):
                t += 1
            point += 1
    t = t//2

    # Return the Jaro Similarity
    return (match/ len1 + match / len2 +
            (match - t) / match)/ 3.0

def jaro_distance_max(row, input_nama):
    print(row)
    result = [jaro_distance(input_nama, s2) for s2 in row]
    print(result)
    percentage = round(np.max([jaro_distance(input_nama, s2) for s2 in row]), 2)
    print(percentage)
    # percentage = round(np.max([jaro_distance(input_nama, s2) for s2 in row]), 2)
    return percentage

def dttot_prepro(path):
    # read file
    print("read_excel file..")
    df_dttot = pd.read_excel(path)
    # cleaning data
    print("cleaning data..")
    df_dttot["Deskripsi"] = df_dttot["Deskripsi"].fillna("No Data")
    pass_name = ["Paspor", "Paspor", "PASPOR", "passport", "Passport"]
    nik_name = ["NIK", "nik", "nomor identifikasi nasional", "Nomor Identifikasi Nasional", "Nomor identifikasi nasional"]
    paspor_str = '|'.join(pass_name)
    nik_str = '|'.join(nik_name)

    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(paspor_str, 'paspor')
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(nik_str, 'NIK')
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(":", "")
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(";", " ")
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(".", "")
    df_dttot['Deskripsi'] = df_dttot['Deskripsi'].str.replace(",", " ")
    df_dttot['Nama'] = df_dttot['Nama'].str.lower()
    df_dttot['NIK'] = df_dttot['Deskripsi'].apply(dttot_extract_NIK)
    df_dttot['paspor'] = df_dttot['Deskripsi'].apply(dttot_extract_paspor)
    df_dttot["nama_list"] = df_dttot['Nama'].str.split("alias")
    # fillnan
    df_dttot = df_dttot.fillna("No Data")
    # filter Orang
    df_dttot = df_dttot[df_dttot["Terduga"] == "Orang"].reset_index(drop=True)
    # change columns name
    df_dttot = df_dttot.rename(columns = {"Tpt Lahir" : "Tempat Lahir",
                                          "Tgl Lahir" : "Tanggal Lahir",
                                          "WN" : "Kewarganegaraan"})
    return df_dttot

def wmd_prepro(df1, df2):
    df1 = df1.fillna("No Data")
    cols = ["Nama", "Alias 1", "Alias 2", "Alias 3", "Alias 4", "Alias 5", "Alias 6", "Alias 7", "Alias 8", "Alias 9", "Alias 10", "Alias 11"]
    df1['Alias'] = df1[cols].values.tolist()
    df1.drop(cols[1:], axis=1, inplace=True)
    df1 = df1.iloc[1:].reset_index(drop=True)

    df2 = df2.fillna("No Data")
    cols = ["Nama", "Alias 1", "Alias 2", "Alias 3", "Alias 4", "Alias 5"]
    df2['Alias'] = df2[cols].values.tolist()
    df2.drop(cols[1:], axis=1, inplace=True)
    df2 = df2.iloc[1:].reset_index(drop=True)

    df_mwd = pd.concat([df1, df2], ignore_index=True)
    df_mwd['Alias'] = df_mwd['Alias'].str.lower()
    df_mwd["nama_list"] = df_mwd['Alias']
    df_mwd = df_mwd.fillna("No Data")
    # change columns name
    df_mwd = df_mwd.rename(columns={"Informasi Lain" : "Deskripsi",
                                    "Nomor Identitas" : "NIK",
                                    "Nomor Paspor" : "paspor"})
    return df_mwd

def UK_prepro(df):
    df["Name 6"] = df["Name 6"].str.replace('"', "")
    # combine multiple columns into 1 with space seperator
    cols = ["Name 6", "Name 1", "Name 2", "Name 3", "Name 4", "Name 5"]
    df["Nama"] = df[cols].apply(lambda x: ' '.join(x.dropna()), axis=1)
    df = df.fillna("No Data")
    df["nama_list"] =  df["Nama"].apply(all_get_list_value)
    # delete No data value in nama_list column
    df['nama_list'] = df['nama_list'].apply(no_data_deletation)
    # create Nama and Alamat columns
    nama_cols = ["Name 6", "Name 1", "Name 2", "Name 3", "Name 4", "Name 5"]
    alamat_cols = ["Address 1", "Address 2", "Address 3", "Address 4", "Address 5", "Address 6"]
    df['Nama'] = df[nama_cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    df['Nama'] = df['Nama'].str.replace("No Data ", "")
    df['Nama'] = df['Nama'].str.replace(" No Data", "")
    df['Nama'] = df['Nama'].str.lower()
    df['Alamat'] = df[alamat_cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    df['Alamat'] = df['Alamat'].str.replace("No Data ", "")
    df['Alamat'] = df['Alamat'].str.replace(" No Data", "")
    # delete Name and Address columns
    df.drop(nama_cols, axis=1, inplace=True)
    df.drop(alamat_cols, axis=1, inplace=True)
    # tempat lahir
    df["Tempat Lahir"] = df["Town of Birth"] + df["Country of Birth"]
    df["Tempat Lahir"] = df["Tempat Lahir"].str.replace("No Data", "")
    # remove columns
    cols = ["Town of Birth", "Country of Birth"]
    df.drop(cols, axis=1, inplace=True)
    # rename Columns
    df = df.rename(columns={"Title" : "Gelar",
                       "DOB" : "Tanggal Lahir",
                       "Nationality" : "Kewarganegaraan",
                       "Passport Details" : "paspor",
                       "Position" : "Pekerjaan",
                       "Country" : "Negara",
                       "Other Information" : "Deskripsi"})
    return df


def UN_prepro(df):
    cols = list(df.columns)
    cols.remove("INDIVIDUAL_DATE_OF_BIRTH")
    df[cols] = df[cols].fillna("No Data")
    cols = ["FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME"]
    # create Nama column
    df['Nama'] = df[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    df['Nama'] = df['Nama'].str.replace("No Data ", "")
    df['Nama'] = df['Nama'].str.replace(" No Data", "")
    df['Nama'] = df['Nama'].str.lower()
    cols_orderdict = ["DESIGNATION", "LIST_TYPE", "LAST_DAY_UPDATED", "INDIVIDUAL_ALIAS", "INDIVIDUAL_ADDRESS",
                      "INDIVIDUAL_PLACE_OF_BIRTH", "INDIVIDUAL_DATE_OF_BIRTH", "INDIVIDUAL_DOCUMENT"]
    for col in cols_orderdict:
        df[col] = UN_extract_orderdict_cols(df, col)

    df['NATIONALITY'] = UN_extract_NATIONALITY(df, 'NATIONALITY')
    df['nama_list']= df[["Nama", "INDIVIDUAL_ALIAS"]].values.tolist()
     # delete No data value in nama_list column
    df['nama_list'] = df['nama_list'].apply(no_data_deletation)
    # delete Name columns
    cols = ["FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME", "INDIVIDUAL_ALIAS"]
    df.drop(cols, axis=1, inplace=True)
    # remove columns
    cols = ["SORT_KEY", "SORT_KEY_LAST_MOD"]
    df.drop(cols, axis=1, inplace=True)
    df['INDIVIDUAL_DATE_OF_BIRTH'] = df['INDIVIDUAL_DATE_OF_BIRTH'].apply(UN_date_prepro)
    # rename Columns
    df = df.rename(columns={"LISTED_ON" : "Listed On",
                            "COMMENTS1" : "Deskripsi",
                            "DESIGNATION" : "Gelar",
                            "NATIONALITY" : "Kewarganegaraan",
                            "LAST_DAY_UPDATED" : "Last Updated",
                            "INDIVIDUAL_ADDRESS" : "Alamat",
                            "INDIVIDUAL_DATE_OF_BIRTH" : "Tanggal Lahir",
                            "INDIVIDUAL_PLACE_OF_BIRTH" : "Tempat Lahir",
                            "INDIVIDUAL_DOCUMENT" : "Document",
                            "NAME_ORIGINAL_SCRIPT" : "Name Original Script",
                            "GENDER" : "Gender",
                            "TITLE" : "Gelar_2"})
    return df


def OPEC_prepro(df_OPEC):
    df_OPEC = df_OPEC.rename(columns={"nama_list" : "Deskripsi"})
    df_OPEC = df_OPEC[df_OPEC["Deskripsi"].str.contains("individual")].reset_index(drop=True)
    df_OPEC = df_OPEC.iloc[1:].reset_index(drop=True)
    df_OPEC["Nama"] = df_OPEC['Deskripsi'].apply(extract_OPEC_Nama)
    df_OPEC["Nama"] = df_OPEC["Nama"].str.replace("\n", ",")
    df_OPEC["Nama"] = df_OPEC["Nama"].str.replace(";", "")
    df_OPEC["Nama"] = df_OPEC["Nama"].str.replace("(", "")
    df_OPEC["Nama"] = df_OPEC["Nama"].str.replace('"', "")
    df_OPEC['Nama'] = df_OPEC['Nama'].str.lower()

    df_OPEC['NIK'] = df_OPEC['Deskripsi'].apply(extract_OPEC_NIK)
    df_OPEC['Tanggal Lahir'] = df_OPEC['Deskripsi'].apply(extract_OPEC_DOB)
    df_OPEC['Tempat Lahir'] = df_OPEC['Deskripsi'].apply(extract_OPEC_POB)
    df_OPEC["nama_list"] = df_OPEC['Nama'].apply(create_OPEC_name_list)
    df_OPEC = df_OPEC.drop_duplicates(subset=['Nama']).reset_index(drop=True)
    df_OPEC = df_OPEC.fillna("No Data")
    return df_OPEC


def get_similarity(df, input_name, treshold_value):
    print("calculate distance")
    df['similarity'] = df['nama_list'].apply(lambda x: jaro_distance_max(x, input_name))
    df = df.sort_values(by="similarity", ascending=False).reset_index(drop=True)
    return df
