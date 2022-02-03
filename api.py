import time
import json
import logging
import uvicorn
import pandas as pd
from ast import literal_eval
from pydantic import BaseModel
from typing import Optional
from service.utility import get_similarity
from datetime import datetime

import secrets
from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

import warnings
warnings.filterwarnings("ignore")

app = FastAPI()
security = HTTPBasic()

class Userinput(BaseModel):
    Nama: str
    NIK: Optional[str]=None
    DOB: Optional[str]=None
    POB: Optional[str]=None

def get_constraint():
    """
    get constraint / schema data for DTTOT
    output : DataFrame
    """
    file_path = "./data/Constraint_PPATK.csv"
    df = pd.read_csv(file_path)
    return df

def get_all_data():
    """
    get data
    output : DataFrame
    """
    df = pd.read_excel("./data/all_data.xlsx")
    df['nama_list'] = df['nama_list'].apply(literal_eval)
    df = df.fillna('no data')
    return df

def get_input_char(df, nama):
    """
    filter nama column based on the first 4 character for each word
    output : DataFrame
    """
    input_char = ''.join([i[:4] for i in nama.strip().split(' ')])
    df = df[df["4_char"].str.contains(input_char)].reset_index(drop=True)
    return df

def DOB_similarity(df, col, dob_input):
    """
    filter DOB column
    output : DataFrame
    """
    df = df[df[col].str.contains(dob_input)].reset_index(drop=True)
    return df

def NIK_similarity(df, col, NIK_input):
    """
    filter NIK column
    output : DataFrame
    """
    df = df[df[col].str.contains(NIK_input)].reset_index(drop=True)
    return df

def POB_similarity(df, col, pob_input):
    """
    filter POB column
    output : DataFrame
    """
    try:
        df = df[df[col].str.contains(pob_input)].reset_index(drop=True)
    except:
        df = df[df[col].str.contains(pob_input).fillna(False)].reset_index(drop=True)
    return df

def nama_similarity(df, input_nama, treshold_value):
    """
    get similarity value for nama column
    output : DataFrame
    """
    df = get_similarity(df, input_nama, treshold_value)
    return df

def to_json(df):
    return df.to_json(orient='records')

def treatment_constraint(nama_status, nik_status, dob_status, pob_status):
    df = get_constraint()
    dict_value = {"nama" :nama_status,
                "nik" : nik_status,
                "dob" : dob_status,
                "pob" : pob_status}
    result = df.loc[(df[list(dict_value)] == pd.Series(dict_value)).all(axis=1)]
    result_recommendation =  list(set(result["recommendation"]))[0]
    return result_recommendation

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, "app")
    correct_password = secrets.compare_digest(credentials.password, "mnc123456")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def main_funct(Nama, NIK, DOB, POB):
    # if the user fill with empty string trun it into None
    if Nama is not None:
        if Nama.isspace() or Nama == "":
            Nama = None
    if NIK is not None:
        if NIK.isspace() or NIK == "":
            NIK = None
    if DOB is not None:
        if DOB.isspace() or DOB == "":
            DOB = None
    if POB is not None:
        if POB.isspace() or POB == "":
            POB = None
    # initialization some variable
    nama_status = "not match"
    nik_status = "not match"
    dob_status = "not match"
    pob_status = "not match"
    alamat_status = "not match"
    Similarity_Percentage = 0.8
    dict_filter = {}

    # get data
    print("Getting Data...")
    start_time = time.time()
    df = get_all_data()
    print("--- %s seconds ---" % (time.time() - start_time))

    print("filter name...")
    start_time = time.time()
    # filter nama berdasarkan 4 character awal untuk setiap kata
    if Nama is not None:
        Nama = Nama.strip()
        Nama = Nama.lower()
        df_nama = get_input_char(df, Nama)
        if df_nama.shape[0] > 0:
            df = df_nama.copy()
            dict_filter["nama"] = Nama
            nama_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # filter NIK_input
    print("Filter NIK...")
    start_time = time.time()
    if NIK is not None:
        NIK = NIK.strip()
        NIK = NIK.lower()
        print(df.shape)
        if df_nama.shape[0] > 0:
            df = df_nama.copy()
        print(df.shape)
        df_NIK = NIK_similarity(df, 'nik', NIK)
        if df_NIK.shape[0] > 0:
            df = df_NIK.copy()
            dict_filter["nik"] = NIK
            if len(NIK) <= 14:
                nik_status = "not match"
            else:
                nik_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # filter DOB_similarity
    print("Filter DOB...")
    start_time = time.time()
    if DOB is not None:
        DOB = DOB.strip()
        DOB = DOB.lower()
        df_DOB = DOB_similarity(df_nama, 'tanggal lahir', DOB)
        if NIK is not None:
            if df_NIK.shape[0] > 0:
                df_DOB = DOB_similarity(df_NIK, 'tanggal lahir', DOB)
        if df_DOB.shape[0] > 0:
            df_nama = df_DOB.copy()
            dob_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # filter POB_similarity
    print("Filter POB...")
    start_time = time.time()
    if POB is not None:
        POB = POB.strip()
        POB = POB.lower()
        df_POB = POB_similarity(df_nama, 'tempat lahir', POB)
        if NIK is not None:
            if df_NIK.shape[0] > 0:
                df_POB = POB_similarity(df_NIK, 'tempat lahir', POB)
        if df_POB.shape[0] > 0 :
            df = df_POB.copy()
            dict_filter["pob"] = POB
            pob_status = "match"
    print("--- %s seconds ---" % (time.time() - start_time))
    # set Note output
    statusList = [nama_status, nik_status, dob_status, pob_status, alamat_status]
    if 'match' in (statusList):
        df_outp = df.copy()
        cols = ["nama", "nama_list", "nik", "tanggal lahir", "tempat lahir", "kewarganegaraan", "paspor", "alamat"]
        df_outp = df_outp[cols]
        outp = to_json(df_outp)
    else:
        outp = None
    # get Similarity_Score
    simalarity_value = None
    if nama_status == "match":
        df = nama_similarity(df, Nama, Similarity_Percentage)
        simalarity_value = df["similarity"][0]
        if simalarity_value < 0.8:
            nama_status = "not match"
    reccomendation = treatment_constraint(nama_status, nik_status, dob_status, pob_status)
    if simalarity_value == None:
        simalarity_value == 0.00

    return reccomendation, simalarity_value, nik_status, dob_status, pob_status, outp


@app.get('/PPATK/', dependencies=[Depends(get_current_username)])
async def dttot(Nama, NIK: Optional[str]=None, DOB: Optional[str]=None, POB: Optional[str]=None):
    reccomendation, simalarity_value, nik_status, dob_status, pob_status, outp = main_funct(Nama, NIK, DOB, POB)
    respond_out = {
        "Recommendation" : reccomendation,
        "Nama Similarity" : simalarity_value,
        "NIK" : nik_status,
        "DOB" : dob_status,
        "POB" : pob_status
    }
    return respond_out


@app.post('/PPATK/')
async def dttot(item: Userinput):
    Nama = item.Nama
    NIK = item.NIK
    DOB = item.DOB
    POB = item.POB
    # Alamat = item.Alamat
    reccomendation, simalarity_value, nik_status, dob_status, pob_status, outp = main_funct(Nama, NIK, DOB, POB)
    respond_out = {
        "Recommendation" : reccomendation,
        "Nama Similarity" : simalarity_value,
        "NIK" : nik_status,
        "DOB" : dob_status,
        "POB" : pob_status
    }
    return respond_out


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
