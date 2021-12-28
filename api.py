import time
import json
import logging
import uvicorn
import pandas as pd
from pydantic import BaseModel
from typing import Optional
from math import floor, ceil
from fastapi import BackgroundTasks, FastAPI
import warnings
import re
from scraparazzie import scraparazzie
from bs4 import BeautifulSoup
import requests

warnings.filterwarnings("ignore")

app = FastAPI()

class Userinput(BaseModel):
    Nama: str
    NIK: Optional[str]=None
    DOB: Optional[str]=None
    POB: Optional[str]=None

def get_constraint():
    """
    get constraint / schema data for pep
    output : DataFrame
    """
    file_path = "./data/pep_scenario.xlsx"
    df = pd.read_excel(file_path)
    return df

def get_all_data():
    """
    get data
    output : DataFrame
    """
    df = pd.read_csv("./data/Test_all_data_pep.csv")
    df = df.fillna("No Data")
    return df

def DOB_similarity(df, col, dob_input):
    """
    filter DOB column
    output : DataFrame
    """
    df = df[df[col].str.contains(dob_input)].reset_index(drop=True)
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

def treatment_constraint(nama_status, dob_status, pob_status):
    df = get_constraint()
    dict_value = {"nama" :nama_status,
                "dob" : dob_status,
                "pob" : pob_status}
    result = df.loc[(df[list(dict_value)] == pd.Series(dict_value)).all(axis=1)]
    result_recommendation =  list(set(result["recommendation"]))[0]
    return result_recommendation


def jaro_distance(s1, s2):
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

    # Count number of occurrences where two characters match but
    # there is a third matched character in between the indices
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

def define_list():
    list_gelar_depan = [" kph ", " cn ", " ust ", " drg ", " tgh ", " mayjen tni purn ", " capt "," brigjen tni purn", " h ", " hj ", " kh ", " dr ",
                    " dra ", " drs ", " prof ", " ir ", " jenderal pol  purn ", "  c  ", " hc ", " krt ", " mayjen tni  mar  purn ", " st ", " tb ",
                    " hc ", " drh ", " irjen  pol  purn ", " pdt ", " marsekal tni purn ", " k ", " letnan jenderal tni purn ", " laksdya  tni purn ",
                    " irjen pol purn ", " mayjen tni mar  purn "]

    return list_gelar_depan

# def news_filter(Nama):
#     print("Get Google News with query {}...".format(Nama))
#     list_keyword = [" partai", " politik", " partai politik", " dpr", " mpr", " anggota dpr", " anggota mpr", " pelantikan",
#                 " presiden", " menteri", " pemilihan", " pilkada", " pemilu"]
#     news_query = scraparazzie.NewsClient(language = 'indonesian', location = 'Indonesia', query = Nama, max_results = 100)
#     news_output = news_query.export_news()
#     list_title = [" " + x["title"] for x in news_output]
#
#     list_idx = []
#     for idx, url_string in enumerate(list_title):
#         if any(ext in url_string for ext in list_keyword):
#             list_idx.append(news_output[idx])
#
#     top_ten = news_output[:10]
#
#     return list_idx, top_ten

def get_url(url):
    headers = {
         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
     }
    r = requests.get(url, headers=headers)  # Using the custom headers we defined above
    soup = BeautifulSoup(r.content, 'html.parser')
    return soup


def extract_funct(soup, summary):
    for container in soup.findAll('div', {"class":'tF2Cxc'}):
        heading = container.find('h3', {"class" : 'LC20lb MBeuO DKV0Md'}).text
        link = container.find('a')['href']

        summary.append({
          'Heading': heading,
          'Link': link,
        })
    return summary


def get_google(Nama):
    print("query for {} from google...".format(Nama))
    # query for 1st page
    url = "https://www.google.co.id/search?q={}".format(Nama)
    soup = get_url(url)
    summary = []
    res = extract_funct(soup, summary)

    # query for 2nd page
    base_url = "https://www.google.co.id"
    page_2 = soup.find('a', {"aria-label":'Page 2'})['href']
    url2 = base_url+page_2
    soup2 = get_url(url)
    res = extract_funct(soup2, res)

    return res[:10]



@app.get('/PEP/')
async def dprd_tk1(Nama, DOB: Optional[str]=None, POB: Optional[str]=None):
    query = Nama
    nama_status = "not match"
    dob_status = "not match"
    pob_status = "not match"

    list_gelar_depan = define_list()
    regex = re.compile('[^a-zA-Z]')

    # Nama preprcessing
    Nama = Nama.lower()
    Nama = Nama.replace(",", " ")
    Nama = Nama.replace(".", " ")
    Nama = " " + Nama
    filter_str = '|'.join(list_gelar_depan)
    for x in range(5):
        Nama = re.sub(filter_str, ' ', Nama)
    Nama_prepro = regex.sub('', Nama)

    df = get_all_data()
    df["score"] = df['only_character'].apply(lambda x: jaro_distance(x, Nama_prepro))
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)

    # filter nama
    df_nama = df[df["score"] >= 0.75].reset_index(drop=True)
    if df_nama.shape[0] > 0:
        nama_status = "match"
        if DOB is not None:
            DOB = DOB.strip()
            DOB = DOB.lower()
            df_DOB = DOB_similarity(df_nama, 'tanggal lahir', DOB)
            if df_DOB.shape[0] > 0:
                df_nama = df_DOB
                dob_status = "match"

        if POB is not None:
            POB = POB.strip()
            POB = POB.lower()
            df_POB = POB_similarity(df_nama, 'tempat lahir', POB)
            if df_POB.shape[0] > 0:
                df_nama = df_POB
                pob_status = "match"
        df_show = df_nama.copy()
        df_show = df_show.head(10)
    else:
        df_show = df_nama.copy()
        df_show = df_show.head(10)
    reccomendation = treatment_constraint(nama_status, dob_status, pob_status)


    if df_show.shape[0] > 0:
        query = df['only_character'][0]
    else:
        pass


    if reccomendation == "Phase 2" or reccomendation == "PEP":
        # list_idx, top_ten = news_filter(df_show["Nama"][0])
        try:
            top_ten = get_google(query)
        except:
            top_ten = []
    else:
        # list_idx = []
        top_ten = []

    cols = ["Nama", "tempat lahir", "tanggal lahir", "score"]
    df_show = df_show[cols]

    respond_out = {
        "Recommendation" : reccomendation,
        "User_Input" : Nama_prepro,
        "Output" : df_show,
        # "Filtered_News" : list_idx,
        "Top_10_Google_Search" : top_ten
    }
    return respond_out


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
