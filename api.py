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
warnings.filterwarnings("ignore")

app = FastAPI()

class Userinput(BaseModel):
    Nama: str
    NIK: Optional[str]=None
    DOB: Optional[str]=None
    POB: Optional[str]=None

def get_all_data():
    """
    get data
    output : DataFrame
    """
    df = pd.read_excel("./data/remove_depan.xlsx")
    df = df.fillna("No Data")
    return df

def jaro_distance(s1, s2):
    print("Calculate Jaro Distance")
    print(s1)
    print(s2)
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

def define_list():
    list_gelar_depan = [" kph ", " cn ", " ust ", " drg ", " tgh ", " mayjen tni purn ", " capt "," brigjen tni purn", 
                        " h ", " hj ", " kh ", " dr ", " dra ", " drs ", " prof ", " ir "]
    
    return list_gelar_depan


@app.get('/PEP/')
async def dprd_tk1(Nama):
    list_gelar_depan = define_list()
    regex = re.compile('[^a-zA-Z]')
    Nama = regex.sub('', Nama)
    
    Nama = Nama.lower()
    Nama = Nama.replace(",", " ")
    Nama = Nama.replace(".", " ")
    Nama = " " + Nama
    filter_str = '|'.join(list_gelar_depan)
    for x in range(5):
        Nama = re.sub(filter_str, ' ', Nama)
    Nama = regex.sub('', Nama)

    df = get_all_data()
    df["score"] = df['only_character'].apply(lambda x: jaro_distance(x, Nama))
    df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
    df_show = df.copy()
    df_show = df_show.head(10)

    respond_out = {
        "User Input" : Nama,
        "Output" : df_show
    }
    return respond_out


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8090, log_level="info", reload=True)

# to run python api.py
# go here http://127.0.0.1:8090/docs
