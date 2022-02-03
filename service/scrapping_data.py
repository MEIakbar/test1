import os
import json
import glob
import requests
import traceback
import pandas as pd
import urllib3
import xmltodict

from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve, quote
from urllib.parse import urljoin



class get_data():

    def __init__(self, all_config_dict):
        for key in all_config_dict:
            self.error_desc= []
            self.failed_list = []
            self.success_list = []
            setattr(self, key, all_config_dict[key])

    @staticmethod
    def parse_config(auth_dict):
        """
        input function: selecting parameters from input file
        parameter required :
        1. type [required]: the file type must be .json

        :return: dictionary
        """
        list_dict_config = []
        all_config_dict = {
            "url_dttot": auth_dict['url']['ppatk'],
            "url_wmd": auth_dict['url']['ppatk'],
            "url_uk": auth_dict['url']['uk'],
            "url_un": auth_dict['url']['un'],
            "url_opec": auth_dict['url']['opec']
        }
        list_dict_config.append(all_config_dict)

        return list_dict_config


    @staticmethod
    def load_config(json_path):
        """
        load Config from JSON file
        """
        f = open(json_path)
        json_config = json.load(f)

        return json_config


    @classmethod
    def load_config_json(cls, json_path):
        auth_json = cls.load_config(json_path)

        return cls(*cls.parse_config(auth_json))


    def get_request(self, url):
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
        r=requests.get(url, headers=headers, verify=False)
        soup1 = BeautifulSoup(r.content, 'html5lib')

        return soup1


    def download_DTTOT(self, link_name):
        find_all_a = link_name.find_all(href=True)
        list_excel = []
        for ele in find_all_a:
            link_donwload = ele.get('href')
            if ".xlsx" in link_donwload:
                file_excel = ele.get('href')
                list_excel.append(file_excel)

        filename = file_excel.rsplit('/', 1)[-1]
        print("Downloading %s to %s..." % (file_excel, filename) )
        urlretrieve(file_excel, filename)
        print("Done.")


    def download_uk(self, link_name):
        find_all_a = link_name.find_all(href=True)
        list_excel = []
        for ele in find_all_a:
            link_donwload = ele.get('href')
            if ".csv" in link_donwload:
                file_excel = ele.get('href')
                list_excel.append(file_excel)

        filename = file_excel.rsplit('/', 1)[-1]
        print("Downloading %s to %s..." % (file_excel, filename) )
        urlretrieve(file_excel, filename)
        print("Done.")

        df = pd.read_csv("ConList.csv", encoding ="ISO-8859-1", header=1)
        return df


    def getxml_un(self, url_un):
        http = urllib3.PoolManager()
        response = http.request('GET', url_un)
        try:
            data = xmltodict.parse(response.data)
        except:
            print("Failed to parse xml from response (%s)" % traceback.format_exc())
        return data


    def get_dttot(self):
        try:
            soup1 = self.get_request(self.url_dttot)
            html = list(soup1.children)[1]
            body = list(html.children)[2]
            body2 = list(body.children)[13]
            body3 = list(body2.children)[8]
            body4 = list(body3.children)[1]
            body5= body4.contents[1].contents[3].contents[11]

            # DTTOT
            DTTOT_link = body5.contents[7]
            self.download_DTTOT(DTTOT_link)
            # WMD
            WMD_link = body5.contents[15]
            self.download_DTTOT(WMD_link)
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("download_DTTOT failed..")


    def get_opec(self):
        try:
            print("Srapping for OPEC...")
            text = []
            soup = self.get_request(self.url_opec)
            spans = soup.find_all('body')
            for span in spans:
                text.append(span.string)
            list_text = text[0].split("\n\n")
            df = pd.DataFrame(list_text, columns=['nama_list'])
            df = df.iloc[:23888]
            df["source"] = "OPEC"
            df.to_csv("OPEC_list.csv", index=False)
            print("Done")
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("download_opec failed..")


    def get_uk(self):
        try:
            print("Srapping for UK...")
            soup = self.get_request(self.url_uk)
            df = self.download_uk(soup)
            df.to_csv("UK_list.csv", index=False)
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("download_uk failed..")


    def get_un(self):
        try:
            file =  self.getxml_un(self.url_un)
            data = file['CONSOLIDATED_LIST']['INDIVIDUALS']['INDIVIDUAL']
            df = pd.DataFrame.from_dict(data)
            df.to_csv("UN_list.csv", index=False)
        except Exception as e:
            self.error_desc.append(e)
            self.failed_list.append("download_uk failed..")


    def get_data(self):
        self.get_dttot()
        self.get_opec()
        self.get_uk()
        self.get_un()
        df = pd.DataFrame.from_dict({"status": self.failed_list,
                                    "error desc": self.error_desc})
        df.to_excel("./log_scrapping.xlsx", index=False)