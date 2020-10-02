from django.shortcuts import render
from concurrent.futures import ThreadPoolExecutor

from meal.api.openAPI import SchoolInfo

import threading
import json

import datetime
import pymongo


atpt_ofcdc_sc_code_set = ('B10', 'C10', 'D10', 'E10', 'F10', 'G10', 'H10', 'I10', 'J10', 'K10', 'M10', 'N10', 'P10',
                          'Q10', 'R10', 'S10', 'T10', 'V10')
atpt_ofcdc_sc_name_set = ('서울특별시', '부산광역시', '대구광역시', '인천광역시', '광주광역시', '대전광역시', '울산광역시', '세종특별자치시',
                          '경기도', '강원도', '충청북도', '충청남도', '전라북도', '전라남도', '경상북도', '경상남도', '제주특별자치도', '재외한국학교')
school_data_list = list()
today = str(datetime.date.today()).replace('-', '')

class School_Info:
    '''
    '''

    _instance = None
    # 몽고디비연결
    client = pymongo.MongoClient('mongodb+srv://reso:1q2w3e4r@reso.ympo0.mongodb.net/school_meal?retryWrites=true&w=majority')
    # collection 생성
    database = client['school_meal']['school_info']

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    @classmethod
    def get_school_info_from_collection(cls, *_query):
        assert cls.database
        return cls.database.find(_query[0], _query[1])

    @classmethod
    def add_school_info_to_collection(cls, _data):
        if type(_data) is list:
            return cls.database.insert_many(_data)
        else:
            return cls.database.insert_one(_data)




def fetch_school_data(atpt_ofcdc_sc_code):
    school = SchoolInfo(atpt_ofcdc_sc_code)
    # print('Thread Name :', threading.current_thread().getName(), 'Start', atpt_ofcdc_sc_code)
    check = school.call_data()
    # print('Thread Name :', threading.current_thread().getName(), 'Done', atpt_ofcdc_sc_code)
    if check:
        tmp = {
            'ATPT_OFCDC_SC_CODE':atpt_ofcdc_sc_code,
            'DATA':school.get_school_data_list(),
            'UPDATE':today
        }
        school_data_list.append(tmp)

def fetch_school_data_db(data):
    # print('Thread Name :', threading.current_thread().getName(), 'Start', atpt_ofcdc_sc_code)
    School_Info.add_school_info_to_collection(data)
    # print('Thread Name :', threading.current_thread().getName(), 'Done', atpt_ofcdc_sc_code)


def school_data_init():

    with ThreadPoolExecutor(max_workers=20) as executor:
        for code in atpt_ofcdc_sc_code_set:
            executor.submit(fetch_school_data, code)

    if len(atpt_ofcdc_sc_code_set) == len(school_data_list):
        with ThreadPoolExecutor(max_workers=20) as executor:
            for data_dict in school_data_list:
                executor.submit(fetch_school_data_db, data_dict)

    # convert into JSON:
    # y = json.dumps(data_json)

def get_local_school_list():
    local_school_list = list()
    # sql_query_0 = {'stdday': find_date}  # mongoDB find sql 의 query
    # sql_query_1 = {'_id': 0}  # mongoDB find sql의 projection

    # School_Info.get_school_info_from_collection()


def send_api(request):
    school_data_init() # 학교 정보

    # data
    context ={'aa':'k'}

    return render(request, 'index.html', context)