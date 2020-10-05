from django.shortcuts import render
from concurrent.futures import ThreadPoolExecutor
from django.http import HttpResponse
from meal.api.openAPI import SchoolInfo, MealInfo
from meal.DBmanager import School_Info_DB, BASED_INFO, USER_INFO
import threading
import json
import requests
import datetime

school_data_list = list()
ATPT_OFCDC_SC_CODE_LIST = list()
ATPT_OFCDC_SC_NAME_LIST = list()
LAST_UPDATE_DATE = ''

TODAY = str(datetime.date.today()).replace('-', '')
TOMORROW = str(datetime.date.today()+datetime.timedelta(days=1)).replace('-', '')
NEXT_WEEK = str(datetime.date.today()+datetime.timedelta(days=7)).replace('-', '')


def init_database():
    '''
    작성 : 서율
    기능 : 최초 데이터 초기화
    '''
    atpt_ofcdc_sc_code_tuple = ('B10', 'C10', 'D10', 'E10', 'F10', 'G10', 'H10', 'I10', 'J10', 'K10', 'M10', 'N10', 'P10',
                          'Q10', 'R10', 'S10', 'T10')
    atpt_ofcdc_sc_name_tuple = ('서울특별시', '부산광역시', '대구광역시', '인천광역시', '광주광역시', '대전광역시', '울산광역시', '세종특별자치시',
                          '경기도', '강원도', '충청북도', '충청남도', '전라북도', '전라남도', '경상북도', '경상남도', '제주특별자치도')
    data_dict = {
        'ATPT_OFCDC_SC_CODE':atpt_ofcdc_sc_code_tuple,
        'ATPT_OFCDC_SC_NAME':atpt_ofcdc_sc_name_tuple,
        'UPDATE_DATE':TODAY
    }
    last_date = {
        'last_date':TODAY
    }
    BASED_INFO.add_based_info_to_collection(data_dict)
    BASED_INFO.add_based_info_to_collection(last_date)


def init_data():
    '''
    작성 : 서율
    기능 : 전역변수 초기화
    '''
    global ATPT_OFCDC_SC_CODE_LIST
    global ATPT_OFCDC_SC_NAME_LIST
    global LAST_UPDATE_DATE

    sql_query_0 = {'last_date': {'$exists': 0}}  # mongoDB find sql 의 query
    sql_query_1 = {'_id': 0}
    db_cursor = BASED_INFO.get_based_info_from_collection(sql_query_0, sql_query_1)
    db_dict = dict(list(db_cursor)[0])

    ATPT_OFCDC_SC_CODE_LIST = db_dict['ATPT_OFCDC_SC_CODE']
    ATPT_OFCDC_SC_NAME_LIST = db_dict['ATPT_OFCDC_SC_NAME']

    sql_query_0 = {'last_date': {'$exists': 1}}  # mongoDB find sql 의 query
    sql_query_1 = {'_id': 0}
    db_cursor = BASED_INFO.get_based_info_from_collection(sql_query_0, sql_query_1)
    LAST_UPDATE_DATE = dict(list(db_cursor)[0])['last_date']


def fetch_school_data(atpt_ofcdc_sc_code):
    '''
    작성 : 윤서율
    기능 : 교육청 코드로 각 지역별 학교데이터 딕셔너리를 만들어 리스트에 넣는다.
    '''
    school = SchoolInfo(atpt_ofcdc_sc_code)
    # print('Thread Name :', threading.current_thread().getName(), 'Start', atpt_ofcdc_sc_code)
    check = school.call_data()
    # print('Thread Name :', threading.current_thread().getName(), 'Done', atpt_ofcdc_sc_code)
    if check:
        data_dict = {
            'ATPT_OFCDC_SC_CODE':atpt_ofcdc_sc_code,
            'DATA':school.get_school_data_list(),
            'UPDATE_DATE':TODAY
        }
        school_data_list.append(data_dict)


def push_school_data_db(data):
    '''
    작성 : 윤서율
    기능 : 만들어진 학교 정보 딕셔너리를 디비에 추가
    '''
    # print('Thread Name :', threading.current_thread().getName(), 'Start', atpt_ofcdc_sc_code)
    School_Info_DB.add_school_info_to_collection(data)
    # print('Thread Name :', threading.current_thread().getName(), 'Done', atpt_ofcdc_sc_code)


def school_data_init():
    '''
    작성 : 윤서율
    기능 : 학교정보를 만들고 데이터베이스에 넣는 함수를 비동기 콜
    '''
    with ThreadPoolExecutor() as executor:
        for code in ATPT_OFCDC_SC_CODE_LIST:
            executor.submit(fetch_school_data, code)

    if len(ATPT_OFCDC_SC_CODE_LIST) == len(school_data_list):
        with ThreadPoolExecutor() as executor:
            for data_dict in school_data_list:
                executor.submit(push_school_data_db, data_dict)

        sql_query_0 = {'last_date': LAST_UPDATE_DATE}
        sql_query_1 = {'$set': {'last_date': TODAY}}
        BASED_INFO.update_based_info_to_collection(sql_query_0, sql_query_1)


def get_local_school_list(atpt_ofcdc_sc_code) -> list:
    '''
    작성 : 서율
    기능 : 교육청에 따른 학교 정보 가져오는 함수
    atpt_ofcdc_sc_code : 교육청 코드
    return : data_list 지역의 학교 정보 전부 리스트
    '''
    sql_query_0 = {'ATPT_OFCDC_SC_CODE': atpt_ofcdc_sc_code, 'UPDATE_DATE': LAST_UPDATE_DATE}
    sql_query_1 = {'_id': 0, 'ATPT_OFCDC_SC_CODE': 0, 'UPDATE_DATE': 0}
    db_cursor = School_Info_DB.get_school_info_from_collection(sql_query_0, sql_query_1)
    db_dict = list(db_cursor)[0]
    data_list = db_dict['DATA']
    return data_list


def meal_data_init(atpt_ofcdc_sc_code, code) -> list:
    '''
    작성 : 서율
    기능 : 교육청 코드와 학교 코드로 MealInfo 객체를 만들어 급식 내용을 가져오는 API 호출한다.
    atpt_ofcdc_sc_code : 교육청 코드
    code : 학교 코드
    return : 급식 일주일 list
    '''
    mi = MealInfo(atpt_ofcdc_sc_code, code, TODAY, NEXT_WEEK)
    check = mi.call_data()
    if check:
        return mi.get_meal_data_list()
    else:
        return None


def get_meal_data():
    '''
    작성 : 윤서율
    기능 : 만들어진 학교 정보 딕셔너리를 디비에 추가
    '''

    # print('Thread Name :', threading.current_thread().getName(), 'Start', atpt_ofcdc_sc_code)
    # MEAL_INFO_B10.add_meal_info_to_collection(data)
    # print('Thread Name :', threading.current_thread().getName(), 'Done', atpt_ofcdc_sc_code)


def sss():
    '''
    작성 : 윤서율
    기능 : 급식 정보를 만들고 데이터베이스에 넣는 함수를 비동기 콜
    '''
    # with ThreadPoolExecutor() as executor:
    #     for code in ATPT_OFCDC_SC_CODE_LIST:
    #         executor.submit(fetch_meal_data, code)

    # with ThreadPoolExecutor() as executor:
    #    for data_dict in zip(school_data_list, ATPT_OFCDC_SC_CODE):
    #        executor.submit(push_meal_data_db, data_dict)

    # sql_query_0 = {'last_date': LAST_UPDATE_DATE}
    # sql_query_1 = {'$set': {'last_date': TODAY}}
    # BASED_INFO.update_based_info_to_collection(sql_query_0, sql_query_1)


def set_user_atpt_ofcdc_sc_code(user_select, user_id):
    '''
    작성 : 서율
    기능 : 학교정보 신규등록 - 지역등록
    user_select : 선택한 학교이름
    user_id : 유저 아이디
    '''
    if user_select in ATPT_OFCDC_SC_NAME_LIST:
        index = ATPT_OFCDC_SC_NAME_LIST.index(user_select)
        atpt_ofcdc_sc_code = ATPT_OFCDC_SC_CODE_LIST[index]
        sql_query_0 = {'user_id': user_id, 'ATPT_OFCDC_SC_CODE': atpt_ofcdc_sc_code}
        USER_INFO.add_user_info_to_collection(sql_query_0)
        return True
    else:
        return False

def update_user_sd_schul_code(user_id, user_select_sch_name, user_select_sch_code):
    '''
    작성 : 서율
    기능 : 
    user_id : 유저 아이디
    user_select_sch_name : 유저 선택 학교 이름
    user_select_sch_code : 유저 선택 학교 코드
    '''
    sql_query_0 = {'user_id': user_id}
    sql_query_1 = {'$set': {'SCHUL_NM': user_select_sch_name, 'SD_SCHUL_CODE': user_select_sch_code}}
    USER_INFO.update_user_info_to_collection(sql_query_0, sql_query_1)



def new_database():
    init_database() # 데이터베이스 초기값 세팅


def send_api(request):
    init_data() # 전역 변수 초기화 필수



    request_body = json.loads(request.body)

    print(request_body)

    user_id = request_body['intent']['id']
    params = request_body['action']['params']
    atpt_ofcdc_sc_name = params['atpt_ofcdc_sc_name']
    result = {
        "version": "2.0",
        "data": {
            "user_id": user_id,
            "atpt_ofcdc_sc_name": atpt_ofcdc_sc_name
        }
    }


    # school_data_init() # 학교 데이터 디비 세팅 한달에 한번?


    # meal_data_init() # 급식 데이터 디비 세팅
    # set_user_atpt_ofcdc_sc_code() # 학교 지역 등록

    res = {
        'statusCode': 200,
        'body': json.dumps(result)
    }


    user_id = 'kim' # 유저
    user_select_sch_name = '가좌고등학교'
    user_select_sch_code = '000'
    # update_user_sd_schul_code(user_id, user_select_sch_name, user_select_sch_code) # 학교 등록

    # data
    # context ={'aa':'k'}
    return HttpResponse(json.dumps(res))
    # return HttpResponse([send_json_data], content_type="application/json; charset=utf-8")