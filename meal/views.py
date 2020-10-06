from django.shortcuts import render
from django.http import HttpResponse

from concurrent.futures import ThreadPoolExecutor
from meal.api.openAPI import SchoolInfo, MealInfo
from meal.DBmanager import School_Info_DB, BASED_INFO, USER_INFO
import threading
import json
import datetime

school_data_list = list()
ATPT_OFCDC_SC_CODE_LIST = list()
ATPT_OFCDC_SC_NAME_LIST = list()
LAST_UPDATE_DATE = ''

TODAY = str(datetime.date.today()).replace('-', '')
TOMORROW = str(datetime.date.today()+datetime.timedelta(days=1)).replace('-', '')
NEXT_WEEK = str(datetime.date.today()+datetime.timedelta(days=7)).replace('-', '')


def init_database(request):
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

    msg = 'init end'

    result = {
        "version": "2.0",
        "data": {
            'msg': '{}'.format(msg)
        }
    }
    print(msg)
    return {
        'statusCode': 200,
        'body': result,
        'headers': {
            'Access-Control-Allow-Origin': '*',
        },
    }


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

#######################################################################
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
########################################################################################

def get_user_info(user_id):
    '''
    작성 : 서율
    기능 : 유저 데이터 가져오기
    '''
    sql_query_0 = {'user_id': user_id}
    sql_query_1 = {'_id':0}

    result_dict = USER_INFO.get_user_info_from_collection(sql_query_0, sql_query_1)
    return result_dict


def get_local_school_list(atpt_ofcdc_sc_code, schul_nm) -> list:
    '''
    작성 : 서율
    기능 : 교육청에 따른 학교 정보 가져오는 함수
    atpt_ofcdc_sc_code : 교육청 코드
    schul_nm : 학교이름
    return : data_list 지역의 학교 정보
    '''
    sql_query_0 = {'ATPT_OFCDC_SC_CODE': atpt_ofcdc_sc_code, 'UPDATE_DATE': LAST_UPDATE_DATE}
    sql_query_1 = {'_id': 0, 'ATPT_OFCDC_SC_CODE': 0, 'UPDATE_DATE': 0}
    db_cursor = School_Info_DB.get_school_info_from_collection(sql_query_0, sql_query_1)
    db_dict = list(db_cursor)[0]
    data_list = [data for data in db_dict['DATA'] if data['SCHUL_NM'] == schul_nm]
    return data_list


def update_user_sd_schul(request):
    '''
    작성 : 서율
    기능 : 유저 학교 이름과 코드 등록, 수정
    user_id : 챗봇 유저 아이디
    user_select_sch_name : 학교 이름
    user_select_sch_code : 학교 코드
    '''
    request_body = json.loads(request.body)

    msg = ''
    try:
        user_id = request_body['userRequest']['user']['id']
        params = request_body['action']['params']
        schul_nm = params['schul_nm']

        user_info_dict = list(get_user_info(user_id))[0]
        atpt_ofcdc_sc_code = user_info_dict['ATPT_OFCDC_SC_CODE']

        sc_data_list = (get_local_school_list(atpt_ofcdc_sc_code, schul_nm))[0]

        sd_schul_code = sc_data_list['SD_SCHUL_CODE']

        sql_query_0 = {'user_id': user_id}
        sql_query_1 = {'$set': {'SCHUL_NM': schul_nm, 'SD_SCHUL_CODE': sd_schul_code}}
        result = USER_INFO.update_user_info_to_collection(sql_query_0, sql_query_1)

        if not result.modified_count:
            sql_query = {
                'user_id': user_id,
                'SCHUL_NM': schul_nm,
                'SD_SCHUL_CODE': sd_schul_code
            }
            USER_INFO.add_user_info_to_collection(sql_query)
        msg = schul_nm

    except:
        msg = '지역등록과 학교등록을 확인해주시기 바랍니다.'

    result = {
        "version": "2.0",
        "data": {
            'msg': '{}'.format(msg)
        }
    }

    return {
        'statusCode': 200,
        'body': result,
        'headers': {
            'Access-Control-Allow-Origin': '*',
        },
    }


def update_user_atpt_ofcdc_sc(request):
    '''
    작성 : 서율
    기능 : 유저 지역 이름 등록, 수정
    처음 등록이라면 디비에 등록, 아니라면 수정
    user_id : 챗봇 유저 아이디
    atpt_ofcdc_sc_name : 지역명
    '''

    event = json.loads(request.body)

    msg = ''
    try:
        user_id = event['userRequest']['user']['id']
        params = event['action']['params']
        atpt_ofcdc_sc_name = params['atpt_ofcdc_sc_name']

        index = ATPT_OFCDC_SC_NAME_LIST.index(atpt_ofcdc_sc_name)

        sql_query_0 = {'user_id': user_id}
        sql_query_1 = {'$set': {'ATPT_OFCDC_SC_NAME': atpt_ofcdc_sc_name, 'ATPT_OFCDC_SC_CODE':ATPT_OFCDC_SC_CODE_LIST[index]}}
        print('변경: ', ATPT_OFCDC_SC_CODE_LIST[index])
        result = USER_INFO.update_user_info_to_collection(sql_query_0, sql_query_1)

        if not result.modified_count:
            sql_query = {
                'user_id': user_id,
                'ATPT_OFCDC_SC_NAME': atpt_ofcdc_sc_name,
                'ATPT_OFCDC_SC_CODE': ATPT_OFCDC_SC_CODE_LIST[index]
            }
            USER_INFO.add_user_info_to_collection(sql_query)
            print('새로 추가')

    except:
        msg = '지역등록과 학교등록을 확인해주시기 바랍니다.'

    result = {
        "version": "2.0",
        "data": {
            'msg': '{}'.format(msg)
        }
    }

    return {
        'statusCode': 200,
        'body': result,
        'headers': {
            'Access-Control-Allow-Origin': '*',
        },
    }


def call_meal_data(request) -> list:
    '''
    작성 : 서율
    기능 : 교육청 코드와 학교 코드로 MealInfo 객체를 만들어 급식 내용을 가져오는 API 호출한다.
    atpt_ofcdc_sc_code : 교육청 코드
    code : 학교 코드
    return : 급식 일주일 list
    '''
    event = json.loads(request.body)

    msg = ''
    user_id = event['userRequest']['user']['id']
    params = event['action']['params']
    target_day = params['target_day']

    if target_day == '오늘':
        target_day = TODAY
    elif target_day == '내일':
        target_day = str(datetime.date.today()+datetime.timedelta(days=1)).replace('-', '')
    elif target_day == '모레':
        target_day = str(datetime.date.today()+datetime.timedelta(days=2)).replace('-', '')
    else:
        target_day = str(datetime.date.today()+datetime.timedelta(days=3)).replace('-', '')

    try:
        user_info_dict = list(get_user_info(user_id))[0]
        atpt_ofcdc_sc_code = user_info_dict['ATPT_OFCDC_SC_CODE']
        sd_schul_code = user_info_dict['SD_SCHUL_CODE']

        mi = MealInfo(atpt_ofcdc_sc_code, sd_schul_code, target_day)
        check = mi.call_data()
        meal_list = mi.get_meal_data_list()
        print(meal_list)

        for data in meal_list:
            msg += data['MMEAL_SC_NM']+'<br/>'
            msg += data['DDISH_NM']
    except:
        msg = '식단정보가 없습니다.'

    print(msg)
    result = {
        "version": "2.0",
        "data": {
            'msg': '{}'.format(msg)
        }
    }
    return {
        'statusCode': 200,
        'body': result,
        'headers': {
            'Access-Control-Allow-Origin': '*',
        },
    }

def send_api(request):
    # res = init_database(request)
    init_data()  # 전역 변수 초기화 필수


    res = 'gg'
    res = update_user_atpt_ofcdc_sc(request) # 지역등록
    # res = update_user_sd_schul(request) # 학교등록
    # res = call_meal_data(request) # 급식 검색

    # school_data_init() # 학교 데이터 디비 세팅 한달에 한번?
        

    return HttpResponse(json.dumps(res))