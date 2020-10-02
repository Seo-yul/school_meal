import requests
import urllib

class SchoolMeal():
    # url = 'https://open.neis.go.kr/hub/mealServiceDietInfo?'
    # key_KEY = '8d8e6417cf5e45af8e732d993d5cfef6'
    # key_Type = 'json'
    # key_pIndex = 1
    # key_pSize = 1
    # key_ATPT_OFCDC_SC_CODE = 'B10'
    # key_SD_SCHUL_CODE = 7010057
    # key_MMEAL_SC_CODE =
    # key_MLSV_YMD = 20191028
    # key_MLSV_FROM_YMD =
    # MLSV_TO_YMD =


    # res = requests.get(gugun_url, headers=res_headers, verify=False)
    # res = requests.get('https://open.neis.go.kr/hub/mealServiceDietInfo?KEY=8d8e6417cf5e45af8e732d993d5cfef6&Type=json&pIndex=1&pSize=1&ATPT_OFCDC_SC_CODE=B10&SD_SCHUL_CODE=7010057&MLSV_YMD=20191028')
    # html = res.json()
    # print(type(html))
    # html = res.text
    # soup = BeautifulSoup(html, 'html.parser')
    pass

class SchoolInfo(object):
    url = 'https://open.neis.go.kr/hub/schoolInfo?'

    def __init__(self, atpt_ofcdc_sc_code):
        self.school_data_list = []
        self.query_string = {
            'KEY': '8d8e6417cf5e45af8e732d993d5cfef6',
            'Type': 'json',
            'pIndex': 1,
            'pSize': 1000,
            'ATPT_OFCDC_SC_CODE' : atpt_ofcdc_sc_code
        }


    def call_data(self) -> bool:
        qs = urllib.parse.urlencode(self.query_string)
        res = requests.get(self.url+qs)
        json = res.json()
        result_bool = False
        try:
            head_json_list = json['schoolInfo'][0]['head']
            list_total_count = head_json_list[0]['list_total_count']
            result = head_json_list[1]['RESULT']
            result_code = result['CODE']

            if result_code == 'INFO-000':
                row_json_list = json['schoolInfo'][1]['row']
                for data in row_json_list:
                    school_data_dict = dict()
                    school_data_dict['ATPT_OFCDC_SC_CODE'] = data['ATPT_OFCDC_SC_CODE'] # 시도교육청코드
                    school_data_dict['ATPT_OFCDC_SC_NM'] = data['ATPT_OFCDC_SC_NM'] # 시도교육청명
                    school_data_dict['SD_SCHUL_CODE'] = data['SD_SCHUL_CODE'] # 표준학교코드
                    school_data_dict['SCHUL_NM'] = data['SCHUL_NM'] # 학교명
                    school_data_dict['SCHUL_KND_SC_NM'] = data['SCHUL_KND_SC_NM'] # 학교종류명 (초등학교, 중학교, 고등학교)
                    school_data_dict['ORG_RDNMA'] = data['ORG_RDNMA'] # 도로명주소
                    self.school_data_list.append(school_data_dict)


                total_page = (list_total_count // self.query_string['pSize'])+1
                if self.query_string['pIndex'] != total_page:
                    self.query_string['pIndex'] += 1
                    self.call_data()
                result_bool = True

        except TypeError as e:
            print(result_code)
            print('openApi call_data() TypeError :', e)
        except KeyError as e:
            print(result_code)
            print('openApi call_data() KeyError :', e)
        except Exception as e:
            print(result_code)
            print(e)
        finally:
            return result_bool

    def get_school_data_list(self) -> list:
        return self.school_data_list