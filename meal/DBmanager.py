import pymongo

class School_Info_DB:

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

class BASED_INFO:

    _instance = None
    # 몽고디비연결
    client = pymongo.MongoClient('mongodb+srv://reso:1q2w3e4r@reso.ympo0.mongodb.net/school_meal?retryWrites=true&w=majority')
    # collection 생성
    database = client['school_meal']['based_info']

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    @classmethod
    def get_based_info_from_collection(cls, *_query):
        assert cls.database
        return cls.database.find(_query[0], _query[1])

    @classmethod
    def add_based_info_to_collection(cls, _data):
        if type(_data) is list:
            return cls.database.insert_many(_data)
        else:
            return cls.database.insert_one(_data)

    @classmethod
    def update_based_info_to_collection(cls, *_query):
        assert cls.database
        return cls.database.update_one(_query[0], _query[1])


class MEAL_INFO_B10():
    _instance = None
    # 몽고디비연결
    client = pymongo.MongoClient(
        'mongodb+srv://reso:1q2w3e4r@reso.ympo0.mongodb.net/school_meal?retryWrites=true&w=majority')
    # collection 생성
    collections = 'B10'
    database = client['school_meal'][collections]

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    @classmethod
    def get_meal_info_from_collection(cls, *_query):
        assert cls.database
        return cls.database.find(_query[0], _query[1])

    @classmethod
    def add_meal_info_to_collection(cls, _data):
        if type(_data) is list:
            return cls.database.insert_many(_data)
        else:
            return cls.database.insert_one(_data)




