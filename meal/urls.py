from django.urls import path
from . import views

urlpatterns = [
    # data 등록
    path('', views.send_api, name='dddd'),

]