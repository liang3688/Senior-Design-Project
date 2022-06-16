from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    #path('advanced', views.advanced, name='advanced'),
    path('search', views.search, name='search'),
    path('more', views.more, name='more'),
]