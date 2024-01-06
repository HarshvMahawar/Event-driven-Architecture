from django.urls import path, include
from . import views

urlpatterns = [
    path('produce/', views.produce, name="produce"),
]