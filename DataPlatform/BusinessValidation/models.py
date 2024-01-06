from django.db import models

class BusinessValidationRecord(models.Model):
    name = models.CharField(max_length=255)
    salary = models.FloatField()
    saving = models.FloatField()
