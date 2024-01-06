from django.core.management.base import BaseCommand
from GoogleSheetIntegration.consumer import GoogleSheetConsumer

class Command(BaseCommand):
    help = 'Launches Listener for GoogleSheetsIntegration : Kafka'
    def handle(self, *args, **options):
        google_sheet_consumer = GoogleSheetConsumer()
        google_sheet_consumer.run()
