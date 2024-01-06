from django.core.management.base import BaseCommand
from BusinessValidation.consumer import BusinessRuleListener

class Command(BaseCommand):
    help = 'Launches Listener for BusinessRuleValidation : Kafka'
    def handle(self, *args, **options):
        listener = BusinessRuleListener()
        listener.run()
        