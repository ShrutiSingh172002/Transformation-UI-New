from django.apps import AppConfig

class ApptransformationConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apptransformation'

    def ready(self):
        import apptransformation.signals  # 👈🏽 Import your signals here
