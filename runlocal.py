# runlocal.py

import os
import sys
from django.core.management import execute_from_command_line

if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'transformation.settings')  # 🔁 Replace 'myproject' with your project name

    # Simulate the command: python manage.py runserver 127.0.0.1:8000
    sys.argv = ['runlocal.py', 'runserver', '127.0.0.1:8000']
    
    execute_from_command_line(sys.argv)
