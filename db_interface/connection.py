"""
====================================================================================
File                :   connection.py
Description         :   Default connection config
Author              :   Murugesan G
Date Created        :   Feb 7th 2023
Last Modified BY    :   Murugesan G
Date Modified       :   Feb 9th 2023
====================================================================================
"""

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'smsvts_flower_market',
        'USER': 'postgres',
        'PASSWORD': 'Postgres@123',
        'HOST': '192.168.29.143',
        'PORT': '5433',
    }
}
