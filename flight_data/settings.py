import logging
from pathlib import Path
from configs import config  # Import your Appconfig

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = config.django.SECRET_KEY

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = config.django.DEBUG

ALLOWED_HOSTS = config.django.ALLOWED_HOSTS

# Static files (CSS, JavaScript, images)
STATIC_URL = '/static/'

# If you're serving static files in production, you may also want to set:
# STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')

# Application definition
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "flight_processing",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "flight_data.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "flight_data.wsgi.application"

# Database
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": config.database.POSTGRES_DB,
        "USER": config.database.POSTGRES_USER,
        "PASSWORD": config.database.POSTGRES_PASSWORD,
        "HOST": config.database.POSTGRES_HOST,
        "PORT": config.database.POSTGRES_PORT,
    }
}

# Celery Configuration
CELERY_BROKER_URL = config.celery.BROKER_URL
CELERY_RESULT_BACKEND = config.celery.RESULT_BACKEND
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"

# Logging Configuration
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
        "celery": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
        "flight_processing": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
        "flight_processing_debug": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
    },
}

# SPARK Configuration
SPARK_CONFIG_OPTION = {
    "spark.executor.memory": config.spark.EXECUTOR_MEMORY,
    "spark.driver.memory": config.spark.DRIVER_MEMORY,
    "spark.executor.cores": config.spark.EXECUTOR_CORES,
    "spark.num.executors": config.spark.NUM_EXECUTORS,
    "spark.dynamicAllocation.enabled": config.spark.DYNAMIC_ALLOCATION_ENABLED,
    "spark.dynamicAllocation.minExecutors": config.spark.DYNAMIC_ALLOCATION_MAX_EXECUTORS,
    "spark.dynamicAllocation.maxExecutors": config.spark.DYNAMIC_ALLOCATION_MAX_EXECUTORS,
    "spark.sql.shuffle.partitions": config.spark.SQL_SHUFFLE_PARTITIONS,
    "spark.sql.adaptive.enabled": config.spark.SQL_ADAPTIVE_ENABLED,
    "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": config.spark.SQL_ADAPTIVE_SHUFFLE_TARGET_SIZE,
}

SPARK_BAD_RECORDS_PATH = config.spark.BAD_RECORDS_PATH
