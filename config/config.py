"""This module contains the configuration for the application."""

import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

PROD = os.getenv("PROD") == "True"

ADMIN_CREDENTIALS = [os.getenv("ADMIN_USERNAME"), os.getenv("ADMIN_PASSWORD")]
SUPERADMIN_CREDENTIALS = [os.getenv("SUPERADMIN_USERNAME"), os.getenv("SUPERADMIN_PASSWORD")]


SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120

