"""Main module, which run app."""

import logging
import uvicorn
from app import app
from app.endpoints import router
from app import pages
from db.db import PgConn
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from utils.clearly_insert_excel import insert_data_to_tables, inserting_teachers
# from models.models import StudentRequest, SchoolRequest, ResultRequest

# Set up logging configuration
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO  # or DEBUG, WARNING, ERROR
)

# Get the logger for this module
logger = logging.getLogger(__name__)

# # Run FastAPI with Uvicorn
if __name__ == "__main__":
    db = PgConn()
    print("Creating tables")
    db.create_tables()
    db.create_indexes()
    db.insert_admins()
    print("Inserting data")
    # insert_data_to_tables(filename="chsb_23_24_1.xlsx", quarter="1", year="2024/2025")
    inserting_teachers(filename="teachers_24_25.xlsx", year="2024/2025")
    uvicorn.run("main:app", host="0.0.0.0", port=3132, reload=True)
