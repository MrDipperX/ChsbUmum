# app/__init__.py

from fastapi import FastAPI
from .endpoints import router

app = FastAPI()

# Include the router
app.include_router(router)