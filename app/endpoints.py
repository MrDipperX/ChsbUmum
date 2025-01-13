""" This module contains the FastAPI endpoints for the bot creation and deletion. """

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from .controllers import school_list, login_user, region_list
from models.models import SchoolListRequest, UserLoginBody, RegionListRequest

# Create a router instance
router = APIRouter()

@router.post("/school/list", name="school_list")
async def get_school_list(school_list_data: SchoolListRequest):
    try:
        success = await school_list(school_list_data)

        # Use JSONResponse to return a proper JSON object
        return JSONResponse(content=success[0], status_code=success[1])

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")


@router.post("/region/list", name="region_list")
async def get_region_list(territory_data: RegionListRequest):
    try:
        success = await region_list(territory_data)

        # Use JSONResponse to return a proper JSON object
        return JSONResponse(content=success[0], status_code=success[1])

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")

@router.post("/login", name="login")
async def login(login : UserLoginBody):
    try:
        success = await login_user(login)

        # Use JSONResponse to return a proper JSON object
        return success

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {str(e)}")