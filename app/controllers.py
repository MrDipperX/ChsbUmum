"""This module contains the functions that handle the business logic of the API endpoints."""

from models.models import SchoolListRequest, UserLoginBody, RegionListRequest
from db.db import PgConn
from utils.jwt_funcs import create_access_token
from fastapi.responses import RedirectResponse, JSONResponse
from config.config import ACCESS_TOKEN_EXPIRE_MINUTES, PROD

async def school_list(scholl_list_data: SchoolListRequest):
    """ Function to get the list of schools """
    if scholl_list_data:
        db = PgConn()
        results = db.get_schools_by_req(scholl_list_data)
        return results, 200
    return "Bad request", 400

async def login_user(login: UserLoginBody):
    db_conn = PgConn()
    user = db_conn.get_admin(login)

    if user:        
        # Create JWT token
        token_data = {"sub": user.username, "user_id": user.user_id}  # Payload
        token = create_access_token(token_data)
        
        # Set token in response header or HTTP-only cookie
        response = JSONResponse(content={"message": "Login successful"}, status_code=200)
        response.set_cookie(
            key="access_token",
            value=token,
            httponly=True,  # Prevent JavaScript access (security against XSS)
            max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,  # Token expiration in seconds
        )
        return response
    else:
        return JSONResponse(content={"error": "Invalid credentials"}, status_code=401)
         

async def region_list(territory_data: RegionListRequest):
    """ Function to get the list of schools """
    if territory_data and territory_data.territory.strip() != "":
        db = PgConn()
        results = db.get_regions_by_territory(territory_data)
        return results, 200
    return "Bad request", 400