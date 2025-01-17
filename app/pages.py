# services/render_service.py

from typing import Any, Optional
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import HTTPException, Request, Form, Depends, status
from fastapi.responses import HTMLResponse, RedirectResponse
from collections import defaultdict
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
import json

from pydantic import ValidationError

from db.db import PgConn
from models.models import StudentRequest, ResultRequest, BaseRequest, CompareRequest, SchoolRequest
from . import app
from utils.const import table_headers, all_subjects, all_exam_methods
from utils.tables_title import generate_school_table_title, generate_student_table_title
from utils.cleaning_results import clean_subjects, clean_results_data, clean_compare_data
from utils.jwt_funcs import create_access_token
from config.config import PROD, SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

# Initialize Jinja2 templates directory
templates = Jinja2Templates(directory="templates")

def https_url_for(request: Request, name: str, **path_params: Any) -> str:
    http_url = request.url_for(name, **path_params)
    # Ensure it is a string before calling replace()
    return str(http_url).replace("http", "https", 1)

security = HTTPBearer()

def jwt_checker(request: Request):
    # Extract the token from cookies
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        # Decode the token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload  # Return the payload for use in protected routes
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

templates.env.globals["https_url_for"] = https_url_for

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def redirect_to_home():
    return RedirectResponse(url="/home", status_code=303)

@app.get("/login", response_class=HTMLResponse, name="login")
async def read_root(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "title" : "Kirish", "is_prod": PROD})

@app.get("/home", response_class=HTMLResponse, name="home")
async def read_root(request: Request, payload: dict = Depends(jwt_checker)):
    db = PgConn()
    # periods = db.get_available_periods()

    year_and_quarter = db.get_last_year_and_quarter()

    year, quarter = year_and_quarter['exam_year'], year_and_quarter['exam_quarter']
    base_request = BaseRequest(examQuarter=quarter, examYear=year)

    data = db.get_base_results(base_request)

    teachers_info_by_territory = data['teachers_info_by_territory']
    if teachers_info_by_territory and len(teachers_info_by_territory) > 0:
        teachers_info_by_territory[0], teachers_info_by_territory[-1] = teachers_info_by_territory[-1], teachers_info_by_territory[0]
    
    return templates.TemplateResponse("home.html", {"request": request, 
                                                    "title" : "Asosiy", 
                                                    # "periods": periods, 
                                                    "schools_count": data['all_schools_count'],
                                                    "regions_count": data['all_regions_count'],
                                                    "teachers_count": data['all_teachers_count'],
                                                    "students_count": data['all_count'],
                                                    "avg_by_territory": data['avg_by_territory'],
                                                    "avg_by_school": data['avg_by_school'],
                                                    "exam_methods_data": data['exam_methods_data'],
                                                    "territories_data": data['territories_data'],
                                                    "teachers_info": data['teachers_info'],
                                                    "teachers_info_by_territory": teachers_info_by_territory,
                                                    "teachers_schools": data['teachers_schools'],
                                                    "is_prod": PROD,

                                                    "examYear": year,
                                                    "examQuarter": quarter
                                                    })

@app.get("/schools", response_class=HTMLResponse, name="schools")
async def read_root(request: Request, payload: dict = Depends(jwt_checker)):
    db = PgConn()

    periods = db.get_available_periods()

    available_info = db.get_available_territories_classes()
    all_classes = available_info['all_classes']
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}

    return templates.TemplateResponse("school.html", {
        "request": request, 
        "title" : "Maktablar", 
        "periods": periods,
        
        "all_classes": all_classes_dict, 
        "all_territories": all_territories_dict,
        "all_subjects": all_subjects, 
        "is_prod": PROD})

@app.get("/students", response_class=HTMLResponse, name="students")
async def read_root(request: Request, payload: dict = Depends(jwt_checker)):
    db = PgConn()

    periods = db.get_available_periods()
    available_info = db.get_available_territories_classes()

    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}
    
    return templates.TemplateResponse("student.html", {
        "request": request, 
        "title" : "Oʻquvchilar", 
        "periods": periods,
        
        "all_classes": all_classes_dict, 
        "all_territories": all_territories_dict,
        "all_subjects": all_subjects, 
        "is_prod": PROD
        })

@app.get("/compare", response_class=HTMLResponse, name="compare")
async def read_root(request: Request, payload: dict = Depends(jwt_checker)):
    db = PgConn()

    periods = db.get_available_paired_periods()
    if periods is None:
        raise HTTPException(status_code=500, detail="Internal Server Error")

    available_info = db.get_available_territories_classes()

    # convert all_classes elements to string
    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}
    return templates.TemplateResponse("compare.html", 
                                      {"request": request, 
                                       "title" : "Natijalar",
                                       "periods": periods,

                                       "all_classes": all_classes_dict, 
                                       "all_subjects": all_subjects, 
                                       "all_exam_methods": all_exam_methods, 
                                       "all_territories": all_territories_dict,
                                       "is_prod": PROD
                                       })

@app.get("/results", response_class=HTMLResponse, name="results")
async def read_root(request: Request, payload: dict = Depends(jwt_checker)):
    db = PgConn()

    periods = db.get_available_periods()
    available_info = db.get_available_territories_classes()

    # convert all_classes elements to string
    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}

    return templates.TemplateResponse("result.html", 
                                      {"request": request, 
                                       "title" : "Natijalar",
                                       "periods": periods,

                                       "all_classes": all_classes_dict, 
                                       "all_subjects": all_subjects, 
                                       "all_exam_methods": all_exam_methods, 
                                       "all_territories": all_territories_dict,
                                       "is_prod": PROD
                                       })

@app.post("/schools", response_class=HTMLResponse)
async def read_school(request: Request, payload: dict = Depends(jwt_checker)):

    db = PgConn()
    form_data = await request.form()
    try:
        school_results = SchoolRequest(**form_data)
    except ValidationError as e:
        return HTMLResponse(content="Invalid data received", status_code=422)
    
    school_results_data = db.get_school_results(school_results)
    total_pages = school_results_data['pages']
    
    school_info = clean_subjects(school_results_data['school_results'])
    table_title = generate_school_table_title(school_results)

    periods = db.get_available_periods()
    available_info = db.get_available_territories_classes()

    # convert all_classes elements to string
    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}

    return templates.TemplateResponse("school.html", 
                                      {
                                          "request": request, 
                                          "title" : "Maktablar", 
                                          "school_results": school_info, 
                                          "table_title": table_title, 
                                          "table_headers": table_headers, 
                                          "periods": periods,

                                          "all_classes": all_classes_dict,  
                                          "all_territories": all_territories_dict,
                                          "all_subjects": all_subjects, 
                                          
                                          "examYear": school_results.exam_year,
                                          "examQuarter": school_results.exam_quarter,
                                          "territory": school_results.territory, 
                                          "studyClass": school_results.study_class,
                                          "subject": school_results.subject,
                                          "page": school_results.page,
                                          "totalPages": total_pages,
                                          "is_prod": PROD}
                                          )

@app.post("/students", response_class=HTMLResponse)
async def read_students(request: Request, payload: dict = Depends(jwt_checker)):

    db = PgConn()
    form_data = await request.form()
    try:
        student_results = StudentRequest(**form_data)
    except ValidationError as e:
        return HTMLResponse(content="Invalid data received", status_code=422)
    
    student_info = db.get_students_results(student_results)
    table_title = generate_student_table_title(student_results)

    total_pages = student_info['total_pages']
    student_info = clean_subjects(student_info['results'])
    available_info = db.get_available_territories_classes()

    # convert all_classes elements to string
    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}

    periods = db.get_available_periods()
    return templates.TemplateResponse("student.html", 
                                      {
                                        "request": request, 
                                        "title" : "Oʻquvchilar", 
                                        "student_results": student_info, 
                                        "table_title":table_title, 
                                        "table_headers": table_headers, 
                                        "periods": periods,

                                        "all_classes": all_classes_dict, 
                                        "all_territories": all_territories_dict,
                                        "all_subjects": all_subjects,
                                        
                                        "examYear": student_results.exam_year,
                                        "examQuarter": student_results.exam_quarter,
                                        "territory": student_results.territory, 
                                        "region": student_results.region,
                                        "studyClass": student_results.study_class, 
                                        "school": student_results.school,
                                        "subject": student_results.subject,
                                        "page": student_results.page,
                                        "totalPages": total_pages,
                                        "is_prod": PROD}
                                        )

@app.post("/compare", response_class=HTMLResponse)
async def read_results(request: Request, payload: dict = Depends(jwt_checker)):

    db = PgConn()
    form_data = await request.form()
    try:
        compare_request = CompareRequest(**form_data)
    except ValidationError as e:
        return HTMLResponse(content="Invalid data received", status_code=422)

    periods = db.get_available_paired_periods()

    examYear = compare_request.exam_year
    if compare_request.first_quarter == 'all':
        quarters = periods[examYear]
    else:
        quarters = [compare_request.first_quarter, compare_request.second_quarter]

    # Initialize data_by_quarters as a nested defaultdict
    data_by_quarters = defaultdict(lambda: defaultdict(list))
   
    for quarter in quarters:
        result_request = ResultRequest(examQuarter=quarter, examYear=examYear, 
                                       territory=compare_request.territory, 
                                       studyClass=compare_request.study_class, 
                                       school=compare_request.school, 
                                       subject=compare_request.subject, 
                                       region=compare_request.region, 
                                       examMethod=compare_request.exam_method)
        
        data = db.get_compare_results(result_request)
        data['some_subject_result'], data['subject_results'], data['subject_results_keys'] = clean_compare_data(data)
        for key in data:
            if data[key] and len(data[key]) > 0:
                data_by_quarters[key][quarter] = data[key]

    data_by_quarters['exam_method_results'] = {str(key): value for key, value in data_by_quarters['exam_method_results'].items()}

    available_info = db.get_available_territories_classes()

    # convert all_classes elements to string
    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}

    return templates.TemplateResponse("compare.html", 
                                      {"request": request, 
                                       "title" : "Qiyosiy tahlil", 
                                       
                                       "all_classes": all_classes_dict, 
                                       "all_subjects": all_subjects, 
                                       "all_exam_methods": all_exam_methods, 
                                       "all_territories": all_territories_dict,

                                       "periods": periods,

                                       "avg_by_territory" : data_by_quarters['avg_by_territory'], 
                                       "subject_results" : data_by_quarters['subject_results'],
                                       "some_subject_result" : data_by_quarters['some_subject_result'], 
                                       "study_class_results": data_by_quarters['study_class_results'],
                                       "exam_method_results": data_by_quarters['exam_method_results'],
                                       "subject_results_keys": data_by_quarters['subject_results_keys'],

                                       "examYear": compare_request.exam_year,
                                       "firstQuarter": compare_request.first_quarter,
                                       "secondQuarter": compare_request.second_quarter,
                                       "territory": compare_request.territory, 
                                       "studyClass": compare_request.study_class, 
                                       "school": compare_request.school, 
                                       "subject": compare_request.subject, 
                                       "region": compare_request.region,
                                       "examMethod": compare_request.exam_method,
                                       "is_prod": PROD}
                                       )

@app.post("/results", response_class=HTMLResponse)
async def read_results(request: Request, payload: dict = Depends(jwt_checker)):

    db = PgConn()

    form_data = await request.form()
    try:
        results = ResultRequest(**form_data)
    except ValidationError as e:
        return HTMLResponse(content="Invalid data received", status_code=422)
    
    results_info = db.get_results(results)
    periods = db.get_available_periods()

    total_pages = results_info['total_pages']

    students_results, some_subject_result, subject_results, subject_results_keys = clean_results_data(results_info)
    available_info = db.get_available_territories_classes()

    # convert all_classes elements to string
    all_classes = map(str, available_info['all_classes'])
    all_territories = available_info['all_territories']

    all_classes_dict = {"": "Barcha sinflar", **{k:k for k in all_classes}}
    all_territories_dict = {"": "Barcha hududlar", **{k:k for k in all_territories}}

    return templates.TemplateResponse("result.html", 
                                      {"request": request, 
                                       "title" : "Natijalar", 
                                       "table_results": students_results,
                                       "avg_by_territory" : results_info['avg_by_territory'], 
                                       "subject_results" : subject_results,
                                       "some_subject_result" : some_subject_result, 
                                       "study_class_results": results_info['study_class_results'],
                                       "exam_method_results": results_info['exam_method_results'],
                                       "table_title": "Natijalar", "table_headers": table_headers, 
                                       "periods": periods, 
                                       "subject_results_keys": subject_results_keys,
                                       "totalPages": total_pages,
                                       "page": results.page,

                                       "all_classes": all_classes_dict, 
                                       "all_subjects": all_subjects, 
                                       "all_exam_methods": all_exam_methods, 
                                       "all_territories": all_territories_dict,

                                       "examYear": results.exam_year,
                                       "examQuarter": results.exam_quarter,
                                       "territory": results.territory, 
                                       "studyClass": results.study_class, 
                                       "school": results.school, 
                                       "subject": results.subject, 
                                       "region": results.region,
                                       "examMethod": results.exam_method,
                                       "subject_not_chosen": results.subject is None or results.subject == "",
                                       "is_prod": PROD}
                                       )

@app.exception_handler(HTTPException)
async def custom_eror_handler(request: Request, exc: HTTPException):
    # print("EXCEPTION", exc)
    if exc.status_code == status.HTTP_401_UNAUTHORIZED:
        # Redirect to the /login page if 401 Unauthorized
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    elif exc.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR:
        print("ERROR")
        return templates.TemplateResponse("InternalServerError.html", {"request": request, "title" : "Serverda xatolik", "is_prod": PROD})
    # For other HTTP exceptions, return the default behavior
    return await request.app.default_exception_handler(request, exc)
    