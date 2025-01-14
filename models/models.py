""" This module contains the Pydantic models for the API. """

# from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional

class BaseRequest(BaseModel):
    exam_quarter: str = Field(..., alias="examQuarter")
    exam_year: str = Field(..., alias="examYear")
    
    @field_validator('exam_quarter')
    def check_exam_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examQuarter cannot be null or empty")
        return v
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v


class SchoolRequest(BaseModel):
    exam_quarter: str = Field(..., alias="examQuarter")
    exam_year: str = Field(..., alias="examYear")

    page: Optional[int] = Field(1, alias="page")
    territory: Optional[str] = Field(None, alias="territory")  # Make this optional
    region : Optional[str] = Field(None, alias="region")
    subject : Optional[str] = Field(None, alias="subject")
    study_class: Optional[str] = Field(None, alias="studyClass") 

    @field_validator('exam_quarter')
    def check_exam_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examQuarter cannot be null or empty")
        return v
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v
    
    @field_validator('territory', 'region', 'study_class', 'subject', mode='before')
    def convert_empty_string_to_none(cls, v):
        return None if v == "" else v

class StudentRequest(BaseModel):
    exam_quarter: str = Field(..., alias="examQuarter")
    exam_year: str = Field(..., alias="examYear")

    territory: Optional[str] = Field(None, alias="territory")  # Make this optional
    region : Optional[str] = Field(None, alias="region")
    study_class: Optional[str] = Field(None, alias="studyClass") 
    school: Optional[str] = Field(None, alias="school")
    subject : Optional[str] = Field(None, alias="subject")
    page : Optional[int] = Field(1, alias="page")

    @field_validator('exam_quarter')
    def check_exam_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examQuarter cannot be null or empty")
        return v
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v
    
    @field_validator('territory', 'study_class', 'region', 'school', 'subject', mode='before')
    def convert_empty_string_to_none(cls, v):
        return None if v == "" else v

class ResultRequest(BaseModel):
    exam_quarter: str = Field(..., alias="examQuarter")
    exam_year: str = Field(..., alias="examYear")

    territory: Optional[str] = Field(None, alias="territory")  # Make this optional
    region : Optional[str] = Field(None, alias="region")
    study_class: Optional[str] = Field(None, alias="studyClass") 
    school: Optional[str] = Field(None, alias="school")
    exam_method: Optional[str] = Field(None, alias="examMethod")
    subject: Optional[str] = Field(None, alias="subject")
    page : Optional[int] = Field(1, alias="page")
    
    
    @field_validator('exam_quarter')
    def check_exam_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examQuarter cannot be null or empty")
        return v
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v
    
    @field_validator('territory', 'study_class', 'school', 'exam_method', 'subject', 'region', mode='before')
    def convert_empty_string_to_none(cls, v):
        return None if v == "" else v
    
class CompareRequest(BaseModel):
    exam_year: str = Field(..., alias="examYear")
    first_quarter: str = Field(..., alias="firstQuarter")
    second_quarter: Optional[str] = Field(None, alias="secondQuarter")
    territory: Optional[str] = Field(None, alias="territory")  # Make this optional
    region : Optional[str] = Field(None, alias="region")
    study_class: Optional[str] = Field(None, alias="studyClass") 
    school: Optional[str] = Field(None, alias="school")
    exam_method: Optional[str] = Field(None, alias="examMethod")
    subject: Optional[str] = Field(None, alias="subject")
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v
    
    @field_validator('first_quarter')
    def check_first_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("firstQuarter cannot be null or empty")
        return v
    
    @field_validator('second_quarter', 'territory', 'study_class', 'school', 'exam_method', 'subject', 'region', mode='before')
    def convert_empty_string_to_none(cls, v):
        return None if v == "" else v

class SchoolListRequest(BaseModel):
    exam_quarter: str = Field(..., alias="examQuarter")
    exam_year: str = Field(..., alias="examYear")
    territory : str = Field(..., alias="territory")
    region : str = Field(..., alias="region")

    @field_validator('exam_quarter')
    def check_exam_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examQuarter cannot be null or empty")
        return v
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v
    
    @field_validator('territory')
    def check_territory(cls, v):
        if not v or v.strip() == "":
            raise ValueError("territory cannot be null or empty")
        return v
    
    @field_validator('region')
    def check_region(cls, v):
        if not v or v.strip() == "":
            raise ValueError("region cannot be null or empty")
        return v
    
class RegionListRequest(BaseModel):
    exam_quarter: str = Field(..., alias="examQuarter")
    exam_year: str = Field(..., alias="examYear")
    territory : str = Field(..., alias="territory")
    
    @field_validator('exam_quarter')
    def check_exam_quarter(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examQuarter cannot be null or empty")
        return v
    
    @field_validator('exam_year')
    def check_exam_year(cls, v):
        if not v or v.strip() == "":
            raise ValueError("examYear cannot be null or empty")
        return v
    
    @field_validator('territory')
    def check_territory(cls, v):
        if not v or v.strip() == "":
            raise ValueError("territory cannot be null or empty")
        return v
    
class UserLoginBody(BaseModel):
    username: str = Field(..., alias="username")
    password: str = Field(..., alias="password")

    @field_validator('username')
    def check_username(cls, v):
        if not v or v.strip() == "":
            raise ValueError("username cannot be null or empty")
        return v
    
    @field_validator('password')
    def check_password(cls, v):
        if not v or v.strip() == "":
            raise ValueError("password cannot be null or empty")
        return v
    
class User(BaseModel):
    user_id: str = Field(..., alias="id")
    username: str = Field(..., alias="username")
    password: str = Field(..., alias="password")
    created_at: datetime = Field(..., alias="createdAt")
    last_login: datetime = Field(..., alias="lastLogin")
    role: str = Field(..., alias="role")
    details: Optional[dict] = Field(None, alias="details")
