from models.models import SchoolRequest, StudentRequest, ResultRequest

def generate_school_table_title(school: SchoolRequest):
    """Function to generate the title for the school results table"""
    if school.territory is None:
        territory_text = "Barcha hududlarning"
    else:
        territory_text = school.territory+"ning"

    if school.region is None:
        region_text = "barcha tumanlar"
    else:
        region_text = school.region+" tumani"
    
    if school.study_class is None:
        study_class_text = "barcha sinflar"
    else:
        study_class_text = school.study_class+"-sinf"

    title = f"{territory_text} {region_text} {study_class_text} bo‘yicha maktablar reytingi"
    return title


def generate_student_table_title(student: StudentRequest):
    """Function to generate the title for the student results table"""
    if student.territory is None:
        territory_text = "Barcha hududlardagi"
    else:
        territory_text = student.territory+"dagi"

    if student.region is None:
        region_text = "barcha tumanlar"
    else:
        region_text = student.region+" tumani"

    if student.school is None:
        school_text = "barcha maktablar"
    else:
        if student.school[-1] != "i":
            school_text = student.school+"i"
        else:
            school_text = student.school
    
    if student.study_class is None:
        study_class_text = "barcha sinflar"
    else:
        study_class_text = student.study_class+"-sinf"

    title = f"{territory_text} {region_text} {school_text} bo‘yicha {study_class_text} o‘quvchilar reytingi"
    return title


