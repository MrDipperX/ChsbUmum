""" This module is used to create a connection to the PostgreSQL database"""

from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
import bcrypt

from config.config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, ADMIN_CREDENTIALS ,SUPERADMIN_CREDENTIALS
from models.models import SchoolRequest, StudentRequest, ResultRequest, BaseRequest, UserLoginBody, User, RegionListRequest, SchoolListRequest
from utils.const import ADMIN_ROLE, SUPERADMIN_ROLE, USER_ROLE


class PgConn:
    """This class is used to create a connection to the PostgreSQL database"""
    def __init__(self):
        self.conn = None
        try:
            self.conn = psycopg2.connect(database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
            self.cur = self.conn.cursor()

        except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
            print(error)

    def create_tables(self):
        with self.conn:

            self.cur.execute(
                f"""
                    CREATE TABLE IF NOT EXISTS um_users(
                        id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
                        username CHARACTER VARYING(255) UNIQUE NOT NULL,
                        password CHARACTER VARYING(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        role CHARACTER VARYING(255) DEFAULT '{USER_ROLE}' NOT NULL,
                        details JSONB
                    );
                """
                )

            self.cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS um_school(
                        id CHARACTER VARYING(255) PRIMARY KEY NOT NULL,
                        name CHARACTER VARYING(255) NOT NULL,
                        territory CHARACTER VARYING(255) NOT NULL,
                        region CHARACTER VARYING(255) NOT NULL
                    );
                """
                )
            # self.conn.commit()

            self.cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS um_school_results(
                        id SERIAL PRIMARY KEY NOT NULL,
                        school_id VARCHAR(255) REFERENCES um_school(id) NOT NULL,
                        average_point JSONB,
                        results JSONB,
                        exam_year CHARACTER VARYING(255),
                        exam_quarter CHARACTER VARYING(255)
                    );
                """
                )
            # self.conn.commit()

            self.cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS um_teachers(
                        id SERIAL PRIMARY KEY NOT NULL,
                        year CHARACTER VARYING(255) NOT NULL,
                        territory VARCHAR(255) NOT NULL,
                        school VARCHAR(255) NOT NULL,
                        teachers_count INTEGER NOT NULL,
                        women_teachers_count INTEGER NOT NULL,
                        women_teachers_percentage FLOAT NOT NULL,
                        men_teachers_count INTEGER NOT NULL,
                        men_teachers_percentage FLOAT NOT NULL,
                        special_teachers_count INTEGER NOT NULL,
                        special_teachers_percentage FLOAT NOT NULL,
                        second_category_teachers_count INTEGER NOT NULL,
                        second_category_teachers_percentage FLOAT NOT NULL,
                        first_category_teachers_count INTEGER NOT NULL,
                        first_category_teachers_percentage FLOAT NOT NULL,
                        highest_category_teachers_count INTEGER NOT NULL,
                        highest_category_teachers_percentage FLOAT NOT NULL
                    );
                """
                )
            # self.conn.commit()

            self.cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS um_student_exams(
                        id CHARACTER VARYING(50) PRIMARY KEY NOT NULL NOT NULL,
                        student_id CHARACTER VARYING(50) NOT NULL,
                        name CHARACTER VARYING(255) NOT NULL,
                        surname CHARACTER VARYING(255) NOT NULL,
                        patronymic CHARACTER VARYING(255),
                        studyLang CHARACTER VARYING(255),
                        studyClass CHARACTER VARYING(255),
                        studyStream CHARACTER VARYING(255),
                        exam_year CHARACTER VARYING(255),
                        exam_quarter CHARACTER VARYING(255),
                        results JSONB,
                        average_point FLOAT,
                        exam_method CHARACTER VARYING(255),
                        school_id CHARACTER VARYING(255) REFERENCES um_school(id) NOT NULL
                        );
                """
                )
            # self.conn.commit()

            self.cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS um_rate(
                        id SERIAL PRIMARY KEY NOT NULL,
                        Knowing_Question_count INTEGER NOT NULL,
                        Knowing_point_per_question INTEGER NOT NULL,
                        Applying_Question_count INTEGER NOT NULL,
                        Applying_point_per_question INTEGER NOT NULL,
                        Reviewing_Question_count INTEGER NOT NULL,
                        Reviewing_point_per_question INTEGER NOT NULL,
                        all_question_count INTEGER NOT NULL,
                        max_point_over_all INTEGER NOT NULL,
                        exam_year CHARACTER VARYING(255),
                        exam_quarter CHARACTER VARYING(255),
                        subject CHARACTER VARYING(255) NOT NULL
                        )
                """
            )
            self.conn.commit()

    def create_indexes(self):
        self.cur.execute(
            """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_um_school ON um_school (id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_um_rate ON um_rate (exam_year, exam_quarter, subject);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_um_school_results ON um_school_results (school_id, exam_year, exam_quarter);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_um_student_exams ON um_student_exams (id, student_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_um_teachers ON um_teachers (year, territory, school);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_um_users ON um_users (username);
                CREATE INDEX IF NOT EXISTS idx_exam_year_quarter ON um_student_exams (exam_year, exam_quarter);

            """
        )
        self.conn.commit()

    def insert_admins(self):
        admin_hashed_password = bcrypt.hashpw(ADMIN_CREDENTIALS[1].encode('utf-8'), bcrypt.gensalt(rounds=10)).decode('utf-8')
        superadmin_hashed_password = bcrypt.hashpw(SUPERADMIN_CREDENTIALS[1].encode('utf-8'), bcrypt.gensalt(rounds=10)).decode('utf-8')

        with self.conn:
            self.cur.execute(
                """
                    INSERT INTO um_users(username, password, role)
                    VALUES (%s, %s, %s), (%s, %s, %s)
                    ON CONFLICT(username) DO NOTHING;
                """, (ADMIN_CREDENTIALS[0], admin_hashed_password, ADMIN_ROLE, 
                      SUPERADMIN_CREDENTIALS[0], superadmin_hashed_password, SUPERADMIN_ROLE)
            )
            self.conn.commit()

    def get_admin(self, user: UserLoginBody) -> User:
        with self.conn:
            # Query to fetch the user
            self.cur.execute(
                """
                    SELECT id, username, password, created_at, last_login, role
                    FROM um_users
                    WHERE username = %s AND role IN (%s, %s)
                """,
                (user.username, SUPERADMIN_ROLE, ADMIN_ROLE)
            )
            
            # Fetch the row
            row = self.cur.fetchone()
            
            if not row:
                return None  # Return None if no matching user is found
            
            # Map the row to the User model
            user_data = {
                "id": row[0],
                "username": row[1],
                "password": row[2],
                "createdAt": row[3],
                "lastLogin": row[4],
                "role": row[5],
            }

            if bcrypt.checkpw(user.password.encode('utf-8'), user_data['password'].encode('utf-8')):
                # Create a User instance
                user_model = User(**user_data)
                return user_model
        return None

    def get_schools_by_req(self, data: SchoolListRequest):
        with self.conn:
            query = """
                SELECT JSON_AGG(
                            DISTINCT um_school.name) schools
                FROM um_student_exams
                        LEFT JOIN um_school ON school_id = um_school.id
                WHERE exam_quarter = %s
                AND exam_year = %s
                AND territory = %s
                AND region = %s;
            """

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (data.exam_quarter, data.exam_year, data.territory, data.region))
                results = cursor.fetchone()
            
        return results['schools']
    
    def get_regions_by_territory(self, region_list: RegionListRequest):
        with self.conn:
            query = """
                SELECT JSON_AGG(
                            DISTINCT region) regions
                FROM um_student_exams
                        LEFT JOIN um_school ON school_id = um_school.id
                WHERE exam_quarter = %s
                AND exam_year = %s
                AND territory = %s;
            """

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (region_list.exam_quarter, region_list.exam_year, region_list.territory))
                results = cursor.fetchone()
            
        return results['regions']
    
    def get_available_periods(self):
        with self.conn:
            query = """
                WITH periods AS (
                    SELECT DISTINCT exam_year, exam_quarter
                    FROM um_rate
                ),
                grouped_periods AS (
                    SELECT exam_year, JSON_AGG(exam_quarter ORDER BY exam_quarter) AS quarters
                    FROM periods
                    GROUP BY exam_year
                    ORDER BY exam_year DESC
                )
                SELECT JSON_OBJECT_AGG(exam_year, quarters) AS result
                FROM grouped_periods;
            """

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchone()

            return results['result']
        
    def get_available_paired_periods(self):
        with self.conn:
            query = """
                WITH periods AS (
                    SELECT DISTINCT exam_year, exam_quarter
                    FROM um_rate
                ),
                grouped_periods AS (
                    SELECT exam_year, JSON_AGG(exam_quarter ORDER BY exam_quarter) AS quarters
                    FROM periods
                    GROUP BY exam_year
                    HAVING COUNT(exam_quarter) > 1
                    ORDER BY exam_year DESC
                )
                SELECT JSON_OBJECT_AGG(exam_year, quarters) AS result
                FROM grouped_periods;
            """

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchone()

            return results['result']

    def get_last_year_and_quarter(self):
        with self.conn:
            query = """
                SELECT exam_year, exam_quarter
                FROM um_rate
                ORDER BY exam_year DESC, exam_quarter DESC
                LIMIT 1;
            """

            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchone()

            return results
        
    def get_base_results(self, base_request: BaseRequest):
        with self.conn:
            query = """
                WITH results AS (SELECT *
                 FROM um_student_exams
                 WHERE exam_year = %(exam_year)s
                   AND exam_quarter = %(exam_quarter)s),
     rates AS (SELECT max_point_over_all, subject
               FROM um_rate
               WHERE exam_year = %(exam_year)s
                 AND exam_quarter = %(exam_quarter)s),
     all_count AS (SELECT COUNT(*) AS count
                   FROM results),
     exam_methods_data AS (SELECT exam_method,
                                  COUNT(*) AS count,
                                  ROUND(COUNT(*)::numeric / (SELECT count FROM all_count)::numeric * 100,
                                        1) AS percentage
                           FROM results
                           GROUP BY exam_method),
     territories_data AS (SELECT territory, um_school.name as school_name, average_point, school_id, region
                          FROM results
                                   LEFT JOIN um_school ON school_id = um_school.id),
     schools_count_by_region AS (SELECT territory, COUNT(DISTINCT school_id) AS school_count
                                 FROM territories_data
                                 GROUP BY territory
                                 ORDER BY COUNT(DISTINCT school_name) DESC),
     all_school_count AS (SELECT SUM(school_count) AS all_schools_count
                          FROM schools_count_by_region),
     avg_by_territory AS (SELECT territory,
                                 ROUND(AVG(average_point / (SELECT AVG(max_point_over_all) FROM rates) * 100)::numeric,
                                       1) AS data
                          FROM territories_data
                          GROUP BY territory
                          ORDER BY ROUND(AVG(average_point)::numeric, 1)),
     avg_by_territory_with_avg_all AS (SELECT 'Barcha hudular' AS territory,
                                              ROUND(
                                                      AVG(average_point / (SELECT AVG(max_point_over_all) FROM rates) * 100)::numeric,
                                                      1
                                              )  AS data
                                       FROM territories_data
                                       UNION ALL
                                       SELECT *
                                       FROM avg_by_territory),
     avg_by_school AS (SELECT territory,
                              school_name,
                              region,
                              ROUND(AVG(average_point / (SELECT AVG(max_point_over_all) FROM rates) * 100)::numeric,
                                    1) AS data
                       FROM territories_data
                       GROUP BY territory, region, school_id, school_name
                       ORDER BY ROUND(AVG(average_point)::numeric, 1)),
    teachers_info AS (SELECT *
                       FROM um_teachers
                       WHERE year = %(exam_year)s
                       ORDER BY teachers_count DESC),
    teachers_schools AS (SELECT json_agg(
                        json_build_object(
                            'territory', territory,
                            'school', school_array
                        )
                    ) AS result
                FROM (
                    SELECT territory, json_agg(school) AS school_array
                    FROM um_teachers
                    WHERE year = %(exam_year)s
                    GROUP BY territory
                    ORDER BY AVG(teachers_count) DESC
                ) subquery),
    teachers_info_by_territory AS (SELECT territory,
                                          SUM(teachers_count)                  as teachers_count,
                                          SUM(women_teachers_count)            as women_teachers_count,
                                          SUM(men_teachers_count)              as men_teachers_count,
                                          SUM(special_teachers_count)          as special_teachers_count,
                                          SUM(first_category_teachers_count)   as first_category_teachers_count,
                                          SUM(second_category_teachers_count)  as second_category_teachers_count,
                                          SUM(highest_category_teachers_count) as highest_category_teachers_count,
                                          ROUND((SUM(women_teachers_count)::NUMERIC / SUM(teachers_count)::NUMERIC *
                                                 100),
                                                2)                             as women_teachers_percentage,
                                          ROUND((SUM(men_teachers_count)::NUMERIC / SUM(teachers_count)::NUMERIC) * 100,
                                                2)                             as men_teachers_percentage,
                                          ROUND((SUM(special_teachers_count)::NUMERIC / SUM(teachers_count)::NUMERIC) *
                                                100,
                                                2)                             as special_teachers_percentage,
                                          ROUND((SUM(first_category_teachers_count)::NUMERIC /
                                                 SUM(teachers_count)::NUMERIC) * 100,
                                                2)                             as first_category_teachers_percentage,
                                          ROUND((SUM(second_category_teachers_count)::NUMERIC /
                                                 SUM(teachers_count)::NUMERIC) * 100,
                                                2)                             as second_category_teachers_percentage,
                                          ROUND((SUM(highest_category_teachers_count)::NUMERIC /
                                                 SUM(teachers_count)::NUMERIC) * 100,
                                                2)                             as highest_category_teachers_percentage
                                   FROM um_teachers
                                   WHERE year = %(exam_year)s
                                     AND territory != 'Barcha hududlar'
                                   GROUP BY territory
                                   UNION ALL
                                   SELECT territory,
                                          teachers_count,
                                          women_teachers_count,
                                          men_teachers_count,
                                          special_teachers_count,
                                          first_category_teachers_count,
                                          second_category_teachers_count,
                                          highest_category_teachers_count,
                                          women_teachers_percentage,
                                          men_teachers_percentage,
                                          special_teachers_percentage,
                                          first_category_teachers_percentage,
                                          second_category_teachers_percentage,
                                          highest_category_teachers_percentage
                                   FROM um_teachers
                                   WHERE year = %(exam_year)s
                                     AND territory = 'Barcha hududlar'),
     all_region_count AS (SELECT COUNT(DISTINCT territory) as count FROM school),
     all_teachers_count AS (SELECT COALESCE(SUM(teachers_count), 0) as count FROM teachers_info WHERE territory = 'Barcha hududlar')
SELECT json_build_object(
               'all_count', (SELECT count FROM all_count),
               'exam_methods_data', (SELECT json_agg(json_build_object(
                'exam_method', exam_method,
                'count', count,
                'percentage', percentage
                                                     ))
                                     FROM exam_methods_data),
               'territories_data', (SELECT json_agg(json_build_object(
                'territory', territory,
                'school_count', school_count
                                                    ))
                                    FROM schools_count_by_region),
               'avg_by_territory', (SELECT json_agg(json_build_object(
                'territory', territory,
                'average', data
                                                    ))
                                    FROM avg_by_territory_with_avg_all),
               'avg_by_school', (SELECT json_agg(json_build_object(
                'territory', territory,
                'school', school_name,
                'region', region,
                'average', data
                                                 ))
                                 FROM avg_by_school),
               'teachers_info', (SELECT json_agg(json_build_object(
                'territory', territory,
                'school', school,
                'teachers_count', teachers_count,
                'women_teachers_count', women_teachers_count,
                'women_teachers_percentage', women_teachers_percentage,
                'men_teachers_count', men_teachers_count,
                'men_teachers_percentage', men_teachers_percentage,
                'special_teachers_count', special_teachers_count,
                'special_teachers_percentage', special_teachers_percentage,
                'first_category_teachers_count', first_category_teachers_count,
                'first_category_teachers_percentage', first_category_teachers_percentage,
                'second_category_teachers_count', second_category_teachers_count,
                'second_category_teachers_percentage', second_category_teachers_percentage,
                'highest_category_teachers_count', highest_category_teachers_count,
                'highest_category_teachers_percentage', highest_category_teachers_percentage
                                                 ))
                                 FROM teachers_info),
               'teachers_info_by_territory', (SELECT json_agg(row_to_json(t)) AS json_object
                                             FROM teachers_info_by_territory AS t),
                'teachers_schools', (SELECT result FROM teachers_schools),
               'all_schools_count', (SELECT all_schools_count FROM all_school_count),
               'all_regions_count', (SELECT count FROM all_region_count),
               'all_teachers_count', (SELECT count FROM all_teachers_count)) AS result;

                """
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, {
                    "exam_year": base_request.exam_year,
                    "exam_quarter": base_request.exam_quarter,
                })
                results = cursor.fetchone()
            
        return results['result']
            
    def get_school_results(self, school_request: SchoolRequest):
        # Base query with placeholders for studyClass and territory filters
        base_query = """
            WITH rates AS (SELECT max_point_over_all, subject
               FROM um_rate
               WHERE exam_quarter = %s
                 AND exam_year = %s),
     school_results AS (SELECT um_school.id AS school_id,
                               um_school.region    AS region,
                               um_school.name      AS school,

                               ROUND(((average_point -> 'all' ->> '{avg_key}')::numeric /
                                      (SELECT AVG(max_point_over_all) FROM rates)) * 100,
                                     1)            AS average,
                               COALESCE(
                                       ROUND(
                                               ((results -> 'all' -> '{result_key}' -> 'math_5&6' ->> 'all_point')::numeric /
                                                (SELECT max_point_over_all FROM rates WHERE subject = 'math_5&6')) *
                                               100, 1),
                                       ROUND(((results -> 'all' -> '{result_key}' -> 'math_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_7')) * 100, 1)
                               )                   AS math,
                               COALESCE(
                                       ROUND(((results -> 'all' -> '{result_key}' -> 'mother_tongue_literature_7' ->>
                                               'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_7')) * 100, 1),
                                       ROUND(((results -> 'all' -> '{result_key}' ->
                                               'mother_tongue_literature_8&10&11' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_8&10&11')) * 100, 1)
                               )                   AS mother_tongue_literature,
                               ROUND(((results -> 'all' -> '{result_key}' -> 'literature_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'literature_5&6')) * 100,
                                     1)            AS literature,
                               ROUND(
                                       ((results -> 'all' -> '{result_key}' -> 'mother_tongue_5&6' ->> 'all_point')::numeric /
                                        (SELECT max_point_over_all FROM rates WHERE subject = 'mother_tongue_5&6')) *
                                       100,
                                       1)          AS mother_tongue,
                               ROUND(
                                       ((results -> 'all' -> '{result_key}' -> 'russian-qaraqalpaq_5&6' ->> 'all_point')::numeric) /
                                       (SELECT max_point_over_all FROM rates WHERE subject = 'russian-qaraqalpaq_5&6') *
                                       100,
                                       1)          AS russian,
                               ROUND(((results -> 'all' -> '{result_key}' -> 'chemistry_8' ->> 'all_point')::numeric) /
                                     (SELECT max_point_over_all FROM rates WHERE subject = 'chemistry_8') * 100,
                                     1)            AS chemistry,
                               ROUND(((results -> 'all' -> '{result_key}' -> 'biology_7' ->> 'all_point')::numeric) /
                                     (SELECT max_point_over_all FROM rates WHERE subject = 'biology_7') * 100,
                                     1)            AS biology,
                               ROUND(
                                       ((results -> 'all' -> '{result_key}' -> 'english_9&10&11' ->> 'all_point')::numeric) /
                                       (SELECT max_point_over_all FROM rates WHERE subject = 'english_9&10&11') * 100,
                                       1)          AS english,
                               ROUND(((results -> 'all' -> '{result_key}' -> 'physics_9' ->> 'all_point')::numeric) /
                                     (SELECT max_point_over_all FROM rates WHERE subject = 'physics_9') * 100,
                                     1)            AS physics,
                               ROUND(
                                       ((results -> 'all' -> '{result_key}' -> 'algebra_8&9&10&11' ->> 'all_point')::numeric) /
                                       (SELECT max_point_over_all FROM rates WHERE subject = 'algebra_8&9&10&11') * 100,
                                       1)          AS algebra,
                               ROUND(
                                       ((results -> 'all' -> '{result_key}' -> 'geometry_8&9&10&11' ->> 'all_point')::numeric) /
                                       (SELECT max_point_over_all FROM rates WHERE subject = 'geometry_8&9&10&11') *
                                       100,
                                       1)          AS geometry
                        FROM um_school_results
                                 LEFT JOIN um_school ON um_school_results.school_id = um_school.id
                        WHERE exam_quarter = %s
                          AND exam_year = %s
                          {territory_filter}
                        ORDER BY um_school.territory),
                        """
        if school_request.subject:
            limited_query = f"""
                    limited_school_results AS (
                        SELECT 
                            school_id,
                            region,
                            school,
                            {school_request.subject}
                        FROM school_results
                        ORDER BY {school_request.subject} DESC NULLS LAST
                        LIMIT 20 OFFSET ({school_request.page}-1) * 20
                    ),"""
        else:
            limited_query = f"""
                    limited_school_results AS (
                        SELECT * FROM school_results
                        ORDER BY average DESC NULLS LAST
                        LIMIT 20 OFFSET ({school_request.page}-1) * 20
                    ),"""
        
        result_query = """
                pages AS (
                    SELECT CEIL(COUNT(*) / 20.0) FROM school_results
                )
SELECT JSON_BUILD_OBJECT('school_results', (SELECT JSON_AGG(limited_school_results) FROM limited_school_results),
       'pages', (SELECT * FROM pages)) AS result;
            """
        
        query = base_query + limited_query + result_query

        
        # Extract the request parameters
        exam_quarter = school_request.exam_quarter
        exam_year = school_request.exam_year
        studyClass = school_request.study_class
        territory = school_request.territory

        # Determine the appropriate key for results
        results_key = 'school_avg' if studyClass is None else f"{studyClass}"
        avg_key = 'overall' if studyClass is None else f"{studyClass}"
        query = query.format(
            avg_key=avg_key,
            result_key=results_key,
            territory_filter="AND territory = %s" if territory else ""
        )

        # Build the parameter list for query execution
        params = [exam_quarter, exam_year, exam_quarter, exam_year]
        if territory:
            params.append(territory)

        # with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        # Execute query with cursor
        with self.conn:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchone()

        return results['result']

    def get_students_results(self, students_request: StudentRequest):
        with self.conn:
            base_query = """
                WITH rates AS (SELECT max_point_over_all, subject
               FROM um_rate
               WHERE exam_quarter = %s
                 AND exam_year = %s),
     student_results AS (SELECT
                             um_school.region,
                             um_school.name,
                                CONCAT(um_student_exams.surname, '. ', LEFT(um_student_exams.name, 1), '. ',
                                       LEFT(um_student_exams.patronymic, 1),
                                       '.')                 as full_name,
                                CONCAT(studystream, '-sinf') as study_class,
                                ROUND(average_point::numeric / (SELECT AVG(max_point_over_all) FROM rates) * 100,
                                      1)                       average,

                                COALESCE(
                                        ROUND((results -> 'math_5&6' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_5&6') *
                                              100, 1),
                                        ROUND((results -> 'math_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_7') *
                                              100, 1)
                                )                              math,
                                COALESCE(
                                        ROUND((results -> 'mother_tongue_literature_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_7&6') *
                                              100, 1),
                                        ROUND((results -> 'mother_tongue_literature_8&10&11' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_8&10&11') *
                                              100, 1)
                                )                              mother_tongue_literature,
                                ROUND((results -> 'literature_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'literature_5&6') * 100,
                                      1)                       literature,
                                ROUND((results -> 'mother_tongue_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'mother_tongue_5&6') * 100,
                                      1)                       mother_tongue,
                                ROUND((results -> 'russian-qaraqalpaq_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'russian-qaraqalpaq_5&6') *
                                      100,
                                      1)                       russian,
                                ROUND((results -> 'chemistry_8' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'chemistry_8') * 100,
                                      1)                       chemistry,
                                ROUND((results -> 'biology_7' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'biology_7') * 100,
                                      1)                       biology,
                                ROUND((results -> 'english_9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'english_9&10&11') * 100,
                                      1)                       english,
                                ROUND((results -> 'physics_9' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'physics_9') * 100,
                                      1)                       physics,
                                ROUND((results -> 'algebra_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'algebra_8&9&10&11') * 100,
                                      1)                       algebra,
                                ROUND((results -> 'geometry_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'geometry_8&9&10&11') * 100,
                                      1)                       geometry
                         FROM um_student_exams
                                  LEFT JOIN um_school ON um_student_exams.school_id = um_school.id
                WHERE exam_quarter = %s
                    AND exam_year = %s
                    AND (%s IS NULL OR um_school.territory = %s)
                    AND (%s IS NULL OR um_school.region = %s)
                    AND (%s IS NULL OR um_school.name = %s)
                    AND (%s IS NULL OR studyclass = %s)),"""
            
            limited_query = f"""
                limited_school_results AS (SELECT *
                                FROM student_results
                                ORDER BY { students_request.subject if students_request.subject  else 'average'} DESC NULLS LAST
                                LIMIT 20 OFFSET ({students_request.page} - 1) * 20),
                                """
            
            if students_request.subject:
                result_query = f"""
                    pages AS (SELECT CEIL(COUNT(*) / 20.0)
                            FROM student_results)
                SELECT JSON_BUILD_OBJECT('results', JSON_AGG(
                        JSON_BUILD_OBJECT(
                                'region', region,
                                'school', name,
                                'full_name', full_name,
                                'class', study_class,
                                '{students_request.subject}', {students_request.subject}
                                
                        )
                                                    ),
                    'total_pages', (SELECT * FROM pages)
                    ) AS result
                FROM limited_school_results ;
                                """
            else:
                result_query = f"""
                    pages AS (SELECT CEIL(COUNT(*) / 20.0)
                            FROM student_results)
                SELECT JSON_BUILD_OBJECT('results', JSON_AGG(
                        JSON_BUILD_OBJECT(
                                'region', region,
                                'school', name,
                                'full_name', full_name,
                                'class', study_class,
                                'average', average,
                                'math', math,
                                'mother_tongue_literature', mother_tongue_literature,
                                'literature', literature,
                                'mother_tongue', mother_tongue,
                                'russian', russian,
                                'algebra', algebra,
                                'geometry', geometry,
                                'physics', physics,
                                'chemistry', chemistry,
                                'biology', biology,
                                'english', english
                        )
                                                    ),
                    'total_pages', (SELECT * FROM pages)
                    ) AS result
                FROM limited_school_results ;
                                """
            
            # print(students_request)
            
            params = [
                students_request.exam_quarter,
                students_request.exam_year,
                students_request.exam_quarter,
                students_request.exam_year,
                students_request.territory, students_request.territory,
                students_request.region, students_request.region,
                students_request.school, students_request.school,
                students_request.study_class, students_request.study_class,
            ]

            query = base_query + limited_query + result_query

            # Execute the query
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchone()

            return results['result']
    
    def get_results(self, params: ResultRequest):

        # Base query setup for required fields
        base_query = f"""
            WITH rates AS (
                SELECT max_point_over_all, subject
                FROM um_rate
                WHERE exam_year = %(exam_year)s
                AND exam_quarter = %(exam_quarter)s
            ),
            results AS (
                SELECT um_school.territory,
                    um_school.region,
                    um_school.id as school_id,
                    um_school.name,
                    um_student_exams.student_id,
                    CONCAT(um_student_exams.surname, '. ', LEFT(um_student_exams.name, 1), '. ',
                            LEFT(um_student_exams.patronymic, 1), '.') AS full_name,
                    studystream,
                    exam_method,
                    ROUND(average_point::numeric / (SELECT AVG(max_point_over_all) FROM rates) * 100,
                                      1)                        average,

                                COALESCE(
                                        ROUND((results -> 'math_5&6' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_5&6') *
                                              100, 1),
                                        ROUND((results -> 'math_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_7') *
                                              100, 1)
                                )                               math,
                                COALESCE(
                                        ROUND((results -> 'mother_tongue_literature_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_7&6') *
                                              100, 1),
                                        ROUND((results -> 'mother_tongue_literature_8&10&11' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_8&10&11') *
                                              100, 1)
                                )                               mother_tongue_literature,
                                ROUND((results -> 'literature_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'literature_5&6') * 100,
                                      1)                        literature,
                                ROUND((results -> 'mother_tongue_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'mother_tongue_5&6') * 100,
                                      1)                        mother_tongue,
                                ROUND((results -> 'russian-qaraqalpaq_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'russian-qaraqalpaq_5&6') *
                                      100,
                                      1)                        russian,
                                ROUND((results -> 'chemistry_8' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'chemistry_8') * 100,
                                      1)                        chemistry,
                                ROUND((results -> 'biology_7' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'biology_7') * 100,
                                      1)                        biology,
                                ROUND((results -> 'english_9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'english_9&10&11') * 100,
                                      1)                        english,
                                ROUND((results -> 'physics_9' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'physics_9') * 100,
                                      1)                        physics,
                                ROUND((results -> 'algebra_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'algebra_8&9&10&11') * 100,
                                      1)                        algebra,
                                ROUND((results -> 'geometry_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'geometry_8&9&10&11') * 100,
                                      1)                        geometry
                FROM um_student_exams
                LEFT JOIN um_school ON um_student_exams.school_id = um_school.id
                WHERE exam_quarter = %(exam_quarter)s
                AND exam_year = %(exam_year)s
            ),
        """

        # Define avg_column
        avg_column = f"AVG({params.subject})" if params.subject else "AVG(average)"
        
        # `avg_by_territory` CTE with fallback conditions
        avg_by_territory_query = f"""
            avg_by_territory AS (
                SELECT territory, ROUND({avg_column}::numeric, 1) AS data
                FROM results
                WHERE {'exam_method = %(exam_method)s' if params.exam_method else '1=1'}
                AND {'studystream = %(study_class)s' if params.study_class else '1=1'}
                GROUP BY territory
                HAVING {avg_column} IS NOT NULL
                ORDER BY {avg_column}
            ), 
        """

        # Define subject avg fields
        avg_fields = f"ROUND(AVG({params.subject})::numeric, 1) AS {params.subject}_avg" if params.subject else """
            ROUND(AVG(average)::numeric, 1)   AS _avg,
            ROUND(AVG(math)::numeric, 1)                     AS math_avg,
            ROUND(AVG(mother_tongue)::numeric, 1)            AS mother_tongue_avg,
            ROUND(AVG(literature)::numeric, 1)               AS literature_avg,
            ROUND(AVG(mother_tongue_literature)::numeric, 1) AS mother_tongue_literature_avg,
            ROUND(AVG(russian)::numeric, 1)                  AS russian_avg,
            ROUND(AVG(algebra)::numeric, 1)                  AS algebra_avg,
            ROUND(AVG(geometry)::numeric, 1)                 AS geometry_avg,
            ROUND(AVG(physics)::numeric, 1)                  AS physics_avg,
            ROUND(AVG(biology)::numeric, 1)                  AS biology_avg,
            ROUND(AVG(chemistry)::numeric, 1)                AS chemistry_avg,
            ROUND(AVG(english)::numeric, 1)                  AS english_avg
        """
        
        # `subject_results` CTE with fallback grouping
        subject_results_query = f"""
            subject_results AS (
                SELECT {avg_fields},
                    {'territory as key' if not params.territory else 'name as key' }
                FROM results
                WHERE {'exam_method = %(exam_method)s' if params.exam_method else '1=1'}
                AND {'studystream = %(study_class)s' if params.study_class else '1=1'}
                AND {'territory = %(territory)s' if params.territory else '1=1'}
                AND {'region = %(region)s' if params.region else '1=1'}
                GROUP BY {'territory' if not params.territory else 'school_id, name'}
                {'HAVING AVG('+ params.subject + ') IS NOT NULL' if params.subject else ''}
            ),
        """

        subject_avg_column = f"ROUND(AVG({params.subject})::numeric, 1) AS avg" if params.subject else "ROUND(AVG(average)::numeric, 1) AS avg"

        study_class_results_query = f"""
            study_class_results AS (
                SELECT {subject_avg_column},
                    studystream as studyclass
                FROM results
                WHERE {'exam_method = %(exam_method)s' if params.exam_method else '1=1'}
                AND {'territory = %(territory)s' if params.territory else '1=1'}
                AND {'region = %(region)s' if params.region else '1=1'}
                AND {'name = %(school)s' if params.school else '1=1'}
                GROUP BY studystream
                {'HAVING AVG('+ params.subject + ') IS NOT NULL' if params.subject else ''}
                ORDER BY studystream::int
            ),
        """

        # `some_subject_result` CTE
        filter_conditions = " AND ".join(
            f"{db_column} = %({model_field})s"
            for model_field, db_column in {
                "exam_method": "exam_method",
                "study_class": "studystream",
                "territory": "territory",
                "region": "region",
                "school": "name"
            }.items() if getattr(params, model_field) is not None
        )
        some_subject_result_query = f"""
            some_subject_result AS (
                SELECT ROUND(AVG(average)::numeric, 1)   AS _avg,
                    ROUND(AVG(math)::numeric, 1)                     AS math_avg,
                    ROUND(AVG(mother_tongue)::numeric, 1)            AS mother_tongue_avg,
                    ROUND(AVG(literature)::numeric, 1)               AS literature_avg,
                    ROUND(AVG(mother_tongue_literature)::numeric, 1) AS mother_tongue_literature_avg,
                    ROUND(AVG(russian)::numeric, 1)                  AS russian_avg,
                    ROUND(AVG(algebra)::numeric, 1)                  AS algebra_avg,
                    ROUND(AVG(geometry)::numeric, 1)                 AS geometry_avg,
                    ROUND(AVG(physics)::numeric, 1)                  AS physics_avg,
                    ROUND(AVG(biology)::numeric, 1)                  AS biology_avg,
                    ROUND(AVG(chemistry)::numeric, 1)                AS chemistry_avg,
                    ROUND(AVG(english)::numeric, 1)                  AS english_avg
                FROM results
                WHERE {filter_conditions or '1=1'}
            ),
        """

        # Mapping for filter conditions in `students_filter`
        column_map = {
            "study_class": "studystream",
            "territory": "territory",
            "region": "region",
            "school": "name"  # `school` in model maps to `name` in the database
        }

        # Generate `students_filter` conditions
        students_filter_conditions = " AND ".join(
            f"{db_column} = %({model_field})s"
            for model_field, db_column in column_map.items()
            if getattr(params, model_field) is not None
        )
        subject_condition = f"{params.subject} IS NOT NULL" if params.subject else "1=1"

        # `students_filter` and `exam_method_results` CTE
        students_filter_query = f"""
            students_filter AS (
                SELECT student_id, average, exam_method
                FROM results
                WHERE {students_filter_conditions or '1=1'} AND {subject_condition}
            ),
            exam_method_results AS (
                SELECT exam_method,
                    (SELECT COUNT(*) FROM students_filter) students_count,
                    COUNT(*) as count,
                    ROUND((COUNT(*)::numeric / (SELECT COUNT(*) FROM students_filter)::numeric) * 100, 1) AS percentage,
                    ROUND(AVG(average::numeric), 1) AS result
                FROM students_filter
                GROUP BY exam_method
            ),
        """

        # `students_results` CTE with dynamic group column
        students_results_query = f"""
            students_results AS (
                SELECT  region as region,
                        school_id as school_id,
                        {params.school and 'full_name' or 'name'},
                        ROUND(AVG(average)::numeric, 1)                  AS average,
                        ROUND(AVG(math)::numeric, 1)                     AS math,
                        ROUND(AVG(mother_tongue)::numeric, 1)            AS mother_tongue,
                        ROUND(AVG(literature)::numeric, 1)               AS literature,
                        ROUND(AVG(mother_tongue_literature)::numeric, 1) AS mother_tongue_literature,
                        ROUND(AVG(russian)::numeric, 1)                  AS russian,
                        ROUND(AVG(algebra)::numeric, 1)                  AS algebra,
                        ROUND(AVG(geometry)::numeric, 1)                 AS geometry,
                        ROUND(AVG(physics)::numeric, 1)                  AS physics,
                        ROUND(AVG(biology)::numeric, 1)                  AS biology,
                        ROUND(AVG(chemistry)::numeric, 1)                AS chemistry,
                        ROUND(AVG(english)::numeric, 1)                  AS english
                FROM results
                WHERE {filter_conditions or '1=1'}
                GROUP BY {params.school and 'region, school_id, student_id, full_name' or 'region, school_id, name'}
            ),
            limited_student_results AS (SELECT *
                                FROM students_results
                                LIMIT 20 OFFSET ({params.page} - 1) * 20),
     pages AS (SELECT CEIL(COUNT(*) / 20.0)
               FROM students_results)
        """

        # Final query
        query = base_query + avg_by_territory_query + subject_results_query + study_class_results_query + some_subject_result_query + students_filter_query + students_results_query + """
            SELECT json_build_object(
                'avg_by_territory', (SELECT json_agg(avg_by_territory) FROM avg_by_territory),
                'subject_results', (SELECT json_agg(subject_results) FROM subject_results),
                'study_class_results', (SELECT json_agg(study_class_results) FROM study_class_results),
                'some_subject_result', (SELECT json_agg(some_subject_result) FROM some_subject_result),
                'students_results', (SELECT json_agg(limited_student_results) FROM limited_student_results),
                'exam_method_results', (SELECT json_agg(exam_method_results) FROM exam_method_results),
                'total_pages', (SELECT * FROM pages)
            ) AS results;
        """

        # Execute the query
        with self.conn:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, {
                    "exam_year": params.exam_year,
                    "exam_quarter": params.exam_quarter,
                    "territory": params.territory,
                    "school": params.school,
                    "exam_method": params.exam_method,
                    "study_class": params.study_class,
                    "subject": params.subject,
                    "region" : params.region,
                })
                result = cursor.fetchone()
            
        return result['results']

    def get_results_table(self, params: ResultRequest):
        # Base query setup for required fields
        base_query = f"""
            WITH rates AS (
                SELECT max_point_over_all, subject
                FROM um_rate
                WHERE exam_year = %(exam_year)s
                AND exam_quarter = %(exam_quarter)s
            ),
            results AS (
                SELECT um_school.territory,
                    um_student_exams.name,
                    CONCAT(um_student_exams.surname, '. ', LEFT(um_student_exams.name, 1), '. ',
                            LEFT(um_student_exams.patronymic, 1), '.') AS full_name,
                    studystream,
                    exam_method,
                    ROUND(average_point::numeric / (SELECT AVG(max_point_over_all) FROM rates) * 100,
                                      1)                        average,

                                COALESCE(
                                        ROUND((results -> 'math_5&6' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_5&6') *
                                              100, 1),
                                        ROUND((results -> 'math_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_7') *
                                              100, 1)
                                )                               math,
                                COALESCE(
                                        ROUND((results -> 'mother_tongue_literature_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_7&6') *
                                              100, 1),
                                        ROUND((results -> 'mother_tongue_literature_8&10&11' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_8&10&11') *
                                              100, 1)
                                )                               mother_tongue_literature,
                                ROUND((results -> 'literature_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'literature_5&6') * 100,
                                      1)                        literature,
                                ROUND((results -> 'mother_tongue_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'mother_tongue_5&6') * 100,
                                      1)                        mother_tongue,
                                ROUND((results -> 'russian-qaraqalpaq_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'russian-qaraqalpaq_5&6') *
                                      100,
                                      1)                        russian,
                                ROUND((results -> 'chemistry_8' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'chemistry_8') * 100,
                                      1)                        chemistry,
                                ROUND((results -> 'biology_7' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'biology_7') * 100,
                                      1)                        biology,
                                ROUND((results -> 'english_9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'english_9&10&11') * 100,
                                      1)                        english,
                                ROUND((results -> 'physics_9' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'physics_9') * 100,
                                      1)                        physics,
                                ROUND((results -> 'algebra_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'algebra_8&9&10&11') * 100,
                                      1)                        algebra,
                                ROUND((results -> 'geometry_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'geometry_8&9&10&11') * 100,
                                      1)                        geometry
                FROM um_student_exams
                LEFT JOIN um_school ON um_student_exams.school_id = um_school.id
                WHERE exam_quarter = %(exam_quarter)s
                AND exam_year = %(exam_year)s
            ),
            students_results AS (SELECT name,
                                 ROUND(AVG(average)::numeric, 1)                  AS average,
                                 ROUND(AVG(math)::numeric, 1)                     AS math,
                                 ROUND(AVG(mother_tongue)::numeric, 1)            AS mother_tongue,
                                 ROUND(AVG(literature)::numeric, 1)               AS literature,
                                 ROUND(AVG(mother_tongue_literature)::numeric, 1) AS mother_tongue_literature,
                                 ROUND(AVG(russian)::numeric, 1)                  AS russian,
                                 ROUND(AVG(algebra)::numeric, 1)                  AS algebra,
                                 ROUND(AVG(geometry)::numeric, 1)                 AS geometry,
                                 ROUND(AVG(physics)::numeric, 1)                  AS physics,
                                 ROUND(AVG(biology)::numeric, 1)                  AS biology,
                                 ROUND(AVG(chemistry)::numeric, 1)                AS chemistry,
                                 ROUND(AVG(english)::numeric, 1)                  AS english
                          FROM results
                          WHERE 1 = 1
                          GROUP BY name),
    limited_student_results AS (SELECT *
                                FROM students_results
                                LIMIT 20 OFFSET (1 - 1) * 20),
     pages AS (SELECT CEIL(COUNT(*) / 20.0)
               FROM students_results)
        """

    def get_compare_results(self, params: ResultRequest):
        # Base query setup for required fields
        base_query = f"""
            WITH rates AS (
                SELECT max_point_over_all, subject
                FROM um_rate
                WHERE exam_year = %(exam_year)s
                AND exam_quarter = %(exam_quarter)s
            ),
            results AS (
                SELECT um_school.territory,
                    um_school.region,
                    um_school.id as school_id,
                    um_school.name,
                    um_student_exams.student_id,
                    CONCAT(um_student_exams.surname, '. ', LEFT(um_student_exams.name, 1), '. ',
                            LEFT(um_student_exams.patronymic, 1), '.') AS full_name,
                    studystream,
                    exam_method,
                    ROUND(average_point::numeric / (SELECT AVG(max_point_over_all) FROM rates) * 100,
                                      1)                        average,

                                COALESCE(
                                        ROUND((results -> 'math_5&6' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_5&6') *
                                              100, 1),
                                        ROUND((results -> 'math_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all FROM rates WHERE subject = 'math_7') *
                                              100, 1)
                                )                               math,
                                COALESCE(
                                        ROUND((results -> 'mother_tongue_literature_7' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_7&6') *
                                              100, 1),
                                        ROUND((results -> 'mother_tongue_literature_8&10&11' ->> 'all_point')::numeric /
                                              (SELECT max_point_over_all
                                               FROM rates
                                               WHERE subject = 'mother_tongue_literature_8&10&11') *
                                              100, 1)
                                )                               mother_tongue_literature,
                                ROUND((results -> 'literature_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'literature_5&6') * 100,
                                      1)                        literature,
                                ROUND((results -> 'mother_tongue_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'mother_tongue_5&6') * 100,
                                      1)                        mother_tongue,
                                ROUND((results -> 'russian-qaraqalpaq_5&6' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'russian-qaraqalpaq_5&6') *
                                      100,
                                      1)                        russian,
                                ROUND((results -> 'chemistry_8' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'chemistry_8') * 100,
                                      1)                        chemistry,
                                ROUND((results -> 'biology_7' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'biology_7') * 100,
                                      1)                        biology,
                                ROUND((results -> 'english_9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'english_9&10&11') * 100,
                                      1)                        english,
                                ROUND((results -> 'physics_9' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'physics_9') * 100,
                                      1)                        physics,
                                ROUND((results -> 'algebra_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'algebra_8&9&10&11') * 100,
                                      1)                        algebra,
                                ROUND((results -> 'geometry_8&9&10&11' ->> 'all_point')::numeric /
                                      (SELECT max_point_over_all FROM rates WHERE subject = 'geometry_8&9&10&11') * 100,
                                      1)                        geometry
                FROM um_student_exams
                LEFT JOIN um_school ON um_student_exams.school_id = um_school.id
                WHERE exam_quarter = %(exam_quarter)s
                AND exam_year = %(exam_year)s
            ),
        """

        # Define avg_column
        avg_column = f"AVG({params.subject})" if params.subject else "AVG(average)"
        
        # `avg_by_territory` CTE with fallback conditions
        avg_by_territory_query = f"""
            avg_by_territory AS (
                SELECT {'territory as key' if not params.territory else 'name as key' }, 
                    ROUND({avg_column}::numeric, 1) AS data
                FROM results
                WHERE {'exam_method = %(exam_method)s' if params.exam_method else '1=1'}
                AND {'studystream = %(study_class)s' if params.study_class else '1=1'}
                AND {'territory = %(territory)s' if params.territory else '1=1'}
                AND {'region = %(region)s' if params.region else '1=1'}
                GROUP BY {'territory' if not params.territory else 'school_id, name'}
                HAVING {avg_column} IS NOT NULL
                ORDER BY {avg_column}
            ),
        """

        # Define subject avg fields
        avg_fields = f"ROUND(AVG({params.subject})::numeric, 1) AS {params.subject}_avg" if params.subject else """
            ROUND(AVG(average)::numeric, 1)   AS _avg,
            ROUND(AVG(math)::numeric, 1)                     AS math_avg,
            ROUND(AVG(mother_tongue)::numeric, 1)            AS mother_tongue_avg,
            ROUND(AVG(literature)::numeric, 1)               AS literature_avg,
            ROUND(AVG(mother_tongue_literature)::numeric, 1) AS mother_tongue_literature_avg,
            ROUND(AVG(russian)::numeric, 1)                  AS russian_avg,
            ROUND(AVG(algebra)::numeric, 1)                  AS algebra_avg,
            ROUND(AVG(geometry)::numeric, 1)                 AS geometry_avg,
            ROUND(AVG(physics)::numeric, 1)                  AS physics_avg,
            ROUND(AVG(biology)::numeric, 1)                  AS biology_avg,
            ROUND(AVG(chemistry)::numeric, 1)                AS chemistry_avg,
            ROUND(AVG(english)::numeric, 1)                  AS english_avg
        """
        
        # `subject_results` CTE with fallback grouping
        subject_results_query = f"""
            subject_results AS (
                SELECT {avg_fields}
                FROM results
                WHERE {'exam_method = %(exam_method)s' if params.exam_method else '1=1'}
                AND {'studystream = %(study_class)s' if params.study_class else '1=1'}
                AND {'territory = %(territory)s' if params.territory else '1=1'}
                AND {'region = %(region)s' if params.region else '1=1'}
                AND {'name = %(school)s' if params.school else '1=1'}
            ),
        """

        subject_avg_column = f"ROUND(AVG({params.subject})::numeric, 1) AS avg" if params.subject else "ROUND(AVG(average)::numeric, 1) AS avg"

        study_class_results_query = f"""
            study_class_results AS (
                SELECT {subject_avg_column},
                    studystream as studyclass
                FROM results
                WHERE {'exam_method = %(exam_method)s' if params.exam_method else '1=1'}
                AND {'territory = %(territory)s' if params.territory else '1=1'}
                AND {'region = %(region)s' if params.region else '1=1'}
                AND {'name = %(school)s' if params.school else '1=1'}
                GROUP BY studystream
                {'HAVING AVG('+ params.subject + ') IS NOT NULL' if params.subject else ''}
                ORDER BY studystream::int
            ),
        """

        column_map = {
            "study_class": "studystream",
            "territory": "territory",
            "region": "region",
            "school": "name"  # `school` in model maps to `name` in the database
        }

        students_filter_conditions = " AND ".join(
            f"{db_column} = %({model_field})s"
            for model_field, db_column in column_map.items()
            if getattr(params, model_field) is not None
        )
        subject_condition = f"{params.subject} IS NOT NULL" if params.subject else "1=1"

        # `students_filter` and `exam_method_results` CTE
        students_filter_query = f"""
            students_filter AS (
                SELECT student_id, average, exam_method
                FROM results
                WHERE {students_filter_conditions or '1=1'} AND {subject_condition}
            ),
            exam_method_results AS (
                SELECT exam_method,
                    (SELECT COUNT(*) FROM students_filter) students_count,
                    COUNT(*) as count,
                    ROUND((COUNT(*)::numeric / (SELECT COUNT(*) FROM students_filter)::numeric) * 100, 1) AS percentage,
                    ROUND(AVG(average::numeric), 1) AS result
                FROM students_filter
                GROUP BY exam_method
            ),
        """

        # `some_subject_result` CTE
        filter_conditions = " AND ".join(
            f"{db_column} = %({model_field})s"
            for model_field, db_column in {
                "exam_method": "exam_method",
                "study_class": "studystream",
                "territory": "territory",
                "region": "region",
                "school": "name"
            }.items() if getattr(params, model_field) is not None
        )
        some_subject_result_query = f"""
            some_subject_result AS (
                SELECT ROUND(AVG(average)::numeric, 1)   AS _avg,
                    ROUND(AVG(math)::numeric, 1)                     AS math_avg,
                    ROUND(AVG(mother_tongue)::numeric, 1)            AS mother_tongue_avg,
                    ROUND(AVG(literature)::numeric, 1)               AS literature_avg,
                    ROUND(AVG(mother_tongue_literature)::numeric, 1) AS mother_tongue_literature_avg,
                    ROUND(AVG(russian)::numeric, 1)                  AS russian_avg,
                    ROUND(AVG(algebra)::numeric, 1)                  AS algebra_avg,
                    ROUND(AVG(geometry)::numeric, 1)                 AS geometry_avg,
                    ROUND(AVG(physics)::numeric, 1)                  AS physics_avg,
                    ROUND(AVG(biology)::numeric, 1)                  AS biology_avg,
                    ROUND(AVG(chemistry)::numeric, 1)                AS chemistry_avg,
                    ROUND(AVG(english)::numeric, 1)                  AS english_avg
                FROM results
                WHERE {filter_conditions or '1=1'}
            )
        """

        # Final query
        query = base_query + avg_by_territory_query + subject_results_query + study_class_results_query + students_filter_query + some_subject_result_query + """
            SELECT json_build_object(
                'avg_by_territory', (SELECT json_agg(avg_by_territory) FROM avg_by_territory),
                'subject_results', (SELECT json_agg(subject_results) FROM subject_results),
                'study_class_results', (SELECT json_agg(study_class_results) FROM study_class_results),
                'some_subject_result', (SELECT json_agg(some_subject_result) FROM some_subject_result),
                'exam_method_results', (SELECT json_agg(exam_method_results) FROM exam_method_results)
            ) AS results;
        """

        # Execute the query
        with self.conn:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, {
                    "exam_year": params.exam_year,
                    "exam_quarter": params.exam_quarter,
                    "territory": params.territory,
                    "school": params.school,
                    "exam_method": params.exam_method,
                    "study_class": params.study_class,
                    "region": params.region,
                    "subject": params.subject
                })
                result = cursor.fetchone()
            
        return result['results']

    def get_available_territories_classes(self):
        query = """
            WITH all_territories AS (SELECT DISTINCT territory as territories
                                    FROM um_school),
                all_classes AS (SELECT DISTINCT CAST(studystream as INT) as classes
                                FROM um_student_exams
                                ORDER BY CAST(studystream as INT)) 
            SELECT JSON_BUILD_OBJECT('all_territories', (SELECT JSON_AGG(territories) FROM all_territories),
                'all_classes', (SELECT JSON_AGG(classes) FROM all_classes)) AS result;
            """     
        with self.conn:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                result = cursor.fetchone()


        return result['result']