import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import insert
from config.config import DB_URL
import hashlib
import psycopg2
import json

subjects_umum = {
    "Matematika  5-6 sinf": "math_5&6", 
    "Ona tili 5-6 sinf": "mother_tongue_5&6", 
    "Adabiyot 5-6 sinf": "literature_5&6", 
    "Русский язык и литература 5-6 sinf\nQaraqalpaq tili ve edebiyati 5-6 sinf": "russian-qaraqalpaq_5&6", 
    "Matematika 7 sinf":"math_7", 
    "\n\nРусский язык и литература (7 -sinf)\nOna tili va adabiyot (7 -sinf)\nQaraqalpaq tili ve edebiyati  (7 -sinf)\n": "mother_tongue_literature_7", 
    "Biologiya (7-sinf)": "biology_7",
    "Algebra(8-9-10-11)" : "algebra_8&9&10&11",
    "Geometriya(8-9-10-11 -sinf)" : "geometry_8&9&10&11",
    "\nOna tili va adabiyot (8-10-11 -sinf)\nУзбекский язык (O'zbek tili) (8-10-11 -sinf)\nQaraqalpaq tili ve edebiyati (8-10-11 -sinf)\n"  : "mother_tongue_literature_8&10&11",
    " Kimyo (8-sinf)\n": "chemistry_8",
    "Fizika (9-sinf)": "physics_9",
    "Ingliz tili (9-10-11 -sinf)": "english_9&10&11"
}

def extract_numbers(text):
    return [float(s.replace(",", ".")) for s in text.split() if s.replace(",", ".").replace(".", "").isdigit()]

def transform_to_results_column_v2(df, subjects):
    # Create an empty list to store results dictionaries for each row
    results_list = []

    for _, row in df.iterrows():
        subject_results = {}  # Dictionary to hold scores for each subject
        for subject, prefix in subjects.items():
            # Attempt to retrieve knowing, applying, reviewing, and all points columns
            try:
                knowing_data = row[subject]
                applying_data = row[f'Unnamed: {df.columns.get_loc(subject) + 1}']
                reviewing_data = row[f'Unnamed: {df.columns.get_loc(subject) + 2}']
                all_data = row[f'Unnamed: {df.columns.get_loc(subject) + 3}']
                # Identify the columns for knowing, applying, reviewing, and all points

                knowing = round(float(knowing_data) if knowing_data != "NaN" and pd.notna(knowing_data) and knowing_data != '' else None, 1)
                applying = round(float(applying_data) if applying_data != "NaN" and pd.notna(applying_data) and applying_data != '' else None, 1)
                reviewing = round(float(reviewing_data) if reviewing_data != "NaN" and pd.notna(reviewing_data) and reviewing_data != '' else None, 1)
                all_point = round(float(all_data) if all_data != "NaN" and pd.notna(all_data) and all_data != '' else None, 1)

                # Create dictionary for the subject
                subject_results[prefix] = {
                    "knowing_point": knowing,
                    "applying_point": applying,
                    "reviewing_point": reviewing,
                    "all_point": all_point
                }
            except Exception as e:
                continue

        # Add the subject results dictionary to the list for the row
        results_list.append(subject_results)

    # Add the results column to the dataframe
    df['results'] = results_list

    return df

def update_first_row(df, subjects):
    for col in df.columns:
        for subject in subjects:
            if subjects[subject] in col:
                # Updating the first row with simple names based on column names
                if "_knowing_point" in col:
                    df.at[0, col] = f"{subjects[subject]}_knowing_point"
                elif "_applying_point" in col:
                    df.at[0, col] = f"{subjects[subject]}_applying_point"
                elif "_reviewing_point" in col:
                    df.at[0, col] = f"{subjects[subject]}_reviewing_point"
                elif "_all_point" in col:
                    df.at[0, col] = f"{subjects[subject]}_all_point"
    return df

def insert_on_conflict_do_nothing(table, conn, keys, data_iter):
    # Build insert statement with ON CONFLICT DO NOTHING
    insert_stmt = insert(table.table).values([dict(zip(keys, row)) for row in data_iter])
    insert_stmt = insert_stmt.on_conflict_do_nothing()
    conn.execute(insert_stmt)

def calculate_average_points(results):
    try:
        # Convert the input string to a valid JSON format
        results = results.replace("NaN", "null").replace("'", "\"")
        result_data = json.loads(results)  # Parse the JSON

        # Calculate average of all "all_point" values
        total_points = 0
        count = 0

        for subject, subject_data in result_data.items():
            all_point = subject_data.get('all_point')
            if all_point is not None:
                total_points += all_point
                count += 1

        if count > 0:
            return round(total_points / count, 1)
        else:
            return None  # No valid points to average
    except Exception as e:
        print(f"Error: {e}")  # Print the error for debugging
        return None  # Handle any parsing or calculation errors

def calculate_results_by_school(df):
    # List to store results
    results_list = []
    
    # Group by school_id
    for school_id, group in df.groupby('school_id'):
        school_results = {}
        average_point = {}

        # Process "on" and "off" exam methods
        for exam_method in ['on', 'off']:
            class_averages = {}
            method_group = group[group['exam_method'] == exam_method]

            # Only process if there is data for this exam_method
            if not method_group.empty:
                exam_method_results = {}
                subject_totals_schoolwide = {}
                subject_counts_schoolwide = {}

                # Group by studyclass within each school_id and exam_method group
                for studyclass, class_group in method_group.groupby('studystream'):
                    subject_totals = {}
                    subject_counts = {}

                    # Initialize subject totals and counts
                    for _, row in class_group.iterrows():
                        results = json.loads(row['results'])

                        for subject, data in results.items():
                            for point_type in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']:
                                point_value = data.get(point_type)
                                if point_value is not None:
                                    if subject not in subject_totals:
                                        subject_totals[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                                        subject_counts[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                                    
                                    # Accumulate values for the current study class
                                    subject_totals[subject][point_type] += point_value
                                    subject_counts[subject][point_type] += 1

                                    # Accumulate values for the whole school
                                    if subject not in subject_totals_schoolwide:
                                        subject_totals_schoolwide[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                                        subject_counts_schoolwide[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                                    subject_totals_schoolwide[subject][point_type] += point_value
                                    subject_counts_schoolwide[subject][point_type] += 1

                    # Calculate rounded averages for each point type in each subject
                    studyclass_averages = {
                        subject: {
                            point_type: round(subject_totals[subject][point_type] / subject_counts[subject][point_type], 2) 
                            if subject_counts[subject][point_type] > 0 else None
                            for point_type in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']
                        } for subject in subject_totals
                    }

                    # Calculate the rounded average "all_point" for this studyclass
                    studyclass_average_point = round(np.nanmean([
                        subject_data['all_point']
                        for subject_data in studyclass_averages.values()
                        if 'all_point' in subject_data
                    ]), 2) if studyclass_averages else None

                    # Add to results if not empty
                    if studyclass_averages:
                        exam_method_results[studyclass] = studyclass_averages
                    if studyclass_average_point is not None:
                        class_averages[studyclass] = studyclass_average_point

                # Calculate the overall average for this exam method
                overall_average_point = round(np.nanmean(list(class_averages.values())), 2) if class_averages else None
                if overall_average_point is not None:
                    class_averages['overall'] = overall_average_point

                # Calculate school-wide averages for this exam method
                school_avg = {
                    subject: {
                        point_type: round(subject_totals_schoolwide[subject][point_type] / subject_counts_schoolwide[subject][point_type], 5)
                        if subject_counts_schoolwide[subject][point_type] > 0 else None
                        for point_type in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']
                    } for subject in subject_totals_schoolwide
                }

                # Only add non-empty results and averages to final output
                if exam_method_results:
                    school_results[exam_method] = {**exam_method_results, 'school_avg': school_avg}
                if class_averages:
                    average_point[exam_method] = class_averages

        # Calculate "all" based on the original logic (without combining "on" and "off" exam methods)
        school_results_all = {}
        subject_totals_schoolwide_all = {}
        subject_counts_schoolwide_all = {}

        # Group by studyclass within each school_id group
        for studyclass, class_group in group.groupby('studystream'):
            subject_totals = {}
            subject_counts = {}

            for _, row in class_group.iterrows():
                results = json.loads(row['results'])

                for subject, data in results.items():
                    for point_type in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']:
                        point_value = data.get(point_type)
                        if point_value is not None:
                            if subject not in subject_totals:
                                subject_totals[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                                subject_counts[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                            
                            # Accumulate values for the current study class
                            subject_totals[subject][point_type] += point_value
                            subject_counts[subject][point_type] += 1
                            
                            # Accumulate values for the whole school
                            if subject not in subject_totals_schoolwide_all:
                                subject_totals_schoolwide_all[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                                subject_counts_schoolwide_all[subject] = {pt: 0 for pt in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']}
                            subject_totals_schoolwide_all[subject][point_type] += point_value
                            subject_counts_schoolwide_all[subject][point_type] += 1

            studyclass_averages = {
                subject: {
                    point_type: round(subject_totals[subject][point_type] / subject_counts[subject][point_type], 2) 
                    if subject_counts[subject][point_type] > 0 else None
                    for point_type in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']
                } for subject in subject_totals
            }

            # Calculate the overall average for "all_point" in this study class
            studyclass_average_point = np.nanmean([
                subject_data['all_point']
                for subject_data in studyclass_averages.values()
                if 'all_point' in subject_data
            ])
            class_averages[studyclass] = studyclass_average_point

            # Add the studyclass results to school_results
            school_results_all[studyclass] = studyclass_averages

        # Calculate school-wide averages across all exam methods
        school_avg_all = {
            subject: {
                point_type: round(subject_totals_schoolwide_all[subject][point_type] / subject_counts_schoolwide_all[subject][point_type], 5)
                if subject_counts_schoolwide_all[subject][point_type] > 0 else None
                for point_type in ['knowing_point', 'applying_point', 'reviewing_point', 'all_point']
            } for subject in subject_totals_schoolwide_all
        }

        # Calculate the overall average for the school based on 'all_point' only, using the school-wide data
        overall_average_point = round(np.nanmean([
            subject_data['all_point']
            for subject_data in school_avg_all.values()
            if 'all_point' in subject_data
        ]), 5)
        
        # Add the overall school average to class_averages under 'overall'
        class_averages['overall'] = overall_average_point
        school_results_all = {**school_results_all, 'school_avg': school_avg_all}
        school_results['all'] = school_results_all
        average_point['all'] = class_averages

        # Append the final results to the list
        results_list.append({
            'school_id': school_id,
            'results': school_results,
            'average_point': average_point
        })

    # Create a new DataFrame from the results
    results_df = pd.DataFrame(results_list)

    return results_df

def insert_data_to_tables(filename, quarter, year):
    try:
        df = pd.read_excel(filename)

        # Create SQLAlchemy engine to connect to PostgreSQL
        engine = create_engine(DB_URL)


        # SCHOOL TABLE INSERTION
        # ------------------------------------------------------------------------------------------------
        school_df = df.copy()
        school_df = school_df[1:]

        school_df = school_df[['SchoolId', 'Hudud', 'Tuman/Shahar', 'Maktab']]

        # Rename for consistency
        school_df.columns = ['id', 'territory', 'region', 'name']

        # Drop duplicates to ensure unique combinations
        school_df['territory'] = school_df['territory'].str.strip()
        school_df['name'] = school_df['name'].str.strip()
        school_df['region'] = school_df['region'].str.strip()
        school_df['id'] = school_df['id'].astype(int)
        school_df = school_df.drop_duplicates()

        school_df.to_sql('um_school', engine, if_exists='append', index=False, method=insert_on_conflict_do_nothing)

        # ------------------------------------------------------------------------------------------------

        # RATE TABLE INSERTION
        # ------------------------------------------------------------------------------------------------
        corrected_result_dict = {}

        for subject in subjects_umum.keys():
            if subject in df.columns:
                # Find the index of the subject column
                subject_index = df.columns.get_loc(subject)
                # Extract only the first row for the subject and the next 3 columns
                lil_df = df.iloc[0, subject_index:subject_index + 4]
                # Convert the extracted row into a list and store in the dictionary
                corrected_result_dict[subject] = lil_df.dropna().astype(str).tolist()

        transformed_data = []

        for subject, data in corrected_result_dict.items():

            knowing = extract_numbers(data[0])
            applying = extract_numbers(data[1])
            reviewing = extract_numbers(data[2])
            overall = extract_numbers(data[3])

            transformed_data.append({
                "knowing_question_count": int(knowing[0]),
                "knowing_point_per_question": knowing[1],
                "applying_question_count": int(applying[0]),
                "applying_point_per_question": applying[1],
                "reviewing_question_count": int(reviewing[0]),
                "reviewing_point_per_question": reviewing[1],
                "all_question_count": int(overall[0]),
                "max_point_over_all": overall[1],
                "subject": subjects_umum[subject]
            })

        rate_df = pd.DataFrame(transformed_data)
        rate_df['exam_year'] = year
        rate_df['exam_quarter'] = quarter

        rate_df.to_sql('um_rate', engine, if_exists='append', index=False, method=insert_on_conflict_do_nothing)


        # ------------------------------------------------------------------------------------------------

        # STUDENT_EXAMS TABLE INSERTION
        # ------------------------------------------------------------------------------------------------

        exams_df = df.copy()
        exams_df = transform_to_results_column_v2(exams_df, subjects_umum)

        exams_df.loc[0, 'results'] = 'results'
        exams_df = exams_df[1:]

        exams_df = exams_df.rename(columns={
            'Familya': 'surname',
            'Ism': 'name',
            "Otasining ismi": "patronymic",
            "Guruh": "studyclass",
            "Sinf": "studystream",
            "Ta'lim tili": "studylang",
            "Javoblar \nvarag'i ID \nraqami": "id",
            "user ID": "student_id",
            "SchoolId": "school_id",
        # Add more mappings for subjects and other columns if required
        })

        exams_df.loc[exams_df['student_id'].isna(), 'student_id'] = exams_df.loc[exams_df['student_id'].isna(), 'person ID']

        exams_df['name'] = exams_df['name'].str.strip()
        exams_df['surname'] = exams_df['surname'].str.strip()
        exams_df['patronymic'] = exams_df['patronymic'].str.strip()
        exams_df['id'] = exams_df['id'].astype(int)
        exams_df['student_id'] = exams_df['student_id'].astype(int)
        exams_df['school_id'] = exams_df['school_id'].astype(int)
        exams_df['studystream'] = exams_df['studystream'].astype(int)

        exams_df = exams_df[['id', 'student_id', 'surname', 'name', 'patronymic', 'studyclass', 'studystream', 'studylang', 'results', 'school_id']]

        exams_df['results'] = exams_df['results'].apply(json.dumps)
        exams_df['average_point'] = exams_df['results'].apply(calculate_average_points)

        exams_df['exam_year'] = year
        exams_df['exam_quarter'] = quarter
        exams_df['exam_method'] = 'off'

        exams_df.to_sql('um_student_exams', engine, if_exists='append', index=False, method=insert_on_conflict_do_nothing)

        # ------------------------------------------------------------------------------------------------


        # SCHOOL_RESULTS TABLE INSERTION
        # ------------------------------------------------------------------------------------------------
        results_df = calculate_results_by_school(exams_df)
        results_df['exam_year'] = year
        results_df['exam_quarter'] = quarter
        results_df['results'] = results_df['results'].apply(json.dumps)
        results_df['average_point'] = results_df['average_point'].apply(json.dumps)

        results_df.to_sql('um_school_results', engine, if_exists='append', index=False, method=insert_on_conflict_do_nothing)
    except Exception as e:
        print(e)

def inserting_teachers(filename, year):
    # try:

        engine = create_engine(DB_URL)

        teachers_df = pd.read_excel(filename)

        teachers_df.drop(columns=['T/r'], inplace=True)

        teachers_df.columns = teachers_df.columns.where(teachers_df.columns.notna(), teachers_df.iloc[0])
        teachers_df = teachers_df[1:]
        teachers_df = teachers_df.loc[:, ~teachers_df.columns.isna()]
        teachers_df.columns = ['territory', 'school', 'teachers_count', 'women_teachers_count', 'women_teachers_percentage', 'special_teachers_count', 'special_teachers_percentage', 
                            'second_category_teachers_count', 'second_category_teachers_percentage', 'first_category_teachers_count', 'first_category_teachers_percentage',
                                'highest_category_teachers_count', 'highest_category_teachers_percentage']
        teachers_df['men_teachers_count'] = teachers_df['teachers_count'] - teachers_df['women_teachers_count']
        teachers_df['men_teachers_percentage'] = 100 - teachers_df['women_teachers_percentage']
        teachers_df['year'] = year

        change_types_columns = ['women_teachers_percentage', 'men_teachers_percentage', 'special_teachers_percentage', 
                            'second_category_teachers_percentage', 'first_category_teachers_percentage', 'highest_category_teachers_percentage']

        for column in change_types_columns:
            teachers_df = teachers_df.astype({column: 'float64'})


        teachers_df = teachers_df.round(2)

                # Select columns to sum
        numeric_columns = [
            'teachers_count',
            'women_teachers_count',
            'special_teachers_count',
            'second_category_teachers_count',
            'first_category_teachers_count',
            'highest_category_teachers_count',
            'men_teachers_count',
        ]

        # Calculate sums for the numeric columns
        summed_values = teachers_df[numeric_columns].sum()

        # Calculate percentages based on summed values
        women_teachers_percentage = (summed_values['women_teachers_count'] / summed_values['teachers_count']) * 100
        special_teachers_percentage = (summed_values['special_teachers_count'] / summed_values['teachers_count']) * 100
        second_category_teachers_percentage = (summed_values['second_category_teachers_count'] / summed_values['teachers_count']) * 100
        first_category_teachers_percentage = (summed_values['first_category_teachers_count'] / summed_values['teachers_count']) * 100
        highest_category_teachers_percentage = (summed_values['highest_category_teachers_count'] / summed_values['teachers_count']) * 100
        men_teachers_percentage = (summed_values['men_teachers_count'] / summed_values['teachers_count']) * 100

        # Create a new row as a dictionary
        new_row = {
            'territory': 'Barcha hududlar',
            'school': 'Barcha hududlar',
            'teachers_count': summed_values['teachers_count'],
            'women_teachers_count': summed_values['women_teachers_count'],
            'women_teachers_percentage': round(women_teachers_percentage, 2),
            'special_teachers_count': summed_values['special_teachers_count'],
            'special_teachers_percentage': round(special_teachers_percentage, 2),
            'second_category_teachers_count': summed_values['second_category_teachers_count'],
            'second_category_teachers_percentage': round(second_category_teachers_percentage, 2),
            'first_category_teachers_count': summed_values['first_category_teachers_count'],
            'first_category_teachers_percentage': round(first_category_teachers_percentage, 2),
            'highest_category_teachers_count': summed_values['highest_category_teachers_count'],
            'highest_category_teachers_percentage': round(highest_category_teachers_percentage, 2),
            'men_teachers_count': summed_values['men_teachers_count'],
            'men_teachers_percentage': round(men_teachers_percentage, 2),
            'year': '2024/2025',  # Assuming the year is constant
        }

        # Append the new row to the dataframe
        teachers_df = pd.concat([teachers_df, pd.DataFrame([new_row])], ignore_index=True)
        teachers_df['school'] = teachers_df['school'].str.replace('\u00A0', ' ')

        teachers_df['teachers_hash'] = teachers_df.apply(generate_hash_teachers, axis=1)

        teachers_df.to_sql('teachers', engine, if_exists='append', index=False, method=insert_on_conflict_do_nothing)
    # except Exception as e:
    #     print(e)
