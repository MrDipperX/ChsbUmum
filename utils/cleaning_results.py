from .const import all_subjects

def clean_subjects(data):
    if not data:
        return []
    # Identify subjects to remove
    subjects_to_remove = set()
    if data:
        # Initialize with keys from the first element
        subjects_to_remove = set(data[0].keys())
        
        for entry in data:
            subjects_to_remove.intersection_update(
                {key for key, value in entry.items() if value is None}
            )

    # Remove identified subjects from each element
    for entry in data:
        for subject in subjects_to_remove:
            entry.pop(subject, None)
    
    return data

def clean_results_data(results_info):
    # Remove empty strings from the data
    students_results = results_info['students_results'] if results_info['students_results'] is None else clean_subjects(results_info['students_results'])

    some_subject_result = {}
    if results_info['some_subject_result'] is not None and len(results_info['some_subject_result'][0]) > 0:
        some_subject_result = { key:val for key, val in results_info['some_subject_result'][0].items() if val is not None }

    subject_results = results_info['subject_results']
    subject_results_keys = [i+"_avg" for i in all_subjects.keys()]
    if subject_results is not None and len(subject_results) > 0:
        subject_results = [{k: v for k, v in item.items() if v is not None} for item in subject_results]

    return (students_results, some_subject_result, subject_results, subject_results_keys)

def clean_compare_data(results_info):
    some_subject_result = {}
    if results_info['some_subject_result'] is not None and len(results_info['some_subject_result'][0]) > 0:
        some_subject_result = { key:val for key, val in results_info['some_subject_result'][0].items() if val is not None }

    subject_results = results_info['subject_results']
    subject_results_keys = [i+"_avg" for i in all_subjects.keys()]
    if subject_results is not None and len(subject_results) > 0:
        subject_results = [{k: v for k, v in item.items() if v is not None} for item in subject_results]

    return (some_subject_result, subject_results, subject_results_keys)