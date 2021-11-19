import csv
import pandas as pd
import numpy as np
import json
import os
from flask import Flask
from flask_restful import Resource, Api, reqparse
import ast

#check tests.csv file.
def check_tests(tests_csv):
    result = "pass"
    df = pd.read_csv(tests_csv)
    if df.isnull().values.any():
        result = "null"
    groupDf = [v for k, v in df.groupby("course_id")]
    if df["id"].min() < 0:
        result = "id"
    for i in groupDf:
        if i["weight"].sum() != 100:
            result = "weight"

    return result

#check students.csv file.
def check_students(students_csv):
    result = "pass"
    df = pd.read_csv(students_csv)
    if df.isnull().values.any():
        result = "null"
    if df["id"].min() < 0:
        result = "id"
    return result



#check courses.csv file.

def check_courses(courses_csv):
    result = "pass"
    df = pd.read_csv(courses_csv)
    if df.isnull().values.any():
        result = "null"
    if df["id"].min() < 0:
        result = "id"
    return result

#check marks.csv file.

def check_marks(marks_csv, students_csv, tests_csv):
    result = "pass"
    marks_df = pd.read_csv(marks_csv)
    students_df = pd.read_csv(students_csv)
    tests_df = pd.read_csv(tests_csv)
    if marks_df.isnull().values.any():
        result = "null"
    for i in marks_df["test_id"].unique():
        if i not in tests_df["id"].unique():
            result = "notInTest"
    for i in marks_df["student_id"].unique():
        if i not in students_df["id"].unique():
            result = "notInStu"
    return result




# [{'id': 1, 'name': 'A', 'totalAverage': None, 'courses': []},
# {'id': 2, 'name': 'B', 'totalAverage': None, 'courses': []},
# {'id': 3, 'name': 'C', 'totalAverage': None, 'courses': []}]
#Construct a list of dictionnary for every student from students.csv.
def construct_json_value(student_csv):
    students = []
    for row in student_csv:
        temp_student_index = row["id"]
        row["id"] = int(temp_student_index)
        students.append(row)

    for i in students:
        i["totalAverage"] = None
        i["courses"] = []
    return students


# [   student_id  test_id  mark  course_id  weight     name  teacher
# 0           1        1    78          1      10  Biology    Mr. D
# 1           1        7    40          3      10     Math   Mrs. C
# 2           1        6    78          3      90     Math   Mrs. C
# 3           1        5    65          2      60  History   Mrs. P
# 4           1        3    95          1      50  Biology    Mr. D
# 5           1        4    32          2      40  History   Mrs. P
# 6           1        2    87          1      40  Biology    Mr. D,
#      student_id  test_id  mark  course_id  weight     name  teacher
# 7            2        2    87          1      40  Biology    Mr. D
# 8            2        3    15          1      50  Biology    Mr. D
# 9            2        7    40          3      10     Math   Mrs. C
# 10           2        6    78          3      90     Math   Mrs. C
# 11           2        1    78          1      10  Biology    Mr. D,
#      student_id  test_id  mark  course_id  weight     name  teacher
# 12           3        3    95          1      50  Biology    Mr. D
# 13           3        4    32          2      40  History   Mrs. P
# 14           3        5    65          2      60  History   Mrs. P
# 15           3        1    78          1      10  Biology    Mr. D
# 16           3        6    78          3      90     Math   Mrs. C
# 17           3        2    87          1      40  Biology    Mr. D
# 18           3        7    40          3      10     Math   Mrs. C]
#Construct course list for each student, including all the courses,tests and marks. 
def construct_individual_courselist(marks_csv, tests_csv, courses_csv):
    marks_df = pd.read_csv(marks_csv)
    tests_df = pd.read_csv(tests_csv)
    courses_df = pd.read_csv(courses_csv)
    tests_df = tests_df.rename(columns={"id": "test_id"}) #rename "id" column to avoid conflict.
    courses_df = courses_df.rename(columns={"id": "course_id"})  #rename "id" column to avoid conflict.
    df = pd.merge(marks_df, tests_df, on="test_id") 
    df = pd.merge(df, courses_df, on="course_id") #merge three table together on test_id and course_id
    student_id_col = df.pop("student_id") #drop student_id column to move it forward.
    df.insert(0, "student_id", student_id_col)
    df = df.sort_values(["student_id"]).reset_index(drop=True)
    student_record_list = [v for k, v in df.groupby("student_id")] #get a list of pd series(grouped by student ID.) to calculate weighted marks later.
    return student_record_list

#weighted marks calculator
def w_avg(df, values, weights):
    d = df[values]
    w = df[weights]
    return (d * w).sum() / w.sum()


#    id     name  teacher  courseAverage  student_id
# 0   1  Biology    Mr. D           90.1           1
# 1   2  History   Mrs. P           51.8           1
# 2   3     Math   Mrs. C           74.2           1
#    id     name  teacher  courseAverage  student_id
# 0   1  Biology    Mr. D           50.1           2
# 1   3     Math   Mrs. C           74.2           2
#    id     name  teacher  courseAverage  student_id
# 0   1  Biology    Mr. D           90.1           3
# 1   2  History   Mrs. P           51.8           3
# 2   3     Math   Mrs. C           74.2           3
#create courses&tests summary for each student.
def create_course_record(student_record_df):
    sorted_by_course = student_record_df.sort_values(["course_id"]).reset_index(
        drop=True
    ) #sort student record dataframe by course_id for a clean order in the result.
    df = sorted_by_course.groupby("course_id").apply(w_avg, "mark", "weight") #calculate weighted marks for each course.
    df = pd.merge(df.to_frame(), sorted_by_course, on="course_id") #merge calculated result with student record df on course_id
    essential_info = (
        df.drop(["mark", "weight", "test_id"], axis=1)
        .drop_duplicates()
        .reset_index(drop=True)
    ) #drop columns that are not required in the final result.
    teacher_name_col = essential_info.pop("teacher") #move "teacher" and "name" column to the front for the correct order in the result.
    essential_info.insert(0, "teacher", teacher_name_col)
    course_name_col = essential_info.pop("name")
    essential_info.insert(0, "name", course_name_col)
    essential_info = essential_info.rename(columns={0: "courseAverage"}) #rename some columns names to necessary info.
    essential_info = essential_info.rename(columns={"course_id": "id"})
    course_id_col = essential_info.pop("id")
    essential_info.insert(0, "id", course_id_col)
    return essential_info




#create a JSON file with all the provided files and requirements.
def dump_json_api(
    students_filename, marks_filename, tests_filename, courses_filename
):
    #construct entire list for all students.
    student_csv = csv.DictReader(open(students_filename))
    whole_list = construct_json_value(student_csv)
    
    #construct individual course list for each student
    individual_course_List = construct_individual_courselist(
        marks_filename, tests_filename, courses_filename
    )

    #create dictionary to map student ID with courses they took so we do not get O(N^2) time complexity.
    #Trade off between time and space, we choose time here.
    studen_courses_dict = {}

    for individual_course in individual_course_List:
        df = create_course_record(individual_course)
        studen_courses_dict[(df["student_id"].unique()[0])] = df.drop(
            ["student_id"], axis=1
        ) #drop the student ID column in the course record since we already it.

    #calculate course records for every student and calcuate total average grade.
    for student in whole_list:
        if int(student["id"]) in studen_courses_dict:
            jsdf = studen_courses_dict[int(student["id"])].to_json(orient="records")
            student["courses"] = json.loads(jsdf)
            student["totalAverage"] = round(
                (studen_courses_dict[int(student["id"])]["courseAverage"]).mean(), 2
            )

    data = {}
    data["students"] = whole_list

    return data


#check each CSV files before create the JSON file.
#if CSV files have invalid values, we will simply return a JSON file with error message.
def check_run_api(
    students_filename, marks_filename, tests_filename, courses_filename
):
    result = ""
    p = "pass"
    if (
        check_courses(courses_filename) == p
        and check_students(students_filename) == p
        and check_tests(tests_filename) == p
        and check_marks(marks_filename, students_filename, tests_filename) == p
    ):
        print("all check on JSON files passed.")
        return dump_json_api(
            students_filename,
            marks_filename,
            tests_filename,
            courses_filename,
        )
    else:
        t = check_tests(tests_filename)
        c = check_courses(courses_filename)
        s = check_students(students_filename)
        m = check_marks(marks_filename, students_filename, tests_filename)
        # print(t,c,s,m)
        if m == "null":
            result = "marks CSV file contains null values"
        if m == "notInTest":
            result = "marksCSV error, Cannot find test ID in test CSV."
        if m == "notInStu":
            result = "marksCSV error, Cannot find student ID in students CSV."

        if t == "null":
            result = "tests CSV file contains null values"
        if t == "id":
            result = "tests CSV file contains invalid ids"
        if t == "weight":
            result = "Invalid test weights"

        if c == "null":
            result = "courses CSV file contains null values"
        if c == "id":
            result = "courses CSV file contains invalid ids"

        if s == "null":
            result = "students CSV file contains null values"
        if s == "id":
            result = "students CSV file contains invalid ids"

    if result:
        d = {}
        d["error"] = result
        return d


def dump_json(
    students_filename, marks_filename, tests_filename, courses_filename, output_path
):

    student_csv = csv.DictReader(open(students_filename))
    whole_list = construct_json_value(student_csv)

    individual_course_List = construct_individual_courselist(
        marks_filename, tests_filename, courses_filename
    )
    studen_courses_dict = {}

    for individual_course in individual_course_List:
        df = create_course_record(individual_course)
        studen_courses_dict[(df["student_id"].unique()[0])] = df.drop(
            ["student_id"], axis=1
        )

    for student in whole_list:
        jsdf = studen_courses_dict[int(student["id"])].to_json(orient="records")
        student["courses"] = json.loads(jsdf)
        student["totalAverage"] = round(
            (studen_courses_dict[int(student["id"])]["courseAverage"]).mean(), 2
        )

    data = {}
    data["students"] = whole_list

    filename = "./" + output_path + "/result.json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as fp:
        json.dump(data, fp, indent=2)


def check_run(
    students_filename, marks_filename, tests_filename, courses_filename, output_path
):
    result = ""
    p = "pass"
    if (
        check_courses(courses_filename) == p
        and check_students(students_filename) == p
        and check_tests(tests_filename) == p
        and check_marks(marks_filename, students_filename, tests_filename) == p
    ):
        print("all check on JSON files passed.")
        dump_json(
            students_filename,
            marks_filename,
            tests_filename,
            courses_filename,
            output_path,
        )
        print("Result JSON file has been created. Please check output directory.")
    else:
        t = check_tests(tests_filename)
        c = check_courses(courses_filename)
        s = check_students(students_filename)
        m = check_marks(marks_filename, students_filename, tests_filename)
        # print(t,c,s,m)
        if m == "null":
            result = "marks CSV file contains null values"
        if m == "notInTest":
            result = "marksCSV error, Cannot find test ID in test CSV."
        if m == "notInStu":
            result = "marksCSV error, Cannot find student ID in students CSV."

        if t == "null":
            result = "tests CSV file contains null values"
        if t == "id":
            result = "tests CSV file contains invalid ids"
        if t == "weight":
            result = "Invalid test weights"

        if c == "null":
            result = "courses CSV file contains null values"
        if c == "id":
            result = "courses CSV file contains invalid ids"

        if s == "null":
            result = "students CSV file contains null values"
        if s == "id":
            result = "students CSV file contains invalid ids"

    if result:
        d = {}
        d["error"] = result
        filename = "./" + output_path + "/result.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as fp:
            json.dump(d, fp, indent=2)