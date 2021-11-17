import csv
import pandas as pd
import numpy as np
import json
import os
from flask import Flask
from flask_restful import Resource, Api, reqparse
import ast

def checkTests(testsCSV):
    result = "pass"
    df = pd.read_csv(testsCSV)
    if df.isnull().values.any():
        result = "null"
    group = df.groupby("course_id")
    groupDf = [v for k, v in df.groupby("course_id")]
    if df["id"].min() < 0:
        result = "id"
    for i in groupDf:
        if i["weight"].sum() != 100:
            result = "weight"

    return result


def checkStudents(studentsCSV):
    result = "pass"
    df = pd.read_csv(studentsCSV)
    if df.isnull().values.any():
        result = "null"
    if df["id"].min() < 0:
        result = "id"
    return result


# print(checkStudents('students.csv'))


def checkCourses(coursesCSV):
    result = "pass"
    df = pd.read_csv(coursesCSV)
    if df.isnull().values.any():
        result = "null"
    if df["id"].min() < 0:
        result = "id"
    return result


def checkMarks(marksCSV, studentsCSV, testsCSV):
    result = "pass"
    marksDf = pd.read_csv(marksCSV)
    studentsDf = pd.read_csv(studentsCSV)
    testsDf = pd.read_csv(testsCSV)
    if marksDf.isnull().values.any():
        result = "null"
    for i in marksDf["test_id"].unique():
        if i not in testsDf["id"].unique():
            result = "notInTest"
    for i in marksDf["student_id"].unique():
        if i not in studentsDf["id"].unique():
            result = "notInStu"
    return result


# print(checkMarks('marks.csv','students.csv','tests.csv'))


# [{'id': 1, 'name': 'A', 'totalAverage': None, 'courses': []},
# {'id': 2, 'name': 'B', 'totalAverage': None, 'courses': []},
# {'id': 3, 'name': 'C', 'totalAverage': None, 'courses': []}]
def constructJsonValue(studentCSV):
    students = []
    for row in studentCSV:
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
def constructIndividualCourseList(marksCSV, testsCSV, coursesCSV):
    marks_df = pd.read_csv(marksCSV)
    tests_df = pd.read_csv(testsCSV)
    courses_df = pd.read_csv(coursesCSV)
    tests_df = tests_df.rename(columns={"id": "test_id"})
    courses_df = courses_df.rename(columns={"id": "course_id"})
    df = pd.merge(marks_df, tests_df, on="test_id")
    df = pd.merge(df, courses_df, on="course_id")
    student_id_col = df.pop("student_id")
    df.insert(0, "student_id", student_id_col)
    df = df.sort_values(["student_id"]).reset_index(drop=True)
    student_record_list = [v for k, v in df.groupby("student_id")]
    return student_record_list


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
def createCourseRecord(student_record_df):
    sorted_by_course = student_record_df.sort_values(["course_id"]).reset_index(
        drop=True
    )
    df = sorted_by_course.groupby("course_id").apply(w_avg, "mark", "weight")
    df = pd.merge(df.to_frame(), sorted_by_course, on="course_id")
    essential_info = (
        df.drop(["mark", "weight", "test_id"], axis=1)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    teacher_name_col = essential_info.pop("teacher")
    essential_info.insert(0, "teacher", teacher_name_col)
    course_name_col = essential_info.pop("name")
    essential_info.insert(0, "name", course_name_col)
    essential_info = essential_info.rename(columns={0: "courseAverage"})
    essential_info = essential_info.rename(columns={"course_id": "id"})
    course_id_col = essential_info.pop("id")
    essential_info.insert(0, "id", course_id_col)
    return essential_info


def dumpJson(
    students_filename, marks_filename, tests_filename, courses_filename, output_path
):

    studentCSV = csv.DictReader(open(students_filename))
    wholeList = constructJsonValue(studentCSV)

    individualCourseList = constructIndividualCourseList(
        marks_filename, tests_filename, courses_filename
    )
    studen_courses_dict = {}

    for individualCourse in individualCourseList:
        df = createCourseRecord(individualCourse)
        studen_courses_dict[(df["student_id"].unique()[0])] = df.drop(
            ["student_id"], axis=1
        )

    for student in wholeList:
        jsdf = studen_courses_dict[int(student["id"])].to_json(orient="records")
        student["courses"] = json.loads(jsdf)
        student["totalAverage"] = round(
            (studen_courses_dict[int(student["id"])]["courseAverage"]).mean(), 2
        )

    data = {}
    data["students"] = wholeList

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
        checkCourses(courses_filename) == p
        and checkStudents(students_filename) == p
        and checkTests(tests_filename) == p
        and checkMarks(marks_filename, students_filename, tests_filename) == p
    ):
        print("all check on JSON files passed.")
        dumpJson(
            students_filename,
            marks_filename,
            tests_filename,
            courses_filename,
            output_path,
        )
        print("Result JSON file has been created. Please check output directory.")
    else:
        t = checkTests(tests_filename)
        c = checkCourses(courses_filename)
        s = checkStudents(students_filename)
        m = checkMarks(marks_filename, students_filename, tests_filename)
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


# check_run("students.csv", "marks.csv", "tests.csv","courses.csv")
# check_run("students.csv", "marks.csv", "tests.csv","courses.csv", "output")
def dump_json_api(
    students_filename, marks_filename, tests_filename, courses_filename
):

    studentCSV = csv.DictReader(open(students_filename))
    wholeList = constructJsonValue(studentCSV)

    individualCourseList = constructIndividualCourseList(
        marks_filename, tests_filename, courses_filename
    )
    studen_courses_dict = {}

    for individualCourse in individualCourseList:
        df = createCourseRecord(individualCourse)
        studen_courses_dict[(df["student_id"].unique()[0])] = df.drop(
            ["student_id"], axis=1
        )
    print(studen_courses_dict)
    for student in wholeList:
        if int(student["id"]) in studen_courses_dict:
            jsdf = studen_courses_dict[int(student["id"])].to_json(orient="records")
            student["courses"] = json.loads(jsdf)
            student["totalAverage"] = round(
                (studen_courses_dict[int(student["id"])]["courseAverage"]).mean(), 2
            )

    data = {}
    data["students"] = wholeList

    return data



def check_run_api(
    students_filename, marks_filename, tests_filename, courses_filename
):
    result = ""
    p = "pass"
    if (
        checkCourses(courses_filename) == p
        and checkStudents(students_filename) == p
        and checkTests(tests_filename) == p
        and checkMarks(marks_filename, students_filename, tests_filename) == p
    ):
        print("all check on JSON files passed.")
        return dump_json_api(
            students_filename,
            marks_filename,
            tests_filename,
            courses_filename,
        )
    else:
        t = checkTests(tests_filename)
        c = checkCourses(courses_filename)
        s = checkStudents(students_filename)
        m = checkMarks(marks_filename, students_filename, tests_filename)
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