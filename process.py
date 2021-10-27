import csv
import pandas as pd
import numpy as np
import json

def checkTests(testsCSV):
    pass
def checkStudents(studentsCSV):
    pass
def checkCourses(coursesCSV):
    pass
def checkMarks(marksCSV):
    pass

#[{'id': 1, 'name': 'A', 'totalAverage': None, 'courses': []}, 
# {'id': 2, 'name': 'B', 'totalAverage': None, 'courses': []}, 
# {'id': 3, 'name': 'C', 'totalAverage': None, 'courses': []}]
def constructJsonValue(studentCSV):
    students = []
    for row in studentCSV:
        temp_student_index = row['id']
        row['id'] = int(temp_student_index)
        students.append(row)

    for i in students:
        i['totalAverage'] = None
        i['courses'] = []
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
    tests_df = tests_df.rename(columns={'id': 'test_id'})
    courses_df = courses_df.rename(columns={'id': 'course_id'})
    #marks_df["course_id"] = marks_df["test_id"].map(tests_df["id"])
    df = pd.merge(marks_df, tests_df, on='test_id')
    df = pd.merge(df, courses_df, on='course_id')
    student_id_col = df.pop('student_id')
    df.insert(0, 'student_id', student_id_col)
    df = df.sort_values(['student_id']).reset_index(drop=True)
    student_record_list = [v for k, v in df.groupby('student_id')]
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
    sorted_by_course = student_record_df.sort_values(['course_id']).reset_index(drop=True)
    df = sorted_by_course.groupby('course_id').apply(w_avg, 'mark', 'weight')
    df = pd.merge(df.to_frame(), sorted_by_course, on='course_id')
    essential_info = df.drop(['mark', 'weight', 'test_id'], axis = 1).drop_duplicates().reset_index(drop=True)
    teacher_name_col = essential_info.pop('teacher')
    essential_info.insert(0, 'teacher', teacher_name_col)
    course_name_col = essential_info.pop('name')
    essential_info.insert(0, 'name', course_name_col)
    essential_info = essential_info.rename(columns={0: 'courseAverage'})
    essential_info = essential_info.rename(columns={'course_id': 'id'})
    course_id_col = essential_info.pop('id')
    essential_info.insert(0, 'id', course_id_col)
    return essential_info

def dumpJson(students_filename,marks_filename, tests_filename, courses_filename):

    studentCSV = csv.DictReader(open(students_filename))      
    wholeList = constructJsonValue(studentCSV)

    individualCourseList = constructIndividualCourseList(marks_filename, tests_filename, courses_filename)
    studen_courses_dict = {}
    
    for individualCourse in individualCourseList:
        df = createCourseRecord(individualCourse)
        studen_courses_dict[(df['student_id'].unique()[0])] = df.drop(['student_id'], axis=1)

    for student in wholeList:
        jsdf = studen_courses_dict[int(student['id'])].to_json(orient = 'records')
        student['courses'] = json.loads(jsdf)
        student['totalAverage'] = round((studen_courses_dict[int(student['id'])]['courseAverage']).mean(), 2)

    data = {}
    data['students'] = wholeList
    with open('result.json', 'w') as fp:
        json.dump(data, fp, indent=2)


#dumpJson("students.csv", "marks.csv", "tests.csv","courses.csv")
