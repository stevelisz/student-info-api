import csv
import pandas as pd
import numpy as np
import json
studentCSV = csv.DictReader(open("students.csv"))
testCSV = csv.DictReader(open("tests.csv"))
coursesCSV = csv.DictReader(open("courses.csv"))
marksCSV = csv.DictReader(open("marks.csv"))


def constructJsonValue(studentCSV):
    students = []
    for row in studentCSV:
        temp_student_index = row['id']
        row['id'] = int(temp_student_index)
        students.append(row)

    for i in students:
        i['totalAverage'] = None
        i['Courses'] = []
    return students
def checkTests(testsCSV):
    pass
def checkStudents(studentsCSV):
    pass
def checkCourses(coursesCSV):
    pass
def checkMarks(marksCSV):
    pass

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
    student_record_list = []
    student_record_list = [v for k, v in df.groupby('student_id')]
    return student_record_list


def w_avg(df, values, weights):
    d = df[values]
    w = df[weights]
    return (d * w).sum() / w.sum()

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

      
wholeList = constructJsonValue(studentCSV)
#print(wholeList['id'])
print(wholeList)
temp = constructIndividualCourseList("marks.csv", "tests.csv","courses.csv")
studen_courses_dict = {}
#print(temp)    
for i in temp:
    df = createCourseRecord(i)

    #print(df['student_id'].unique()[0])
    studen_courses_dict[(df['student_id'].unique()[0])] = df.drop(['student_id'], axis=1)

#print(studen_courses_dict[2])

for i in wholeList:
    #.to_json(orient = 'records')
    
    print(studen_courses_dict[int(i['id'])])
    jsdf = studen_courses_dict[int(i['id'])].to_json(orient = 'records')
    i['Courses'] = json.loads(jsdf)
    i['totalAverage'] = round((studen_courses_dict[int(i['id'])]['courseAverage']).mean(), 2)



#print(wholeList)##
#json_object = json.dumps(wholeList, indent = 4) 
print(wholeList)
data = {}
data['students'] = wholeList
with open('result.json', 'w') as fp:
    json.dump(data, fp, indent=2)
#print(json_data)