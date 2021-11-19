import sys
import argparse
from process import check_run_api
from flask import Flask
from flask_restful import Resource, Api, reqparse
import ast
import pandas as pd
import json

app = Flask(__name__)
api = Api(app)

#Created Flask app to host process.py script as a service on an endpoint.

#Records endpoint, to get the final result.
class Records(Resource):
    
    def get(self):
        return check_run_api("students.csv", "marks.csv", "tests.csv", "courses.csv")
api.add_resource(Records, '/records')



#Students endpoint. 
# To get students.csv
# Post new students to students.csv
# Update students to students.csv
# Delete students from students.csv
class Students(Resource):
    def __init__(self):
        self.data = pd.read_csv("students.csv")
        self.js_data = self.data.to_json(orient="records")

    def get(self):
        return {'data': json.loads(self.js_data)}, 200

    def post(self):
        parser = reqparse.RequestParser()  # initialize
        
        parser.add_argument('id', required=True)  # add args
        parser.add_argument('name', required=True)
                
        args = parser.parse_args()
        data = pd.read_csv('students.csv')
        if int(args['id']) in list(data['id']):
            return {
                'message': f"'{args['id']}' already exists in students table."
            }, 401
        elif not args['name']:
            return {
                'message': f" Student name: '{args['name']}' contains invalid value."
            }, 401
        else:
            new_data = pd.DataFrame({
                'id': int(args['id']),
                'name': args['name'],
            }, index = [0])
            data = data.append(new_data, ignore_index=True)
            data.to_csv('students.csv', index=False)
            return {'data': json.loads(self.js_data)}, 200


    def put(self):
        parser = reqparse.RequestParser()  # initialize
        parser.add_argument('id', required=True)  # add args
        parser.add_argument('name', required=True)
        args = parser.parse_args()  # parse arguments to dictionary

        # read CSV
        data = pd.read_csv('students.csv')

        if int(args['id']) in list(data['id']):
            data['name'][data['id'] == int(args['id'])] = args['name']

        # save back to CSV
            data.to_csv('students.csv', index=False)
            # return data and 200 OK
            read = pd.read_csv("students.csv")
            js_data_put = read.to_json(orient="records")
            return {'data': json.loads(js_data_put)}, 200
        else:
            # otherwise the student does not exist
            return {
                'message': f"'{args['id']}' student not found."
            }, 404


    def delete(self):
        parser = reqparse.RequestParser()  # initialize
        parser.add_argument('id', required=True)  # add args
        args = parser.parse_args()  # parse arguments to dictionary
        data = pd.read_csv('students.csv')

        if int(args['id']) in list(data['id']):
            data = data[data.id != int(args['id'])]

        # save back to CSV
            data.to_csv('students.csv', index=False)
            # return data and 200 OK
            read = pd.read_csv("students.csv")
            js_data_put = read.to_json(orient="records")
            return {'data': json.loads(js_data_put)}, 200
        else:
            # otherwise the userId does not exist
            return {
                'message': f"'{args['id']}' student not found."
            }, 404

api.add_resource(Students, '/students')



#Marks endpoint, to get the marks.csv.
class Marks(Resource):
    def __init__(self):
        self.data = pd.read_csv("marks.csv")
        self.js_data = self.data.to_json(orient="records")
    def get(self):
        return {'data': json.loads(self.js_data)}, 200
api.add_resource(Marks, '/marks')


#Tests endpoint, to get the tests.csv.
class Tests(Resource):
    def __init__(self):
        self.data = pd.read_csv("tests.csv")
        self.js_data = self.data.to_json(orient="records")
    def get(self):
        return {'data': json.loads(self.js_data)}, 200         
api.add_resource(Tests, '/tests')



#Courses endpoint, to get the courses.csv.
class Courses(Resource):
    def __init__(self):
        self.data = pd.read_csv("courses.csv")
        self.js_data = self.data.to_json(orient="records")
    def get(self):
        return {'data': json.loads(self.js_data)}, 200
api.add_resource(Courses, '/courses')

if __name__ == '__main__':
    app.run()







# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("pathToStudentsFile")
#     parser.add_argument("pathToMarksFile")
#     parser.add_argument("pathToTestsFile")
#     parser.add_argument("pathToCoursesFile")
#     parser.add_argument("pathToOutputFile")
#     args = parser.parse_args()

#     check_run(
#         args.pathToStudentsFile,
#         args.pathToMarksFile,
#         args.pathToTestsFile,
#         args.pathToCoursesFile,
#         args.pathToOutputFile,
#     )


# check_run('students.csv', 'marks.csv', 'tests.csv', 'courses.csv', 'result')
# main()
