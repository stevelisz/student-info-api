import sys
import argparse
from process import check_run
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("pathToStudentsFile")
    parser.add_argument("pathToMarksFile")
    parser.add_argument("pathToTestsFile")
    parser.add_argument("pathToCoursesFile")
    parser.add_argument("pathToOutputFile")
    args = parser.parse_args()

    check_run(args.pathToStudentsFile, args.pathToMarksFile, args.pathToTestsFile, args.pathToCoursesFile, args.pathToOutputFile)

main()
