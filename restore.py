import boto3
import datetime
from datetime import datetime
import time
import traceback
import csv
import requests
import os

bsess = boto3.Session(profile_name='default')

class Logs:
    @staticmethod
    def warning(logBody):
        print("[WARNING] {}".format(logBody))

    @staticmethod
    def critical(logBody):
        print("[CRITICAL] {}".format(logBody))

    @staticmethod
    def info(logBody):
        print("[INFO] {}".format(logBody))


class S3:
    BUCKET = ""
    REGION = ""
    def __init__(self, bucket, region):
        self.BUCKET = bucket
        self.REGION = region

    def downloadFile (self, src, dest):
        try:
            bsess.resource('s3').meta.client.download_file(self.BUCKET, src, dest)
        except Exception as e:
            Logs.critical("Error downloading file")
            Logs.critical(traceback.format_exc())
            exit()
class CSV:
    FILENAME = ""
    FOLDER = 'C:/Users/nisha/.jenkins/workspace/test/dev/11/'
    def __init__(self, filename):
        self.FILENAME = filename
    
    def readBackup(self):
        row_count = 0
        groups = []
        import csv
        with open(self.FILENAME, 'r') as file:
            csv_file = csv.DictReader(file)
            for row in csv_file:
                groups.append(dict(row))
        return groups

class Cognito:
    USERPOOLID = ""
    REGION = ""
    ATTRIBUTES = ""
    def __init__(self, userPoolId, region, attributes):
        self.USERPOOLID = userPoolId
        self.REGION = region
        self.ATTRIBUTES = attributes
    
    def importUsers (self):
        try:
            print("importUsers")
        except Exception as e:
            Logs.critical("Error importing users")
            Logs.critical(traceback.format_exc())
            exit()
    
    def importGroups (self, groups):
        try:
            boto = bsess.client('cognito-idp')
            for group in groups:
                print(group)
                if not self.checkIfGroupExists(group["GroupName"]):
                    kwargs = {
                        'UserPoolId': self.USERPOOLID
                    }
                    for attribute in self.ATTRIBUTES:
                        if (group[str(attribute)].isnumeric()):
                            kwargs[str(attribute)] = int(group[attribute])
                        else:
                            kwargs[str(attribute)] = str(group[attribute])
                    response = boto.create_group(**kwargs)
                else:
                    Logs.info("Group {} already exists".format(group["GroupName"]))
        except Exception as e:
            Logs.critical("Error importing groups")
            Logs.critical(traceback.format_exc())
            exit()

    def checkIfGroupExists(self, groupName):
        try:
            boto = bsess.client('cognito-idp')
            response = boto.get_group(
                GroupName=groupName,
                UserPoolId=self.USERPOOLID
            )
            return True
        except Exception as e:
            return False
    
    def importUsers(self, filename):
        try:
            #client = bsess.client("cognito-idp", region_name="ap-south-1")
            boto = bsess.client('cognito-idp')
            
            response = boto.get_csv_header(
                UserPoolId=self.USERPOOLID
            )

            response = boto.create_user_import_job(
                JobName='Import-Test-Job',
                UserPoolId=self.USERPOOLID,
                CloudWatchLogsRoleArn='arn:aws:iam::401901776652:role/CognitoImportRole'
            )
            #print(response)

            #UPLOAD CSV File
            content_deposition = 'attachment;filename='+filename;
            presigned_url = response['UserImportJob']['PreSignedUrl']
            print(presigned_url)
            headers_dict = {
                'x-amz-server-side-encryption': 'aws:kms',
                'Content-Disposition': content_deposition
            }
            with open(filename, 'rb') as csvFile:
                file_upload_response = requests.put(
                    presigned_url, 
                    data=csvFile, 
                    headers=headers_dict
                )

            response2 = boto.start_user_import_job(
                UserPoolId=self.USERPOOLID,
                JobId=response["UserImportJob"]["JobId"]
            )
            print(response2)
        except Exception as e:
            Logs.critical("Error importing users")
            Logs.critical(traceback.format_exc())
            exit()


def main():
    REGION = "us-east-2"
    COGNITO_ID = "us-east-1_2etl4eauM"
    BACKUP_FILE_USERS = "cognito_backup_users_20240126-1333.csv"
    BACKUP_FILE_GROUPS = "cognito_backup_groups_20240126-1333.csv"
    BACKUP_BUCKET = "userpool-backup"
    cognitS3 = S3(BACKUP_BUCKET, REGION)
    FOLDER='C:/Users/nisha/.jenkins/workspace/test/dev/11/'
    # DOWNLOAD GROUPS
    cognitS3.downloadFile(BACKUP_FILE_GROUPS, FOLDER+BACKUP_FILE_GROUPS)
    # IMPORT GROUPS
    csvGroups = CSV(FOLDER+BACKUP_FILE_GROUPS)
    groups = csvGroups.readBackup()
    GATTRIBUTES = [
        'GroupName',
        'Description',
        'Precedence'
    ]
    cognito = Cognito(COGNITO_ID, REGION, GATTRIBUTES)
    cognito.importGroups(groups)


    # DOWNLOAD USERS
    cognitS3.downloadFile(BACKUP_FILE_USERS, FOLDER+BACKUP_FILE_USERS)

    #csvUsers = CSV(FOLDER+BACKUP_FILE_USERS)
    #users = csvUsers.readBackup()
    ATTRIBUTES = [
        'email',
        'username'        
    ]
    cognitoUsers = Cognito(COGNITO_ID, REGION, ATTRIBUTES)
    cognitoUsers.importUsers(FOLDER+BACKUP_FILE_USERS)

    # IMPORT USERS

main()
