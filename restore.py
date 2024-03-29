import boto3
import datetime
from datetime import datetime
import time
import traceback
import csv
import requests
import os

bsess = boto3.Session(profile_name='default')
dateNow=os.environ.get('BACKUP_DATE', '')
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

    def downloadFile(self, src, dest):
        try:
            src = "cognito-backup/" + dateNow + "/" + src
            bsess.resource('s3').meta.client.download_file(self.BUCKET, src, dest)
        except Exception as e:
            Logs.critical("Error downloading file")
            Logs.critical(traceback.format_exc())
            exit()


class CSV:
    FILENAME = ""
    FOLDER = os.environ.get('WORKSPACE', '')

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

    def importGroups(self, groups, cognitS3, FOLDER):
        try:
            boto = bsess.client('cognito-idp')
            for group in groups:
                if not self.checkIfGroupExists(group["GroupName"]):
                    kwargs = {
                        'UserPoolId': self.USERPOOLID
                    }
                    for attribute in self.ATTRIBUTES:
                        
                        if attribute.isnumeric():
                            kwargs[attribute] = int(group[attribute])
                        elif attribute == "Precedence":
        # Check if "Precedence" is not empty before attempting the conversion
                            if group[attribute]:
                                kwargs[attribute] = int(group[attribute])
                        else:
                            kwargs[attribute] = str(group[attribute])
                    response = boto.create_group(**kwargs)                
                BACKUP_USER_GRP="cognito_backup_users_"+ group["GroupName"] + ".csv"
                cognitS3.downloadFile(BACKUP_USER_GRP, FOLDER + "\\" + BACKUP_USER_GRP)
                csvGroups = CSV(FOLDER + "\\" + BACKUP_USER_GRP)                    
                UserGroups = csvGroups.readBackup()                    
                for u in UserGroups:                      
                      boto.admin_add_user_to_group(UserPoolId=self.USERPOOLID,Username=u["cognito:username"],GroupName=group["GroupName"])
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
            boto = bsess.client('cognito-idp')
            
            response = boto.get_csv_header(
                UserPoolId=self.USERPOOLID
            )

            response = boto.create_user_import_job(
                JobName='Import-Test-Job',
                UserPoolId=self.USERPOOLID,
                CloudWatchLogsRoleArn='arn:aws:iam::401901776652:role/CognitoImportRole'
            )

            # UPLOAD CSV File
            presigned_url = response['UserImportJob']['PreSignedUrl']
            print(presigned_url)
            headers_dict = {
                'x-amz-server-side-encryption': 'aws:kms',
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
    REGION = os.environ.get('REGION', '')
    COGNITO_ID = os.environ.get('COGNITO_ID', '')    
    BACKUP_FILE_USERS = "cognito_backup_users"+".csv"
    BACKUP_FILE_GROUPS = "cognito_backup_groups"+".csv"
    BACKUP_BUCKET = os.environ.get('BACKUP_BUCKET', '')
    cognitS3 = S3(BACKUP_BUCKET, REGION)
    FOLDER = os.environ.get('WORKSPACE', '')

    # DOWNLOAD USERS
    cognitS3.downloadFile(BACKUP_FILE_USERS, FOLDER + BACKUP_FILE_USERS)

    # IMPORT USERS
    ATTRIBUTES = [
        'email',
        'username'
    ]
    cognitoUsers = Cognito(COGNITO_ID, REGION, ATTRIBUTES)
    cognitoUsers.importUsers(FOLDER + BACKUP_FILE_USERS)
    
    
    # DOWNLOAD GROUPS
    cognitS3.downloadFile(BACKUP_FILE_GROUPS, FOLDER + BACKUP_FILE_GROUPS)

    # IMPORT GROUPS
    csvGroups = CSV(FOLDER + BACKUP_FILE_GROUPS)
    groups = csvGroups.readBackup()
    GATTRIBUTES = [
        'GroupName',
        'Description',
        'Precedence'
    ]
    time.sleep(20)
    cognito = Cognito(COGNITO_ID, REGION, GATTRIBUTES)
    cognito.importGroups(groups, cognitS3, FOLDER)



main()
