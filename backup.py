from email import header
import boto3
import datetime
from datetime import datetime
import time
import argparse
import traceback
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

class Cognito:
    USERPOOLID = ""
    REGION = ""
    ATTRIBUTES = ""

    def __init__(self, userPoolId, region, attributes):
        self.USERPOOLID = userPoolId
        self.REGION = region
        self.ATTRIBUTES = attributes
    
    def getAttributes(self):
        try:
            boto = bsess.client('cognito-idp')
            headers = boto.get_csv_header(
                UserPoolId=self.USERPOOLID
            )
            self.ATTRIBUTES = headers["CSVHeader"]
            return headers["CSVHeader"]
        except Exception as e:
            Logs.critical("There is an error listing users attributes")
            Logs.critical(traceback.format_exc())
            exit()

    def listUsers(self):
        try:
            boto = bsess.client('cognito-idp')

            users = []
            next_page = None
            kwargs = {
                'UserPoolId': self.USERPOOLID,
            }
            users_remain = True
            while users_remain:
                if next_page:
                    kwargs['PaginationToken'] = next_page
                response = boto.list_users(**kwargs)
                users.extend(response['Users'])
                next_page = response.get('PaginationToken', None)
                users_remain = next_page is not None
                # COOL DOWN BEFORE NEXT QUERY
                time.sleep(0.15)

            return users
        except Exception as e:
            Logs.critical("There is an error listing cognito users")
            Logs.critical(traceback.format_exc())
            exit()
    
    def listGroups(self):
        try:
            boto = bsess.client('cognito-idp')
            groups = []
            next_page = None
            kwargs = {
                'UserPoolId': self.USERPOOLID,
            }
            groups_remain = True
            while groups_remain:
                if next_page:
                    kwargs['NextToken'] = next_page
                response = boto.list_groups(**kwargs)
                groups.extend(response['Groups'])
                next_page = response.get('NextToken', None)
                groups_remain = next_page is not None
                # COOL DOWN BEFORE NEXT QUERY
                time.sleep(0.15)
            return groups
        except Exception as e:
            Logs.critical("There is an error listing cognito groups")
            Logs.critical(traceback.format_exc())
            exit()

    def listUsersInGroup(self, group_name):
        try:
            boto = bsess.client('cognito-idp')
            users = []
            next_page = None
            kwargs = {
                'UserPoolId': self.USERPOOLID,
                'GroupName': group_name,
            }
            users_remain = True
            while users_remain:
                if next_page:
                    kwargs['PaginationToken'] = next_page
                response = boto.list_users_in_group(**kwargs)
                users.extend(response['Users'])
                next_page = response.get('PaginationToken', None)
                users_remain = next_page is not None
                # COOL DOWN BEFORE NEXT QUERY
                time.sleep(0.15)
            return users
        except Exception as e:
            Logs.critical("Error listing cognito users in group {}".format(group_name))
            Logs.critical(traceback.format_exc())
            exit()

class CSV:
    FILENAME = ""
    FOLDER = ''
    ATTRIBUTES = ""
    CSV_LINES = []

    def __init__(self, attributes, prefix):
        self.ATTRIBUTES = attributes
        # Use Jenkins workspace as the base folder
        self.FOLDER = os.environ.get('WORKSPACE', '')
        self.FILENAME = "cognito_backup_" + prefix + "_" + datetime.now().strftime("%Y%m%d-%H%M") + ".csv"
        self.CSV_LINES = []

    def generateUserContent(self, records):
        try:
            # ADD TITLES
            csv_new_line = self.addTitles()

            # ADD USERS
            for user in records:
                csv_line = csv_new_line.copy()
                for requ_attr in self.ATTRIBUTES:
                    csv_line[requ_attr] = ''
                    if requ_attr in user.keys():
                        csv_line[requ_attr] = str(user[requ_attr])
                        continue
                    for usr_attr in user['Attributes']:
                        if usr_attr['Name'] == requ_attr:
                            csv_line[requ_attr] = str(usr_attr['Value'])
                csv_line["cognito:mfa_enabled"] = "false"
                csv_line["cognito:username"] = user["Username"]
                self.CSV_LINES.append(",".join(csv_line.values()) + '\n')       
            return self.CSV_LINES
        except Exception as e:
            Logs.critical("Error generating csv content")
            Logs.critical(traceback.format_exc())
            exit()
    
    def generateGroupContent(self, records):
        try:
            # ADD TITLES
            csv_new_line = self.addTitles()

            # ADD GROUPS
            for group in records:
                csv_line = {}
                for groupParam in self.ATTRIBUTES:
                    csv_line[str(groupParam)] = str(group.get(str(groupParam), ''))
                self.CSV_LINES.append(",".join(csv_line.values()) + '\n')
            return self.CSV_LINES
        except Exception as e:
            Logs.critical("Error generating csv content")
            Logs.critical(traceback.format_exc())
            exit()
    
    def addTitles(self):
        csv_new_line = {self.ATTRIBUTES[i]: '' for i in range(len(self.ATTRIBUTES))}
        self.CSV_LINES.append(",".join(csv_new_line) + '\n')
        return csv_new_line
    
    def saveToFile(self):
        try:
            # Use os.path.join to create the full path
            csvFile = open(os.path.join(self.FOLDER, self.FILENAME), 'a')
            csvFile.writelines(self.CSV_LINES)
            csvFile.close()
        except Exception as e:
            Logs.critical("Error saving csv file")
            Logs.critical(traceback.format_exc())
            exit()

class S3:
    BUCKET = ""
    REGION = ""

    def __init__(self, bucket, region):
        self.BUCKET = bucket
        self.REGION = region
    
    def uploadFile(self, src, dest):
        try:
            bsess.resource('s3').meta.client.upload_file(src, self.BUCKET, dest)
        except Exception as e:
            Logs.critical(f"Error uploading the backup file {src} to S3: {e}")
            Logs.critical(traceback.format_exc())
            exit()

def main():
    REGION =  os.environ.get('REGION', '')
    COGNITO_ID = os.environ.get('COGNITO_ID', '')
    BACKUP_BUCKET = os.environ.get('BACKUP_BUCKET', '')
    GATTRIBUTES = [
        'GroupName',
        'Description',
        'Precedence'
    ]

    cognito = Cognito(COGNITO_ID, REGION, [])
    cognitoS3 = S3(BACKUP_BUCKET, REGION)

    ATTRIBUTES = cognito.getAttributes()
    
    csvUsers = CSV(ATTRIBUTES, "users")
    user_records = cognito.listUsers()
    csvUsers.generateUserContent(user_records)
    csvUsers.saveToFile()    
    Logs.info("Total Exported User Records: {}".format(len(csvUsers.CSV_LINES)))
    cognitoS3.uploadFile(csvUsers.FOLDER + "/" + csvUsers.FILENAME, csvUsers.FILENAME)

    csvGroups = CSV(GATTRIBUTES, "groups")
    group_records = cognito.listGroups()


    csvGroups.generateGroupContent(group_records)
    csvGroups.saveToFile()  # Save group information for each group
    # Upload the final group data after processing all groups
    groups_filename = csvGroups.FILENAME    
    cognitoS3.uploadFile(csvGroups.FOLDER + "/" + groups_filename, groups_filename)

    Logs.info("Total Exported Group Records: {}".format(len(csvGroups.CSV_LINES)))
    
    for group in group_records:
        group_users = cognito.listUsersInGroup(group_name=group['GroupName'])
        csvUsers = CSV(ATTRIBUTES, "users_{}".format(group['GroupName']))
        csvUsers.generateUserContent(group_users)
        csvUsers.saveToFile()

        # Upload user data for each group
        users_filename = "cognito_backup_users_{}_{}.csv".format(group['GroupName'], datetime.now().strftime("%Y%m%d-%H%M"))
        cognitoS3.uploadFile(csvUsers.FOLDER + "/" + users_filename, users_filename)

main()
