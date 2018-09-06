import json
import pymysql as mysql
import traceback
import time
import datetime

class MemberLabel:

    def __init__(self):
        self.host = '192.168.0.28'
        self.port = 3306
        self.user = "root"
        self.password = "123456"
        self.dataBase = 'ipb_pudong'

    
    def getConnection(self):
        db = mysql.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                db = self.dataBase,
                charset="utf8"
        )
        return db

    def getChangeMemberLabelSQL(self):
        return  '''
                    SELECT 
                        ID, MEMBERLABEL
                    FROM
                        org_partymember
                    WHERE
                        IFNULL(MEMBERLABEL, '') != ''
                '''


    def getUpdateMemberLabelSql(self):
        return  '''
                    UPDATE org_partymember 
                        SET 
                            ISSJ = '',
                            ISDDB = '',
                            ISSJYJT = '',
                            ISRDDB = '',
                            ISZXWY = '',
                            ISDXXP = ''
                        WHERE
                            id = '';
                '''


    def refresh(self):
        db = self.getConnection()
        cursor = db.cursor()
        cursor.execute(self.getChangeMemberLabelSQL())
        members = cursor.fetchall()
        jsonStr = '{"书记":"ISSJ","党代表":"ISDDB","书记主任一肩挑":"ISSJYJT","人大代表":"ISRDDB","政协委员:"ISZXWY"","定向选派":"ISDXXP"}';
        jsonObj = json.loads(jsonStr)
        for member in members:
            labels = member[1].split(',')
            memberId = member[0]
            






