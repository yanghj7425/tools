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
                            ISSJ = '{ISSJ}',
                            ISDDB = '{ISDDB}',
                            ISSJYJT = '{ISSJYJT}',
                            ISRDDB = '{ISRDDB}',
                            ISZXWY = '{ISZXWY}',
                            ISDXXP = '{ISDXXP}'
                        WHERE
                            ID = '{memberId}';
                '''

    def isMatchLabel(self, labels, lab):
        for label in labels:
            if label == lab:
                return 1
        return 0


    def refresh(self,file):
        db = self.getConnection()
        cursor = db.cursor()
        cursor.execute(self.getChangeMemberLabelSQL())
        members = cursor.fetchall()
        jsonStr = '{"书记":"ISSJ","党代表":"ISDDB","书记主任一肩挑":"ISSJYJT","人大代表":"ISRDDB","政协委员":"ISZXWY","定向选派":"ISDXXP"}'

        jsonObj = json.loads(jsonStr)
        labelJson = {}
        for member in members:
            memberId = member[0]
            labels = member[1].split(',')
            for lab in jsonObj:
                key = jsonObj[lab]
                labelJson[key] = self.isMatchLabel(labels,lab)

            labelJson['memberId'] = memberId
            
            updateSQL =  self.getUpdateMemberLabelSql().format(ISSJ = labelJson.get("ISSJ"),
                                                               ISDDB = labelJson.get("ISDDB"), 
                                                               ISSJYJT = labelJson.get("ISSJYJT"), 
                                                               ISRDDB = labelJson.get("ISRDDB"),
                                                               ISZXWY = labelJson.get("ISZXWY"),
                                                               ISDXXP = labelJson.get("ISDXXP"),
                                                               memberId = labelJson.get("memberId"))
            file.write( "\t\t\t\t\t#"+member[1]+ updateSQL +"\r\n")
            
t = time.time()
reLabel = MemberLabel()

fielName = 'RefreshMemberLabel' + str(int(t))+ '.sql'
file = open(fielName, 'a', encoding='utf-8')

reLabel.refresh(file)
file.close()



