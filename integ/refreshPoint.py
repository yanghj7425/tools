import pymysql as mysql
import integ

import traceback
import time
import datetime


class refreshPoint:
    def __init__(self):

        self.host = '192.168.0.49'
        self.port = 3306
        self.user = "root"
        self.password = "dreamsoft"
        self.dataBase = 'ipb_pudong'

    
    def getConnection(self):

        db = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.dataBase,
            charset="utf8")
        return db



    def getTotalPointSQL(self):

        return '''
                SELECT 
                    MEMBERID, TOTAL
                FROM
                    (SELECT 
                        MEMBERID, TOTAL
                    FROM
                        integ_person_point
                    WHERE
                        YEAR = '2018') A
                GROUP BY MEMBERID , TOTAL
               '''


    def getBaseInfoSumSQL(self):

        return '''
                    SELECT 
                        sum(BASCPOINT)
                    FROM
                        integ_base
                    WHERE
                        memberid = '{memberId}'
                            AND year = '2018'
                '''
    
    def getUpdateTotalPointSQL(self):
        return '''
                UPDATE integ_person_point 
                SET 
                    TOTAL = '{baseSum}'
                WHERE
                    memberid = '{memberId}' AND year = '2018';
              '''


    def refresh(self,file):
        db = self.getConnection()
        cursor = db.cursor()

        cursor.execute(self.getTotalPointSQL())


        memberPoints = cursor.fetchall()

        for memberPoint in memberPoints:

            memberId = memberPoint[0]
            totalPoint = float(memberPoint[1])

            queryMemberBaseSumSQL = self.getBaseInfoSumSQL().format(memberId = memberId)
            cursor.execute(queryMemberBaseSumSQL)

            memberBaseSums = cursor.fetchall()

            for memberBaseSum in memberBaseSums:
                baseSum = float(memberBaseSum[0])
                if totalPoint != baseSum:
                    print(memberId, totalPoint, baseSum)
                    updateTotalPointSQL = self.getUpdateTotalPointSQL().format(memberId = memberId, baseSum = baseSum )
                    file.write(updateTotalPointSQL)










a = refreshPoint()
t = time.time()

fileName = 'refreshPoint' + str(int(t)) + '.sql'
file = open(fileName,'a',encoding='utf-8')
a.refresh(file)
file.close()
