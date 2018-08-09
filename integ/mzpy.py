import pymysql as mysql

import traceback
import time
import datetime


import integ

class SumInteg:
    def __init__(self):
        self.host = '192.168.0.28'
        self.port = 3306
        self.user = "root"
        self.password = "123456"
        self.dataBase = 'ipb_pudong'

    def getConnection(self):
        db =  mysql.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                db = self.dataBase
            )

        return db

    def changeSumInteg(self,file):

        db = self.getConnection()

        cur = db.cursor()

        queryIntegSql = '''
                                SELECT 
                                    PARTY_MEMBER_ID, rule_id, SUM(sub.point), frule.total,rule.POINT
                                FROM
                                    INTEG_PERSON_SUBSIDIARY sub
                                LEFT JOIN integ_rule rule ON sub.rule_id = rule.id
                                LEFT JOIN integ_rule frule ON rule.FID = frule.ID
                                WHERE
                                    sub.rule_id = '53e7c191529411e79ab100262d0ccbdf'
                                        AND sub.year = '2018'
                                GROUP BY PARTY_MEMBER_ID , rule_id
                                HAVING SUM(sub.point) > frule.total
                            '''

        queryOverFlowIntegSql = ''' 
                                    SELECT 
                                        ID, POINT
                                    FROM
                                        INTEG_PERSON_SUBSIDIARY
                                    WHERE
                                        PARTY_MEMBER_ID = '{memberId}'
                                            AND rule_id = '{ruleId}'
                                            AND point != 0
                                    ORDER BY point DESC , creattime
                                    LIMIT 1 , 100

                                '''


        setZearoPoint2Subsidiary =  '''
                                    UPDATE INTEG_PERSON_SUBSIDIARY 
                                    SET 
                                        POINT = '0'
                                    WHERE
                                        id = '{overFlowItemId}' '''

        cur.execute(queryIntegSql)
        result = cur.fetchall()
        integSum = integ.Integ()

       
        
        for item in result:
            memberId = item[0]
            ruleId = item[1]
            curSumInteg = item[2] # 当前获得分数
            maxSumInteg = item[3]
            eachInteg = item[4]
            
            overCount = maxSumInteg/eachInteg

            formatSql = queryOverFlowIntegSql.format(memberId= memberId,ruleId = ruleId, overCount= overCount)

            cur.execute(formatSql)
            overFlowItems = cur.fetchall()

            for overFlowItem in overFlowItems:
                
                overFlowItemId = overFlowItem[0]
                overFlowItemScore = overFlowItem[1]
                formatSql = setZearoPoint2Subsidiary.format(overFlowItemId = overFlowItemId)
                try:
                    cur.execute(formatSql)
                    file.write('\n'+formatSql + ';#'+ str(overFlowItemScore) + '\n' )
                    db.commit()
                except:
                    traceback.print_exc()
                    db.rollback()
               
                
            
            integSum.show(file, memberId)



t = time.time()
fielName = 'sumIntegConifg_' + str(int(t))+ '.sql'
file = open(fielName, 'a', encoding='utf-8')

sumInteg = SumInteg()

sumInteg.changeSumInteg(file)

file.close()




    