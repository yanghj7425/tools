import pymysql as mysql
import integ

import traceback
import time
import datetime

class Integ:
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


    def getBeChangedMemberSQL(self):
        return   '''
                    SELECT 
                        rul.id,
                        sub.PARTY_MEMBER_ID,
                        rul.fid,
                        ifnull((SUM(sub.POINT) - TOTAL) / (rul.POINT),0) overs,
                        SUM(sub.POINT) - IFNULL(TOTAL, 0) AS subtraction,
                        IFNULL(TOTAL, 0) TOTAL,
                        SUM(sub.POINT),
                        ITME_NAME
                    FROM
                        INTEG_PERSON_SUBSIDIARY sub
                            LEFT JOIN
                        integ_rule rul ON rul.id = sub.RULE_ID
                    WHERE
                        sub.year = '2018'
                    GROUP BY rul.id , sub.PARTY_MEMBER_ID;

                '''
    

    def getUpdateSubsidiarySQL(self):
        return  '''
                    UPDATE INTEG_PERSON_SUBSIDIARY 
                    SET 
                        POINT = 0,
                        ISOVERFLOW = 1,
                        STATE = 0,
                        REMARK = '修改置0'
                    WHERE
                        PARTY_MEMBER_ID = '{memberId}'
                            AND rule_id = '{ruleId}'
                            ORDER BY CREATTIME DESC LIMIT {overs};
                '''


    def getUpdateBaseIntegSQL(self):
        return  '''
                    UPDATE integ_base 
                        SET 
                            BASCPOINT = BASCPOINT - {subtraction}
                        WHERE
                            memberid = '{memberId}'
                            AND BASCRULEID = '{ruleFid}';
                '''


    def getUpdatePersonPointSQL(self): 
        return  '''
                    UPDATE integ_person_point 
                        SET 
                            TOTAL = TOTAL - {subtraction}
                        WHERE
                            MEMBERID = '{memberId}';
                '''

    
    def queryBeChangedMember(self,file):
        db =self.getConnection()
        cursor = db.cursor()
        cursor.execute(self.getBeChangedMemberSQL())
        beChanged = cursor.fetchall()
        for member in beChanged:
            rul_id = member[0]
            member_id = member[1]
            rul_fid = member[2]
            overs = member[3]
            subtraction = member[4]
            total = member[5]
            sub_sum = member[6]

            print(rul_id, member_id,rul_fid, overs, subtraction)
            # 初始化单项总分
            basePoint = 0

            # 如果分数超出
            if overs >= 0 :
                # 设置对应规则下的满分单项总分 
                basePoint = total
                # 置空积分明细
                updateSubsidiaryFormatSQL = self.getUpdateSubsidiarySQL().format(memberId = member_id, ruleId = rul_id, overs = int(overs))
                #cursor.execute(updateSubsidiaryFormatSQL)
                file.write(updateSubsidiaryFormatSQL)
            
            else: # 如果没有超出，则把计算得到的总分设置为 单项总分
                basePoint = sub_sum
                
            updateBaseIntegFormatSQL = self.getUpdateBaseIntegSQL().format(memberId = member_id, ruleFid = rul_fid, subtraction = basePoint)
            file.write(updateBaseIntegFormatSQL)
            #cursor.execute(updateBaseIntegFormatSQL)

            updatePersonPointFormatSQL = self.getUpdatePersonPointSQL().format(memberId = member_id, subtraction = basePoint)
            file.write(updatePersonPointFormatSQL)
            #cursor.execute(updatePersonPointFormatSQL)

            db.commit()
     

a = Integ()
t = time.time()
fielName = 'integConifgRefresh' + str(int(t))+ '.sql'
file = open(fielName, 'a', encoding='utf-8')
a.queryBeChangedMember(file)
file.close()


