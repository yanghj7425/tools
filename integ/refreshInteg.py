import pymysql as mysql
import integ

import traceback
import time
import datetime


class refreshInteg:
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

    # 获取需要检查积分的党员ID
    def getCheckMemberId(self):
        return '''
                    SELECT 
                        PARTY_MEMBER_ID
                    FROM
                        integ_person_subsidiary
                    GROUP BY PARTY_MEMBER_ID;
                '''


    def getErrorIntegMemeberSQL(self):
        return '''
                    SELECT 
                        CALC_SUM, BASCPOINT,  C.TOTAL, BASCRULEID, MEMBERID
                    FROM
                        (SELECT 
                            SUM(A.POINT) AS CALC_SUM,
                                R.TOTAL,
                                A.PARTY_MEMBER_ID AS MEMBERID,
                                R.FID
                        FROM
                            (SELECT 
                            PARTY_MEMBER_ID, RULE_ID, POINT
                        FROM
                            integ_person_subsidiary
                        WHERE
                            PARTY_MEMBER_ID = '{memberId}'
                                AND YEAR = '2018') A
                        LEFT JOIN integ_rule R ON A.RULE_ID = R.ID
                        GROUP BY R.FID , A.PARTY_MEMBER_ID) C
                            LEFT JOIN
                        (SELECT 
                            BASCPOINT, BASCRULEID
                        FROM
                            integ_base
                        WHERE
                            MEMBERID = '{memberId}'
                                AND year = '2018') D ON C.FID = D.BASCRULEID
                    WHERE
                        CALC_SUM != BASCPOINT;
               '''



    def getOverFlowSubsidiarySQL(self):
        return '''
                   SELECT 
                        R.ID,
                        R.FID,
                        R.TOTAL,
                        A.PARTY_MEMBER_ID,
                        A.CALC_SUM,
                        A.SHOULDPOINT,
                        IFNULL((A.CALC_SUM - R.TOTAL) / A.SHOULDPOINT,
                                0) AS overs
                    FROM
                        (SELECT 
                            PARTY_MEMBER_ID,
                                RULE_ID,
                                SUM(POINT) AS CALC_SUM,
                                SHOULDPOINT
                        FROM
                            (SELECT 
                            PARTY_MEMBER_ID, RULE_ID, ISOVERFLOW, POINT, SHOULDPOINT
                        FROM
                            integ_person_subsidiary
                        WHERE
                            PARTY_MEMBER_ID = '{memberId}'
                                AND YEAR = 2018) A
                        GROUP BY PARTY_MEMBER_ID , RULE_ID) A
                            INNER JOIN
                        (SELECT 
                            ID, FID, TOTAL
                        FROM
                            integ_rule
                        WHERE
                            FID = '{base_ruleId}') R ON R.ID = A.RULE_ID;
                '''

    def getIntegBaseSQL(self):

        return '''
                    SELECT 
                        BASCPOINT
                    FROM
                        integ_base
                    WHERE
                        year = '2018'
                            AND MEMBERID = '{memberId}'
                            AND BASCRULEID = '{level2Id}';
                '''

    def getUpdateSubsidiarySQL(self):
        return '''
                    UPDATE INTEG_PERSON_SUBSIDIARY 
                    SET 
                        POINT = 0,
                        ISOVERFLOW = 1,
                        STATE = 0,
                        REMARK = '修改置0'
                    WHERE
                        PARTY_MEMBER_ID = '{memberId}'
                            AND rule_id = '{ruleId}'
                            AND year = '2018'
                            ORDER BY CREATTIME DESC LIMIT {overs};
                '''

    def getUpdateBaseIntegSQL(self):
        return '''
                    UPDATE integ_base 
                        SET 
                            BASCPOINT = BASCPOINT + {subtraction}
                        WHERE
                            memberid = '{memberId}'
                            AND BASCRULEID = '{ruleFid}'
                            AND YEAR = 2018;
                '''

    def getUpdatePersonPointSQL(self):
        return '''
                    UPDATE integ_person_point 
                        SET 
                            TOTAL = TOTAL + {subtraction}
                        WHERE
                            MEMBERID = '{memberId}'
                            AND YEAR = 2018;
                '''


    # 获取现在该党员数据库的总分
    def getIntegPersonPoint(self):
        return  '''         
                    SELECT 
                        TOTAL
                    FROM
                        integ_person_point
                    WHERE
                        MEMBERID = '{memberId}'
                            AND YEAR = '2018';
                '''





    # 处理冗余的积明细
    def doCalcEachMemberIntegSubsidiary(self, file, overFlowSubs, cursor):
    
        for overFlowSub in overFlowSubs:
            #规则明细ID
            rId = overFlowSub[0]
            #党员 ID
            aMemberId = overFlowSub[3]
            #多出的明细数量
            overs = overFlowSub[6]
            # 如果分数超出
            if int(float(overs)) > 0:
                # 置空积分明细
                updateSubsidiaryFormatSQL = self.getUpdateSubsidiarySQL().format(memberId=aMemberId, ruleId=rId, overs=int(float(overs)))
                # cursor.execute(updateSubsidiaryFormatSQL)
                file.write(updateSubsidiaryFormatSQL)
            
    def doCalcIntegBase(self, file, cursor, overFlowSubs):
        print(overFlowSubs)

        for overFlowSub in  overFlowSubs:
            #规则二级 ID
            rFid = overFlowSub[1]
            print(rFid)
            #党员 ID
            memberId = overFlowSub[3]  
            queryIntegBaseSQL = self.getIntegBaseSQL().format(memberId = memberId, level2Id= rFid)     
            cursor.execute(queryIntegBaseSQL)
            integBases = cursor.fetchall()

            for integBase in integBases:
                print (integBase)

    #查询积分出错的党员
    def queryErrorIntegMemeber(self, memberId, cursor):
        
        queryErrorIntegMemeberSQL=self.getErrorIntegMemeberSQL().format(memberId=memberId)
        cursor.execute(queryErrorIntegMemeberSQL)   
        return cursor.fetchall()



    def queryBeChangedMember(self, file):
        db = self.getConnection()
        cursor = db.cursor()

        ## 获取需要检查积分的党员ID
        cursor.execute(self.getCheckMemberId())
        memberIds = cursor.fetchall()

        for memberLoop in memberIds:

            # 具体的党员ID
            memberId = memberLoop[0]

            # 获取该党员出积分项
            IntegMembers = self.queryErrorIntegMemeber(memberId, cursor)
            if IntegMembers:

                
                for eMember in IntegMembers:

                    beAddInteg = 0.0

                    # 根据积分明细算出该党员的总分
                    calc_aum = float(eMember[0])
                    # 现在的总分
                    base_point = float(eMember[1])
                    # 该项满分
                    cTotal = float(eMember[2]) 
                    # 二级 ID
                    base_ruleId = eMember[3]
                    # 党员 ID
                    cmemberId = eMember[4]

                    if calc_aum != base_point:

                        #查询溢出的明细
                        queryOverflowSQL=self.getOverFlowSubsidiarySQL().format(memberId = cmemberId, base_ruleId = base_ruleId)
                        cursor.execute(queryOverflowSQL)
                        overFlowSubs = cursor.fetchall()
                        # 处理冗余的积分明细
                        self.doCalcEachMemberIntegSubsidiary(file, overFlowSubs, cursor)
                        
                        # 如果该项明细总和大于现在该党员的积分
                        if calc_aum > base_point:
                            # 如果大于单项总分
                            if calc_aum > cTotal:
                                beAddInteg = beAddInteg + cTotal - base_point

                            else:
                                beAddInteg = beAddInteg + calc_aum -base_point

                            print('large：',beAddInteg)
                        elif calc_aum < base_point:
                            beAddInteg = beAddInteg + calc_aum -  base_point

                            print('lower：', beAddInteg)
                        
                        ## 更新Base分数
                        updatebaseIntegSQL =  self.getUpdateBaseIntegSQL().format(subtraction = beAddInteg, memberId = cmemberId, ruleFid = base_ruleId)

                        file.write(updatebaseIntegSQL)
                         ## 更新 point
                        updatePersonPointSQL = self.getUpdatePersonPointSQL().format(subtraction = beAddInteg, memberId = cmemberId)
                        file.write(updatePersonPointSQL)
                        #print(calc_aum, base_point, cTotal, base_ruleId, memberId)
                      
                
            
                   



                    



                   
           # else:
                #print('no error {memberId}',memberId)
           


        # 处理完一个党员的数据

        db.commit()


a = refreshInteg()
t = time.time()
fielName = 'refreshInteg' + str(int(t)) + '.sql'
file = open(fielName, 'a', encoding='utf-8')
a.queryBeChangedMember(file)
file.close()
