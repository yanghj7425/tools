import os
import string
import json


class Terminal:
    def __init__(self):
        self.file_path = os.getcwd(
        ) + '\\terminalExam\\VOSClim_GTS_nov_2018.txt'

    # 读取源文件
    def read_source_file(self):
        file = open(self.file_path, 'r')
        lines = []
        for line in file:
            each_line = line.strip()
            lines.append(each_line)
        file.close()
        return lines

    #处理船信息
    def parse_ship_info(self, ship_info):
        arr = []
        zero_arr = '0000'
        for each_line in ship_info:
            ship_json = {}
            # 年
            year = each_line[0:4]

            # 月
            month = zero_arr[0:2 - len(each_line[4:6].
                                       strip())] + each_line[4:6].strip()

            # 日
            day = zero_arr[0:2 - len(each_line[6:8].
                                     strip())] + each_line[6:8].strip()

            # 时
            hour = zero_arr[0:4 - len(each_line[8:12].
                                      strip())] + each_line[8:12].strip()

            #组装时间： 4位年 2位月 2位日 4位时
            ship_json['time'] = int(year + month + day + hour)

            # 纬度
            ship_json['latitude'] = int(each_line[12:18].strip()) / 100

            # 经度
            ship_json['longitude'] = int(each_line[18:23].strip()) / 100

            # 船名
            ship_json['name'] = each_line[34:].strip()

            arr.append(ship_json)

        return arr

    # 获取所有船只的名称
    def get_ship_name(self, ship_info):
        ship_arr_name = []
        for each_line in ship_info:
            if each_line.get('name') not in ship_arr_name:
                ship_arr_name.append(each_line.get('name'))

        return ship_arr_name

    # 获取指定船只信息，并按时间升序排序
    def get_sorted_ship_info(self, ship_info_arr, will_sorted_name):
        sorted_ship_info = []
        for each_ship in ship_info_arr:
            if each_ship.get('name') == will_sorted_name:
                sorted_ship_info.append(each_ship)
        sorted_ship_info.sort(key=lambda k: (k.get('time', 0)), reverse=False)
        return sorted_ship_info


minal = Terminal()
lines = minal.read_source_file()

ship_info_arr = minal.parse_ship_info(lines)

# ZDLS1
will_sorted_name = 'ZDLS1'
sorted_info = minal.get_sorted_ship_info(ship_info_arr, will_sorted_name)
for info in sorted_info:
    print(info)

# ship_name_arr = minal.get_ship_name(ship_info_arr)

# print(ship_name_arr)

# print(len(ship_info_arr))
# for each_info in ship_info_arr:
#     print(each_info)
