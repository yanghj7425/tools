import os
import string
import json
import matplotlib.pyplot as plt


class Terminal:
    """Description for Terminal
    
    初始化是要时设置文件存储的路径
        file_path: 文本文件存储路径 

    函数:
        read_source_file(self)  # 读取 文本文件数据
        parse_ships_info(self, ships_info) # 解析文本文件内的数据
        get_ships_name(self, ships_info) # 获取文本文件中的所有船只
        get_sorted_ships_info(self, be_sorted_name, ships_info) # 得到指定船只的数据，并按时间升序排序
        show_broken_view(self, be_show_ships, ships_info) # 显示船只信息的折线图
        show_time_statistics(self, time_zone, ships_info) # 显示时段轨迹点数
    """

    def __init__(self):
        self.file_path = os.getcwd(
        ) + '\\terminalExam\\VOSClim_GTS_nov_2018.txt'
        plt.rcParams['font.sans-serif'] = ['SimHei']  #用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  #用来正常显示负号

    # 读取源文件
    def read_source_file(self):
        '''
        文档注释
        
        Description
            读取文本文件的数据

            Return
                lines: 读取到的所有行，格式 ['']

        '''
        file = open(self.file_path, 'r')
        lines = []
        for line in file:
            each_line = line.strip()
            lines.append(each_line)
        file.close()
        return lines

    #处理船信息
    def parse_ships_info(self, ships_info):
        '''
        文档注释
        
        Description
              处理船信息

            Args
                ship_info：需要解析的船只信息，格式：[{}]

            Return
                arr: 处理好的数据，格式：[{}]

        '''
        arr = []
        zero_arr = '0000'
        for each_line in ships_info:
            ship_json = {}
            # 年
            year = each_line[0:4]

            # 月
            month = zero_arr[0:2 - len(each_line[4:6]
                                       .strip())] + each_line[4:6].strip()

            # 日
            day = zero_arr[0:2 - len(each_line[6:8]
                                     .strip())] + each_line[6:8].strip()
            # 时
            hour = zero_arr[0:4 - len(each_line[8:10]
                                      .strip())] + each_line[8:10].strip()

            ship_json['hour'] = int(hour)
            #分
            minute = zero_arr[0:2 - len(each_line[10:12]
                                        .strip())] + each_line[10:12].strip()

            #组装时间： 4位年 2位月 2位日 4位时
            ship_json['time'] = int(year + month + day + hour + minute)

            # 纬度
            ship_json['latitude'] = int(each_line[12:18].strip()) / 100

            # 经度
            ship_json['longitude'] = int(each_line[18:23].strip()) / 100

            # 船名
            ship_json['name'] = each_line[33:].strip()

            arr.append(ship_json)

        return arr

    # 获取所有船只的名称
    def get_ships_name(self, ships_info):
        '''
        文档注释
        
        Description
              获取所有船只的名称

            Args
                ships_info：所有船只信息的数据集，格式: [{}]

            Return
                ships_name: 处理好的数据，格式：['']

        '''
        ships_name = []
        for each_line in ships_info:
            if each_line.get('name') not in ships_name:
                ships_name.append(each_line.get('name'))

        return ships_name

    # 获取指定船只信息，并按时间升序排序
    def get_sorted_ships_info(self, be_sorted_name, ships_info):
        '''
        文档注释

        Description
            获取指定船只信息，并按时间升序排序

        Args
            be_sorted_name：被筛选的船只名称，格式：''
            ships_info: 所有船只信息的数据集，格式：[{}]

        Return
            sorted_ships_info: 给定船只，按时间升序排序的数据，格式：[{}]

        '''
        sorted_ships_info = []
        for each_ship in ships_info:
            if each_ship.get('name') == be_sorted_name:
                sorted_ships_info.append(each_ship)
        # 排序
        sorted_ships_info.sort(key=lambda k: (k.get('time', 0)), reverse=False)
        return sorted_ships_info

    def show_broken_view(self, be_show_ships, ships_info):
        '''
        文档注释
        Description
            时段轨迹点数  

        Args
            be_show_ships: 需要显示折线图的船只名称，格式:['']
            ships_info: 对应的船只信息，格式:[{}]
        '''
        lines_color = ['#ADFF2F', '#1E90FF']
        lines_marker = ['.', ',']
        plt.xlabel('longitude')
        plt.ylabel('latitude')
        plt.title(be_show_ships[0] + ',' + be_show_ships[1] + '航行路线图')

        for name in be_show_ships:
            sorted_ships_info = self.get_sorted_ships_info(name, ships_info)
            x = []
            y = []
            for each_info in sorted_ships_info:
                x.append(each_info['longitude'])
                y.append(each_info['latitude'])
            # print('x:', x)
            # print('y:', y)
            plt.plot(
                x,
                y,
                color=lines_color[be_show_ships.index(name)],
                label=name,
                marker=lines_marker[be_show_ships.index(name)],  # 显示标签
                linewidth=2.0)
            plt.legend(loc='best')  # 显示图例

    def show_time_statistics(self, time_zone, ships_info):
        '''
        文档注释
        Description

        Args
            time_zone: 时段,格式:[int]
            ships_info: 对应的船只信息，格式:[{}]
        '''
        if len(time_zone) != 2:
            print('传入时间不合法')
            os._exit(0)

        for time in time_zone:
            if not time >= 0 and time <= 24:
                print('传入时间不合法')
                os._exit(0)

        times_arr = [i for i in range(time_zone[0], time_zone[1] + 1)]
        counts_arr = [0 for i in range(time_zone[0], time_zone[1] + 1)]

        plt.xlabel('时')
        plt.ylabel('次数')
        plt.title('时段轨迹点数')

        for ship_info in ships_info:
            hour = ship_info['hour']
            if hour in times_arr:
                counts_arr[times_arr.index(
                    hour)] = counts_arr[times_arr.index(hour)] + 1

        plt.plot(times_arr, counts_arr, marker='o')
        plt.xticks(times_arr)
        # print(times_arr)
        # print(counts_arr)

    def showPanl(self, ships_info):

        fig = plt.figure(figsize=(9, 7))
        fig.add_subplot(221)  #2行2列第1幅图
        will_sorted_name = ['1XJBH', '1XJAB']
        minal.show_broken_view(will_sorted_name, ships_info)  # 显示折线图

        fig.add_subplot(222)  #2行2列第2幅图
        time_zone = [0, 23]  # 小时
        minal.show_time_statistics(time_zone, ships_info)  # 时段轨迹点数
        plt.show()


minal = Terminal()
lines = minal.read_source_file()

ships_info = minal.parse_ships_info(lines)

ships_name = minal.get_ships_name(ships_info)

minal.showPanl(ships_info)

# ZDLS1
# will_sorted_name = ['1XJBH', '1XJAB']

# minal.show_broken_view(will_sorted_name, ships_info)  # 显示折线图

# time_zone = [0, 23]  # 小时
# minal.show_time_statistics(time_zone, ships_info)  # 时段轨迹点数
