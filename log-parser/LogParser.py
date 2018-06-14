# Тестовое задание

import os, argparse
from datetime import datetime
import numpy as np

class Loader:
    def __init__(self, fileInName=""):
        self.__file = ""
        self.__data = {}

        if (fileInName != ""):
            try:
                self.__file = open(fileInName, 'rb')

                headerReadLine = self.__file.readline().decode('utf8')
                keyArr = headerReadLine.split(',')
                self.__line = {key.rstrip(): 0 for key in keyArr}

            except IOError:
                print("Ошибка чтения файла при инициализации")

        self.__load_data()

    def __del__(self):
        if (self.__file):
            self.__file.close()

    def __parse_line(self, line):
        tmpArr = line.split(',')

        self.__line.update({
            "elapsed": int(tmpArr[1]),
            "label": tmpArr[2],
            "responseCode": int(tmpArr[3]),
            "success": bool(tmpArr[7])
        })

    def __get_next_line(self):
        try:
            line = self.__file.readline().decode('utf8')
            if (line == ''):
                return line
        except IOError:
            print("Ошибка чтения файла")

        self.__parse_line(line)


        return self.__line.copy()

    def __load_data(self):
        line = self.__get_next_line()

        while (line != ''):
            label = line["label"]
            labelArr = self.__data.get(label)

            line.pop("label")
            if (labelArr != None):
                labelArr.append(line)
            else:
                self.__data.update({label: []})
                self.__data.get(label).append(line)

            line = self.__get_next_line()

    def get_metrics(self):
        metrics = []

        for key in self.__data:
            labelData = self.__data.get(key)

            nLabelData = len(labelData)
            elapsedArr = []
            for i in range(nLabelData):
                if (labelData[i]["responseCode"] == 200 and labelData[i]["success"] == True):
                    elapsedArr.append(labelData[i]["elapsed"])

            elapsedArr = np.array(elapsedArr)
            outLabelRes = []

            outLabelRes.append(key)                            # метод БЛ
            outLabelRes.append(elapsedArr.size)                # кол-во вызовов
            outLabelRes.append(np.mean(elapsedArr))            # среднее
            outLabelRes.append(np.percentile(elapsedArr, 90))  # 90 перцентиль
            outLabelRes.append(np.percentile(elapsedArr, 95))  # 95 перцентиль
            outLabelRes.append(np.percentile(elapsedArr, 99))  # 99 перцентиль
            outLabelRes.append(np.percentile(elapsedArr, 100)) # 100 перцентиль

            metrics.append(outLabelRes)

        return metrics

class Handler:
    def __init__(self, loader, printer):
        self.__loader = loader
        self.__printer = printer

    def __del__(self):
        self.__loader.__del__()
        self.__printer.__del__()

    def fout(self):
        metrics = self.__loader.get_metrics()
        self.__printer.out_to_file(metrics)

class Printer:
    def __init__(self, fileOutName=""):
        self.__file = ""

        if (fileOutName != ""):
            try:
                self.__file = open(fileOutName, 'a')
                headerWriteLine = "label,nCalls,average,ninetyPerl,ninetyFivePerl,ninetyNinePerl,hundredPerl;\n"
                self.__file.write(headerWriteLine)
            except IOError:
                print("Ошибка записи в файл при инициализации")

    def __del__(self):
        if (self.__file):
            self.__file.close()

    def out_to_file(self, data):
        try:
            for elem in data:
                line = ','.join([str(e) for e in elem]) + ";\n"
                self.__file.write(line)
        except IOError:
            print("Ошибка записи в файл")


def __main__():
    #os.environ['HTTP_PROXY']='80.250.174.240:3128'
    os.environ['LANG'] = 'ru_RU.UTF-8' and 'en_US.UTF-8' and 'window-1251'

    cmdStr = argparse.ArgumentParser()
    cmdStr.add_argument('-log', type=str, help='Лог-файл')
    args = cmdStr.parse_args()
    args.out = "out_" + str(datetime.now().strftime("%Y-%m-%d-%H.%M.%S")) + ".csv"
    print(args.out)

    loader = Loader(args.log)
    printer = Printer(args.out)
    handler = Handler(loader,printer)

    handler.fout()

if __name__ == '__main__':
    __main__()
