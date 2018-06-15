# Тестовое задание

import os, argparse
from datetime import datetime
import numpy as np
import http.client
import json

class Loader:
    def __init__(self, **args):
        self.__sourceType = args.get("type") # f-file, s-server
        if (self.__sourceType == None):
            self.__sourceType = "f"

        self._source = args.get("source")
        if (self._source == None):
            self._source = "log.csv"

        self._region = args.get("region")
        if (self._region == None):
            self._region = 113
        self._region = int(self._region)

        self._depth = args.get("depth")
        if (self._depth == None):
            self._depth = 5
        self._depth = int(self._depth)

        self._data = {}
        self.load_data()

    def __del__(self):
        pass

    def load_data(self):
        pass

    def get_response(self):
        pass

class LoaderFromFile(Loader):
    def __del__(self):
        self._source.close()

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
            line = self._source.readline().decode('utf8')
            if (line == ''):
                return line
        except IOError:
            print("Ошибка чтения файла")

        self.__parse_line(line)

        return self.__line.copy()

    def load_data(self):
        try:
            self._source = open(self._source, 'rb')

            headerReadLine = self._source.readline().decode('utf8')
            keyArr = headerReadLine.split(',')
            self.__line = {key.rstrip(): 0 for key in keyArr}

        except IOError:
            print("Ошибка чтения файла при инициализации")

        line = self.__get_next_line()

        while (line != ''):
            label = line["label"]
            labelArr = self._data.get(label)

            line.pop("label")
            if (labelArr != None):
                labelArr.append(line)
            else:
                self._data.update({label: []})
                self._data.get(label).append(line)

            line = self.__get_next_line()

    def get_response(self):
        metrics = []

        for key in self._data:
            labelData = self._data.get(key)

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

class LoaderFromServer(Loader):
    def load_data(self):
        self._source = http.client.HTTPSConnection(self._source)
        self._data = []

        headers = {"User-Agent": "api-test-agent"}
        for i in range(self._depth):
            body = json.dumps({"text": "java", "area": self._region, "page": i})
            self._source.request("GET","/vacancies", body, headers)
            self._data.append(json.loads(self._source.getresponse().read()))

    def get_response(self):
        result = []

        for i in range(len(self._data)):
            result += self._data[i]["items"]

        return result


class Handler:
    def __init__(self, loader, printer):
        self.__loader = loader
        self.__printer = printer

    def __del__(self):
        self.__loader.__del__()
        self.__printer.__del__()

    def fout(self):
        resultData = self.__loader.get_response()#self.__loader.get_metrics()
        self.__printer.out_to_file(resultData)


class Printer:
    def __init__(self, fileOutName=""):
        self._file = ""

        if (fileOutName != ""):
            try:
                self._file = open(fileOutName, 'a')
            except IOError:
                print("Ошибка записи в файл при инициализации")

    def __del__(self):
        if (self._file):
            self._file.close()

class PrinterFromFile(Printer):
    def out_to_file(self, data):
        try:
            headerWriteLine = "label,nCalls,average,ninetyPerl,ninetyFivePerl,ninetyNinePerl,hundredPerl;\n"
            self._file.write(headerWriteLine)

            for elem in data:
                line = ','.join([str(e) for e in elem]) + ";\n"
                self._file.write(line)
        except IOError:
            print("Ошибка записи в файл")

class PrinterFromServer(Printer):
    def out_to_file(self, data):
        try:
            flag = True
            for elem in data:
                if (flag):
                    headerWriteLine = ','.join([str(e) for e in elem]) + ";\n"
                    self._file.write(headerWriteLine)
                    flag = False

                line = ','.join([str(elem[e]) for e in elem]) + ";\n"
                self._file.write(line)
        except IOError:
            print("Ошибка записи в файл")


def __main__():
    #os.environ['HTTP_PROXY']='80.250.174.240:3128'
    os.environ['LANG'] = 'ru_RU.UTF-8' and 'en_US.UTF-8' and 'window-1251'

    cmdStr = argparse.ArgumentParser()
    cmdStr.add_argument('-type', type=str, help='Тип запроса (файл или api сервера hh, по умолчанию - файл)')
    cmdStr.add_argument('-source', type=str, help='Источник (имя файла или сервера, по умолчанию - файл log.csv)')
    cmdStr.add_argument('-region', type=str, help='Код региона в api.hh.ru (по умолчанию - Россия, 113)')
    cmdStr.add_argument('-depth', type=str, help='Глубина поиска (кол-во страниц ответа сервера, по умолчанию - 5)')
    args = cmdStr.parse_args()
    args.out = "out_" + str(datetime.now().strftime("%Y-%m-%d-%H.%M.%S")) + ".csv"
    print(args.out)

    if (args.type == "s"):
        printer = PrinterFromServer(args.out)
        loader = LoaderFromServer(type=args.type, source=args.source, region=args.region, depth=args.depth)
    else:
        printer = PrinterFromFile(args.out)
        loader = LoaderFromFile(type=args.type, source=args.source)

    handler = Handler(loader, printer)

    handler.fout()

if __name__ == '__main__':
    __main__()
