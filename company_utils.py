import os
import re
import csv
import logging
import threading
import pandas as pd
from flask import jsonify
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


logging.basicConfig(level=logging.DEBUG)

class CompanyTransactions():
    def __init__(self):
        self.transactions=[]
        self.products=[]
        self.dfTransactions=pd.DataFrame(self.transactions)
        self.dfProducts = pd.DataFrame(self.products)

    def loadTrasnsactions(self):
        try:
            path = os.getcwd()+'/transaction'
            files = os.listdir(path)
            temp_transactions=[]
            temp_transactions_df=[]

            for file in files:
                if file[-4:] == '.csv' and re.match(r"^Transaction_*", file):
                    with open(os.path.join(path,file),'r') as transfile:
                        dictData= csv.DictReader(transfile)
                        for row in dictData:
                            temp_dict = dict(row)
                            temp_dict_df = dict()
                            temp_dict['transactionId']= int(temp_dict['transactionId'])
                            temp_dict['productId'] = int(temp_dict['productId'])
                            temp_dict['transactionAmount'] = float(temp_dict['transactionAmount'])
                            temp_dict['transactionDatetime'] = datetime.strptime(temp_dict['transactionDatetime'], "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%f")
                            temp_transactions.append(temp_dict)
                            temp_dict_df.update(temp_dict)
                            temp_dict_df['transactionDatetime'] = pd.to_datetime(temp_dict['transactionDatetime'])
                            temp_transactions_df.append(temp_dict_df)
            self.transactions = temp_transactions
            self.transactions=sorted(self.transactions, key= lambda item:item['transactionId'])
            self.dfTransactions=pd.DataFrame(temp_transactions_df)
            return self.transactions, self.dfTransactions
        except Exception as e:
            logging.error("loading transaction gave following error %s "+str(e))

    def loadProducts(self):
        try:
            path = os.getcwd()+'/product'
            files = os.listdir(path)
            for file in files:
                if file[-4:] == '.csv' and re.match(r"^Product*", file):
                    with open(os.path.join(path,file),'r') as transfile:
                        dictData= csv.DictReader(transfile)
                        for row in dictData:
                            temp_dict = dict(row)
                            temp_dict['productId'] = int(temp_dict['productId'])
                            temp_dict['productName'] = temp_dict['productName']
                            temp_dict['productManufacturingCity'] = temp_dict['productManufacturingCity']
                            self.products.append(temp_dict)
            self.dfProducts = pd.DataFrame(self.products)
            return self.products, self.dfProducts
        except Exception as e:
            logging.error("loading product gave following error %s "+str(e))
    def get_transaction_by_ID(self,Id):
        try:
            if self.transactions:
                start = 0
                end = len(self.transactions)-1
                midIndex = len(self.transactions)//2
                while self.transactions[midIndex]['transactionId'] != Id and start<=end:

                    if Id < self.transactions[midIndex]['transactionId']:
                        end=midIndex-1
                    else:
                        start=midIndex+1
                    midIndex=(start+end)//2

                if self.transactions[midIndex]['transactionId'] == Id:
                    productName = self.get_productName_by_Id(self.transactions[midIndex]['productId'])
                    result_dict = self.transactions[midIndex]
                    del result_dict['productId']
                    result_dict['productName'] = productName
                    return result_dict
                return {"msg": str(Id) + " transactionId not found in data"}
            return {"msg": "Data not found"}
        except IndexError as err:
            logging.error("get transaction by Id gave following error %s " + str(err))
            return jsonify({"error":"Please enter valid value"})
        except Exception as e:
            return jsonify({'error':'Please enter valid trasactionId'})

    def get_productName_by_Id(self,Id):
        try:
            if self.products:
                start = 0
                end = len(self.products)-1
                midIndex = len(self.products)//2
                while self.products[midIndex]['productId'] != Id and start<=end:
                    if Id < self.products[midIndex]['productId']:
                        end=midIndex-1
                    else:
                        start=midIndex+1
                    midIndex=(start+end)//2
                if self.products[midIndex]['productId'] == Id:
                    return self.products[midIndex]['productName']
                return None
            return None
        except Exception as e:
            logging.error("get productName by Id gave following error %s " + str(e))
            return None
    def get_cityName_by_Id(self,Id):
        try:
            if self.products:
                start = 0
                end = len(self.products)-1
                midIndex = len(self.products)//2
                while self.products[midIndex]['productId'] != Id and start<=end:
                    if Id < self.products[midIndex]['productId']:
                        end=midIndex-1
                    else:
                        start=midIndex+1
                    midIndex=(start+end)//2
                if self.products[midIndex]['productId'] == Id:
                    return self.products[midIndex]['productManufacturingCity']
            return None
        except Exception as e:
            logging.error("get cityName by Id gave following error %s " + str(e))
            return None

    def get_product_summary(self,lastNdays):
        try:
            startDate = pd.to_datetime(datetime.now() - timedelta(days=lastNdays))
            endDateTime = pd.to_datetime(datetime.now())
            resultset = self.dfTransactions[(self.dfTransactions['transactionDatetime'] >= startDate) & (self.dfTransactions['transactionDatetime'] <= endDateTime)].groupby(
                'productId')[['transactionAmount']].sum()

            responseArray = []

            for index, row in resultset.iterrows():
                respObj = {}
                respObj['productName'] = self.get_productName_by_Id(index)
                respObj['totalAmount'] = float(row['transactionAmount'])
                responseArray.append(respObj)

            return jsonify({'summary': responseArray})
        except Exception as e:
            logging.error("get product summary gave following error %s " + str(e))
            return jsonify({"error": "something went wrong"})

    def get_citywise_summary(self,lastNdays):
        try:
            startDate = pd.to_datetime(datetime.now() - timedelta(days=lastNdays))
            endDateTime = pd.to_datetime(datetime.now())
            resultset = self.dfTransactions[(self.dfTransactions['transactionDatetime'] >= startDate) & (self.dfTransactions['transactionDatetime'] <= endDateTime)].groupby(
                'productId')[['transactionAmount']].sum()

            responseArray = []

            for index, row in resultset.iterrows():
                respObj = {}
                respObj['cityName'] = self.get_cityName_by_Id(index)
                respObj['totalAmount'] = float(row['transactionAmount'])
                responseArray.append(respObj)


            temp_df = pd.DataFrame(responseArray)

            resultset = temp_df.groupby('cityName')[['totalAmount']].sum()
            responseArray = []
            for index, row in resultset.iterrows():
                respObj = {}
                respObj['cityName'] = index
                respObj['totalAmount'] = row['totalAmount']
                responseArray.append(respObj)
            return jsonify({'summary': responseArray})
        except Exception as e:
            logging.error("get citywise summary gave following error %s " + str(e))
            return jsonify({"error": "something went wrong"})


class ScheduleReloading():
    def __init__(self,duration,function):
        self.duration = duration
        self.function = function
        self.scheduler = BackgroundScheduler(timezone="Asia/Calcutta")

    def start(self):
        self.scheduler.add_job(func=self.function, trigger="interval", seconds=self.duration)
        self.scheduler.start()


    def terminate(self):
        self.scheduler.shutdown()

    def threadCheck(self):
        for thread in threading.enumerate():
            print(thread.name)

