import logging
from flask import Flask, jsonify
from company_utils import CompanyTransactions, ScheduleReloading



if __name__=='__main__':
    app = Flask(__name__)
    transactions = CompanyTransactions()
    transactions.loadProducts()
    transactions.loadTrasnsactions()

    @app.errorhandler(Exception)
    def handle_bad_request(e):
        return 'bad request!', 404


    @app.route("/assignment/transaction/<transactionId>", methods=["GET"])
    def transac(transactionId):
        try:
            return transactions.get_transaction_by_ID(int(transactionId))
        except ValueError as error:
            return jsonify({"error": "parameter transaction Id : please enter a valid Integer value"})


    @app.route("/assignment/transactionSummaryByProducts/<lastDays>", methods=["GET"])
    def summaryByProduct(lastDays):
        return transactions.get_product_summary(int(lastDays))


    @app.route("/assignment/transactionSummaryByManufacturingCity/<lastDays>", methods=["GET"])
    def summaryByCity(lastDays):
        return transactions.get_citywise_summary(int(lastDays))

    shed= ScheduleReloading(duration=5,function=transactions.loadTrasnsactions)
    shed.start()


    app.run(port=8080, use_reloader=True, debug= True)


