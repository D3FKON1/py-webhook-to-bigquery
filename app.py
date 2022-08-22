from flask import Flask, request
from google.cloud import bigquery
from datetime import datetime
import os
import json
import pytz

import google.cloud.logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
import logging

app = Flask(__name__)

@app.route('/', defaults={'path': ''}, methods=['POST'])
@app.route('/<path:path>', methods=['POST'])

def root(path):
    # Debug log the IP and Debug log HTTP headers 
    logging.info("New Inbound Webhook from IP: {}".format(request.remote_addr))
    logging.debug("Headers: {}".format(request.headers))

    # Get path of the request and log
    uri = request.full_path.replace("/","")
    uri = uri[:-1]
    logging.info("Endpoint: {}".format(uri))

    # API Key Auth, Return 401 error if not matching
    request_api = request.headers['X-Api-Key']
    if request_api != os.environ['WEBHOOK_APIKEY']:
        error_message = {
            "err_field": "X-Api-Key",
            "err_message": "Invalid Authentication"
            }
        logging.warn("Invalid Authentication: %s {}".format(request.remote_addr))
        return (error_message, 401)
    
    # Content type check and erroring
    request_type = request.headers['Content-Type']
    if request_type != "application/json"
        error_message = {
            "err_field": "Content-Type",
            "err_message": "Unsupported Media Type"
            }
        logging.warn("Unsupported Media Type: {}".format(request.remote_addr))
        return (error_message, 415)  

    #Get json from body of request
    data = request.get_json()

    #Add Endpoint to json data
    data.update({"Endpoint":uri})

    #Add timestamp to json data
    date_now = datetime.now(pytz.timezone('Pacific/Auckland'))
    data.update({"Timestamp":date_now.strftime('%Y-%m-%dT%H:%M:%S.%f%z')})

    # Debug log JSON data to be inserted into database
    logging.info("Inserting row")
    logging.debug(data)

    # Make a BigQuery client object with error handling
    pdt = os.environ['TABLEID']
    client = bigquery.Client()
    errors = client.insert_rows_json(
        pdt, [data], row_ids=[None] * len(data)
        )
    
    # Make the Bigquery insert request.
    if errors == []:
        logging.info('Row Inserted')
        return ('', 200)
    else:
        logging.error("Encountered errors while inserting rows: {}".format(errors))
        inserting_error ={
            "error": "Internal Server Error",
            "err_message": "Error loading data into Database"
            }
        return (inserting_error, 500)

if __name__ == "__main__": app.run()