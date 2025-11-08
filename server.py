import json
from flask import Flask, request, jsonify
import requests
import datetime

from sql_interface import *

APPROVED_DB_COLUMN_NAMES = "Barcode VARCHAR(255), Name VARCHAR(255), PRIMARY KEY (Barcode)"
STORE_DB_COLUMN_NAMES = "StoreID VARCHAR(255), Name VARCHAR(255), PRIMARY KEY (StoreID)"

DB_HOST = "localhost"
DB_NAME = "hacks2025"
DB_USERNAME = "root"
DB_PASSWORD = "root"

class Server:
    def __init__(self, host='0.0.0.0', port=5000):
        """
        Initializes the backend server.
        """
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        self.last_data_received = None
        self.setup_routes()
        
        self.dbase = SQLInterface(DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD)

    def setup_routes(self):
        @self.app.route('/')
        def health_check():
            return {"status": "ok", "message": "WebServer is running."}
        
        @self.app.route('/api/receive', methods=['POST'])
        def handle_receive_and_process():
            try:
                # RECEIVE THE COMMAND
                data = request.get_json()
                if not data:
                    return {"error": "No JSON data provided"}, 400

                # Get the command from the JSON, or 'none' if it's not provided
                command = data.get("command", "none")

                # get approved items from store
                if command == "get_appr":
                    # This command stores a payload
                    storeid = data.get("StoreID")
                    self.last_data_received = storeid
                    
                    response_data = self.dbase.get_table_as_json_payload("store_" + storeid)
                    status_code = 200
                # get list of stores
                elif command == "get_stores":
                    response_data = self.dbase.get_table_as_json_payload("stores")
                    status_code = 200
                
                # check if item is covered in store
                elif command == "covered?":
                    code = data.get("Barcode")
                    storeid = data.get("StoreID")
                    tablename = "store_" + storeid
                    print(f"querying from {tablename} with code {code}")
                    resp = self.dbase.query(tablename, "Name", "Barcode", code)
                    print(f"Response: {resp}")
                    if resp is not None:
                      response_data = {
                        "Response": "Yes",
                        "Barcode": code,
                        "Name": resp
                      }
                    else:
                      match = self.dbase.query("eanref", "product_name", "code", code)
                      if match is not None:
                        response_data = {
                          "Response": "No",
                          "Barcode": code,
                          "Name": match
                        }
                      else:
                        response_data = {
                          "Response": "No",
                          "Barcode": "None",
                          "Name": "None"
                        }
                    status_code = 200
                else:
                    self.last_data_received = data
                    response_data = {
                        "message": "Received unknown command or data"
                    }
                    status_code = 200
                
                return response_data, status_code
            
            except Exception as e:
                return {"error": f"Invalid request: {e}"}, 400
        
        @self.app.route('/api/data', methods=['GET'])
        def get_data():
            if self.last_data_received is None:
                return {"message": "No data has been stored yet."}, 200
            
            return {
                "status": "ok",
                "last_stored_data": self.last_data_received
                }, 200
        
        @self.app.route('/api/send', methods=['POST'])
        def send_data():
            try:
                data = request.get_json()
                if not data or 'url' not in data or 'payload' not in data:
                    return {"error": "Request must include 'url' and 'payload' keys"}, 400

                target_url = data['url']
                payload = data['payload']

                print(f"Sending data to {target_url}...")
                response = requests.post(target_url, json=payload)
                response.raise_for_status() 

                return {
                    "status": "success",
                    "message": f"Successfully sent data to {target_url}.",
                    "remote_server_status_code": response.status_code
                }, 200

            except requests.exceptions.RequestException as e:
                return {"status": "error", "message": f"Failed to send data: {e}"}, 500
            except Exception as e:
                return {"error": f"Invalid request: {e}"}, 400

    def run(self):
        """
        Starts the Flask web server.
        """
        print(f"Starting isolated WebServer on http://{self.host}:{self.port}")
        self.dbase.connect()
        self.app.run(host=self.host, port=self.port, debug=True)
         
if __name__ == "__main__":
  server = Server()
  server.run()