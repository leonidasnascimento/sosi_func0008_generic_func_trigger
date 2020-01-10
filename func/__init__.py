import datetime
import logging
import azure.functions as func
import json
import pathlib
import threading
import time
import array
import requests

from configuration_manager.reader import reader

SETTINGS_FILE_PATH = pathlib.Path(
    __file__).parent.parent.__str__() + "//local.settings.json"

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    try:
        logging.info("Timer job 'sosi_func0008_generic_func_trigger' has begun")

        url_query_str: str = "{}/?code={}"        
        config_obj: reader = reader(SETTINGS_FILE_PATH, 'Values')
        function_url: str = config_obj.get_value("function_url")
        stock_code_list_service_url: str = config_obj.get_value("stock_code_list_service_url")
        x_functions_key: str = config_obj.get_value("x_functions_key")
    
        # Getting stock code list
        response = requests.request("GET", stock_code_list_service_url)
        stk_codes = json.loads(response.text)

        for code in stk_codes:
            if not code:
                continue

            logging.info(code["stock"])
            function_url_aux: str = url_query_str.format(function_url, code["stock"])

            threading.Thread(target=start_function, args=(function_url_aux,x_functions_key,)).start()
            pass 
    except Exception as ex:
        error_log = '{} -> {}'.format(utc_timestamp, str(ex))
        logging.exception(error_log)
    pass

def start_function(url: str, x_functions_key: str):    
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'x-functions-key': x_functions_key,
        'content-length': str(len(str(json).encode('utf-8')))
    }

    requests.request("POST", url, headers=headers)
    pass