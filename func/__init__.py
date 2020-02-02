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
        
        config_obj: reader = reader(SETTINGS_FILE_PATH, 'Values')
        function_url: str = config_obj.get_value("function_url")
        stock_code_list_service_url: str = config_obj.get_value("stock_code_list_service_url")
        x_functions_key: str = config_obj.get_value("x_functions_key")
    
        # Getting stock code list
        response = requests.request("GET", stock_code_list_service_url)
        stk_codes = json.loads(response.text)
        req_count: int  = 0

        for code in stk_codes:
            logging.info(code["stock"])
            threading.Thread(target=invoke_url, args=(function_url, code["stock"], x_functions_key)).start()
            
            # req_count = req_count + 1

            # if req_count == 100:
            #     time.sleep(5)
            #     req_count = 0
            #     pass
            pass 
    except Exception as ex:
        error_log = '{} -> {}'.format(utc_timestamp, str(ex))
        logging.exception(error_log)
    pass

def invoke_url(url: str, code: str, x_functions_key: str):    
    querystring = {"code": code}
    
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'x-functions-key': x_functions_key
    }

    requests.request("POST", url, headers=headers, params=querystring)
    pass