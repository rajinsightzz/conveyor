import os
import logging
import time
import xml.etree.ElementTree as ET
import datetime
from logging import handlers
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import paho.mqtt.client as mqtt
import pymysql
import traceback
import json
import threading
''' UI Process ID and Base Code Path '''
BASE_PATH = os.getcwd()
LAST_CLEANED_FILE = os.path.join(BASE_PATH, 'last_cleaned_hour.txt')

debugMode=True
log_name=os.path.basename(__file__[:-2])+"log"
logger=logging.getLogger(log_name[:-4])
if debugMode==True:
    log_level=logging.DEBUG
else:
    log_level=logging.ERROR

logger.setLevel(log_level)
log_format=logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log_fl=handlers.RotatingFileHandler(log_name,maxBytes=104857600,backupCount=3) # 1MB log files max
log_fl.setFormatter(log_format)
log_fl.setLevel(log_level)
logger.addHandler(log_fl)
logger.info("PLC Cpmmunication Module Initialized")

configHashMap = {}

def loadConfiguration():
    global configHashMap
    try:
        config_file_path = os.path.join(os.path.curdir,"plc_config.xml")
        config_parse = ET.parse(config_file_path)
        config_root = config_parse.getroot()
           
        configHashMap["DB_USER"] = config_root[0][0].text
        configHashMap["DB_PASS"] = config_root[0][1].text
        configHashMap["DB_HOST"] = config_root[0][2].text
        configHashMap["DB_NAME"] = config_root[0][3].text
        
        configHashMap["PLC_CONVEYOR_IP"] = config_root[1][0].text
        configHashMap["CONVEYOR_STATUS_REGISTER"] = config_root[1][1].text
        configHashMap["CLEANING_MECHANISM_1"] = float(config_root[1][2].text)
        configHashMap["LIGHT_ON_REGISTER"] = config_root[1][3].text
        configHashMap["CLEANING_MECHANISM_2"] = config_root[1][4].text
        configHashMap["DISTANCE_REGISTER"] = int(config_root[1][5].text)

        # Read MQTT details
        configHashMap['MQTT_BROKER'] = config_root[2][0].text
        configHashMap['MQTT_PORT'] = int(config_root[2][1].text)
        configHashMap['MQTT_TOPIC'] = config_root[2][2].text
        

    except Exception as e:
        logger.error(f"loadConfiguration() Exception is {e}")
 
def initClient():
    global configHashMap
    client = None
    try:
        client = ModbusTcpClient(configHashMap.get("PLC_CONVEYOR_IP"), port=502, timeout=3)
        client.connect()
    except Exception as e:
        logger.error(f"initClient() Exception is {e}")
    return client

def writeToPlc(register, val, client):
    try:
        client.write_register(address = int(register),value=val)
    except Exception as e:
        logger.error(f"writeToPlc() Exception is {e}")
# this is old code..(do not remove this)
# def readFromPlc(register, client):    
#     value=-1
#     try:
#         result = client.read_holding_registers(address =int(register) ,count=2)  
#         value=result.getRegister(0)
#     except Exception as e:
#         logger.error(f"readFromPLC() Exception is {e}")
#     return value

def readFromPlc(register, client):    
    value=-1
    try:
        response = client.read_holding_registers(address =int(register) ,count=2) 
        if response.isError():
            print("Error reading Modbus registers:", response)
        else:
            print("Raw Registers:", response.registers)  # Debugging step

            # Decode based on expected data type
            decoder = BinaryPayloadDecoder.fromRegisters(response.registers, byteorder=Endian.BIG, wordorder=Endian.LITTLE)

            # Example for different data types:
            value = abs(decoder.decode_16bit_int())  # For signed 16-bit integer
            # value1 = decoder.decode_16bit_uint()  # For unsigned 16-bit integer
            # value2 = decoder.decode_32bit_int()  # For signed 32-bit integer (if reading 2 registers)
            # value3 = decoder.decode_32bit_float()  # For float (if reading 2 registers)

            print("Decoded Value:", value)
        # value=result.getRegister(0)
    except Exception as e:
        logger.error(f"readFromPLC() Exception is {e}")
        print(f"readFromPLC() Exception is {e}")
    return value
''' Database Function Start'''
def getDatabaseConnection():
    global configHashMap
    dbConnection = None
    try:
        dbConnection = pymysql.connect(
            host = configHashMap.get("DB_HOST"),
            user = configHashMap.get("DB_USER"), 
            passwd = configHashMap.get("DB_PASS"),
            db = configHashMap.get("DB_NAME"))
    except Exception as e:
        logger.critical("getDatabaseConnection() Exception is : "+ str(e))
        logger.critical(traceback.format_exc())
        raise Exception(f"raise from getDatabaseConnection() {e}")
    return dbConnection

def load_last_cleaned_hour():
    """Load the last cleaned hour from a file. Create the file if it doesn't exist."""

    if os.path.exists(LAST_CLEANED_FILE):
        with open(LAST_CLEANED_FILE,'r') as file:
            try:
                last_cleaned_hour = int(file.read().strip())
            except Exception as e:
                last_cleaned_hour = -1
                logger.error(f"load_last_cleaned_hour() Exception is {e}")

    else:
        # If the file doesn't exist, create it and initialize to -1
        with open(LAST_CLEANED_FILE,'w') as file:
            file.write(str(-1))   
        last_cleaned_hour = -1 # Default value when the file is created

    return last_cleaned_hour

def update_last_cleaned_hour(hour):
    """Update the last cleaned hour in the file."""
    with open(LAST_CLEANED_FILE,'w') as file:
        file.write(str(hour))


def on_connect(client, userdata, flags, rc):
    """Callback function to handle MQTT broker connection."""
    if rc == 0:
        logging.info("Connected to MQTT Broker!")
    else:
        logging.error(f"Failed to connect, return code {rc}")

def initMqttClient():
    """Creates, connects, and starts the MQTT client loop."""
    global configHashMap
    client = mqtt.Client()
    client.on_connect = on_connect
    try:
        client.connect(configHashMap.get('MQTT_BROKER'), configHashMap.get('MQTT_PORT'), 60)
        print("Connected...")
        client.loop_start()
        return client
    except Exception as e:
        logging.error(f"initMqttClient Exception--: {e}")
        return None

def mainFunction():
    conveyorStatus = 0
    start_service = 0
    stop_service = 0
    db_configuration = 0
    modbus_configuration = 0
    dbConn = None
    cur = None
    global configHashMap
    loadConfiguration()
    mqtt_client = initMqttClient()
    
    speed_count = 0
    lastCheckConveyorStatus = datetime.datetime.now()
    tenSecDeltaTime = datetime.timedelta(seconds=5)
    CLEANING_STARTED = False
    while True:
        try:
            if db_configuration == 0:
                dbConn = getDatabaseConnection()
                if dbConn != None:
                    cur = dbConn.cursor()
                    db_configuration = 1
            elif modbus_configuration == 0:
                client=initClient()
                if client is not None:
                    modbus_configuration = 1
            else:

                try:
                    # Publish distance data to MQTT if connections are valid
                    if client is not None and mqtt_client is not None and mqtt_client.is_connected():
                        distance = readFromPlc(configHashMap.get("DISTANCE_REGISTER"), client)
                        payload = json.dumps({"distance": distance})
                        print("Published Distance:", payload)
                        mqtt_client.publish(configHashMap.get('MQTT_TOPIC'), payload)
                    else:
                        logging.warning("MQTT or Modbus client not connected.")
                    
                except Exception as e:
                    logging.error(f"publish_distance_mqtt() Exception: {e}")
                    time.sleep(0.5) 
                
                # Check conveyor status every 5 seconds
                if (datetime.datetime.now() - lastCheckConveyorStatus) > tenSecDeltaTime:
                    lastCheckConveyorStatus = datetime.datetime.now()

                    # Read light and conveyor status from PLC
                    lightStatus = readFromPlc(configHashMap.get("LIGHT_ON_REGISTER"), client)
                    conveyorStatus = readFromPlc(configHashMap.get("CONVEYOR_STATUS_REGISTER"), client)
                   
                    # Synchronize light with conveyor status
                    writeToPlc(configHashMap.get("LIGHT_ON_REGISTER"), conveyorStatus, client)
                  
                    logger.info("The Light status is %s",lightStatus)
                    logger.info("The conveyour status is %s",conveyorStatus)

                    # Get the current hour in 24-hour format
                    current_hour = datetime.datetime.now().hour
                    # List of cleaning time
                    cleaning_times = [6,14,22] # 6AM,2PM,10PM
                    # Load the last cleaned hour from the file.
                    last_cleaned_hour = load_last_cleaned_hour()
                    
                    if current_hour in cleaning_times and current_hour != last_cleaned_hour:
                        logger.info(f"Cleaning the camera at {current_hour}:00")

                        # Start cleaning mechanism
                        writeToPlc(configHashMap["CLEANING_MECHANISM_1"], 1, client)
                        CLEANING_STARTED = True

                        # Update the last cleaned hour in the file
                        update_last_cleaned_hour(current_hour)
                    else:
                        # Stop cleaning mechanism if cleaning was started
                        if CLEANING_STARTED is True:
                            CLEANING_STARTED = False
                            writeToPlc(configHashMap["CLEANING_MECHANISM_1"], 0, client)
                            logger.info("CLEANING_MECHANISM completed.")

                    # Update database if conveyor status changes
                    if conveyorStatus == 1 and start_service == 0:
                        logger.info("Conveyor status updated into DB")
                        query = "UPDATE " + configHashMap.get("DB_NAME") + ".CONVEYOR_STATUS_TABLE SET CONVEYOR_STATUS = '1' WHERE (ID = '1')"
                        cur.execute(query)
                        dbConn.commit()
                        start_service = 1
                        stop_service = 0
                        query = "INSERT INTO " + configHashMap.get("DB_NAME") + ".CONVEYOR_STATUS_HISTORY (STATUS) VALUES ('1')"
                        cur.execute(query)
                        dbConn.commit()
                    elif conveyorStatus == 0 and stop_service == 0:
                        logger.info("Conveyor status updated into DB")
                        query = "UPDATE " + configHashMap.get("DB_NAME") + ".CONVEYOR_STATUS_TABLE SET CONVEYOR_STATUS = '0' WHERE (ID = '1')"
                        cur.execute(query)
                        dbConn.commit()
                        stop_service = 1
                        start_service = 0
                        query = "INSERT INTO " + configHashMap.get("DB_NAME") + ".CONVEYOR_STATUS_HISTORY (STATUS) VALUES ('0')"
                        cur.execute(query)
                        dbConn.commit()
                        
        except Exception as e:
            logger.error(f"mainFunction() Inside While Exception is : {e}")
            if cur is not None:
                cur.close()
            if dbConn is not None:
                dbConn.close()
            if db_configuration == 1:
                db_configuration = 0
            if modbus_configuration == 1:
                modbus_configuration = 0
                client.close()
            time.sleep(5)

if __name__=="__main__":
    mainFunction()
