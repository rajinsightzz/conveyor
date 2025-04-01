import cv2
import os
import numpy as np
import logging
import traceback
from logging import handlers
from logging.handlers import TimedRotatingFileHandler
import time
import datetime
import glob
import json
import pymysql
from pymodbus.client import ModbusTcpClient
import ast
import xml.etree.ElementTree as ET
from PIL import ImageFont, ImageDraw, Image
import queue
import threading
from logging import handlers
from detectron2.utils.logger import setup_logger
from posix import mkdir
from collections import OrderedDict
setup_logger()
from detectron2 import model_zoo
from detectron2.engine import DefaultPredictor
from detectron2.config import get_cfg
from detectron2.utils.visualizer import Visualizer
from detectron2.data import MetadataCatalog, DatasetCatalog
from detectron2.structures import BoxMode
from detectron2.engine import DefaultTrainer
from detectron2.utils.visualizer import ColorMode
# import paho.mqtt.client as mqtt
import re
# from MailMod
# from MailModule.MAIL_SERVICE_V2 import GEmailClass

''' UI Process ID and Base Code Path '''
ALGO_PROCESS_ID = os.getpid()
BASE_PATH = os.getcwd()
#logger = None

defectDistance = 0  # Global variable to store real-time distance from MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "plc/distance"
debugMode = True
PROCESS_ID = os.getpid()
gMailObj = None
configHashMap = {}
configObject = OrderedDict()
client = None
LIST_LENGTH = 5
THRESHOLD_VALUE = 1
version = "1.0.0"
error_flag = 0

class CONFIG_KEY_NAME:
    CODE_PATH = "CODE_PATH"
    LOG_FILE_PATH = "LOG_FILE_PATH"
    LOG_BACKUP_COUNT = "LOG_BACKUP_COUNT"
    DEFECTIVE_IMAGE_PATH = "DEFECTIVE_IMAGE_PATH"
    RAW_IMAGE_DIR_PATH = "RAW_IMAGE_DIR_PATH"
    FRAME_START_INFO_FILE_PATH = "FRAME_START_INFO_FILE_PATH"
    TEMP_RAW_IMAGE_DIR_PATH = "TEMP_RAW_IMAGE_DIR_PATH" 
    
    DB_USER = "DB_USER"
    DB_PASS = "DB_PASS"
    DB_HOST = "DB_HOST"
    DB_NAME = "DB_NAME"
    
    CONFIG_YAML_FL_PATH = "CONFIG_YAML_FL_PATH"
    MODEL = "MODEL"
    DETECTTHRESH = "DETECTTHRESH"
    MASK_MODEL_PATH = "MASK_MODEL_PATH"
    JSON_PATH = "JSON_PATH"
    ALL_CLASS_NAMES = "ALL_CLASS_NAMES"
    DETECT_LABEL_NAMES = "DETECT_LABEL_NAMES"
    
    PLC_IP_ADDRESS = "PLC_IP_ADDRESS"
    PLC_WRITE_DB_NUMBER = "PLC_WRITE_DB_NUMBER"
    PLC_READ_DB_NUMBER = "PLC_READ_DB_NUMBER"

    MARKING_CONFIG_YAML_FL_PATH = "MARKING_CONFIG_YAML_FL_PATH"
    MARKING_MODEL = "MARKING_MODEL"
    MARKING_DETECTTHRESH = "MARKING_DETECTTHRESH"
    MARKING_MASK_MODEL_PATH = "MARKING_MASK_MODEL_PATH"
    MARKING_JSON_PATH = "MARKING_JSON_PATH"
    MARKING_OUTPATH_PATH = "MARKING_OUTPATH_PATH"
    MARKING_ALL_CLASS_NAMES = "MARKING_ALL_CLASS_NAMES"
    MARKING_DETECT_LABEL_NAMES = "MARKING_DETECT_LABEL_NAMES"

    MQTT_BROKER = "MQTT_BROKER"
    MQTT_PORT   = "MQTT_PORT"
    MQTT_TOPIC = "MQTT_TOPIC"

def loadConfiguration():
    global configHashMap
    try:
        print("Curd path = ", os.path.curdir)
        config_file_path = os.path.join(os.path.curdir,"configV1.xml")
        if debugMode is True:
            config_file_path = os.path.join(os.path.curdir,"config_local.xml")
        config_parse = ET.parse(config_file_path)
        config_root = config_parse.getroot()
        
        configHashMap[CONFIG_KEY_NAME.CODE_PATH] = config_root[0][0].text
        configHashMap[CONFIG_KEY_NAME.LOG_FILE_PATH] = config_root[0][1].text
        configHashMap[CONFIG_KEY_NAME.LOG_BACKUP_COUNT] = int(config_root[0][2].text)
        configHashMap[CONFIG_KEY_NAME.DEFECTIVE_IMAGE_PATH] = config_root[0][3].text
        configHashMap[CONFIG_KEY_NAME.RAW_IMAGE_DIR_PATH] = config_root[0][4].text
        configHashMap[CONFIG_KEY_NAME.FRAME_START_INFO_FILE_PATH] = config_root[0][5].text
        configHashMap[CONFIG_KEY_NAME.TEMP_RAW_IMAGE_DIR_PATH] = config_root[0][6].text
        
        configHashMap[CONFIG_KEY_NAME.DB_USER] = config_root[1][0].text
        configHashMap[CONFIG_KEY_NAME.DB_PASS] = config_root[1][1].text
        configHashMap[CONFIG_KEY_NAME.DB_HOST] = config_root[1][2].text
        configHashMap[CONFIG_KEY_NAME.DB_NAME] = config_root[1][3].text
        
        configHashMap[CONFIG_KEY_NAME.CONFIG_YAML_FL_PATH] = config_root[2][0].text
        configHashMap[CONFIG_KEY_NAME.MODEL] = config_root[2][1].text
        configHashMap[CONFIG_KEY_NAME.DETECTTHRESH] = float(config_root[2][2].text)
        configHashMap[CONFIG_KEY_NAME.MASK_MODEL_PATH] = config_root[2][3].text
        configHashMap[CONFIG_KEY_NAME.JSON_PATH] = config_root[2][4].text
        configHashMap[CONFIG_KEY_NAME.ALL_CLASS_NAMES] = ast.literal_eval(config_root[2][5].text)
        configHashMap[CONFIG_KEY_NAME.DETECT_LABEL_NAMES] = ast.literal_eval(config_root[2][6].text)
        
        configHashMap[CONFIG_KEY_NAME.PLC_IP_ADDRESS] = config_root[3][0].text
        configHashMap[CONFIG_KEY_NAME.PLC_WRITE_DB_NUMBER] = int(config_root[3][1].text)
        configHashMap[CONFIG_KEY_NAME.PLC_READ_DB_NUMBER] = int(config_root[3][2].text)

        # Read MQTT details
        configHashMap[CONFIG_KEY_NAME.MQTT_BROKER] = config_root[4][0].text
        configHashMap[CONFIG_KEY_NAME.MQTT_PORT] = int(config_root[4][1].text)
        configHashMap[CONFIG_KEY_NAME.MQTT_TOPIC] = config_root[2][2].text

        # configHashMap[CONFIG_KEY_NAME.MARKING_CONFIG_YAML_FL_PATH] = config_root[4][0].text
        # configHashMap[CONFIG_KEY_NAME.MARKING_MODEL] = config_root[4][1].text
        # configHashMap[CONFIG_KEY_NAME.MARKING_DETECTTHRESH] = float(config_root[4][2].text)
        # configHashMap[CONFIG_KEY_NAME.MARKING_MASK_MODEL_PATH] = config_root[4][3].text
        # configHashMap[CONFIG_KEY_NAME.MARKING_JSON_PATH] = config_root[4][4].text
        # configHashMap[CONFIG_KEY_NAME.MARKING_OUTPATH_PATH] = config_root[4][5].text
        # configHashMap[CONFIG_KEY_NAME.MARKING_ALL_CLASS_NAMES] = ast.literal_eval(config_root[4][6].text)
        # configHashMap[CONFIG_KEY_NAME.MARKING_DETECT_LABEL_NAMES] = ast.literal_eval(config_root[4][7].text)
    except Exception as e:
        print(f"loadConfiguration() Exception is {e}")
        print(traceback.format_exc())

''' Database Function Start'''
def getDatabaseConnection(logger):
    global configHashMap
    dbConnection = None
    try:
        dbConnection = pymysql.connect(
            host = configHashMap.get(CONFIG_KEY_NAME.DB_HOST),
            user = configHashMap.get(CONFIG_KEY_NAME.DB_USER), 
            passwd = configHashMap.get(CONFIG_KEY_NAME.DB_PASS),
            db = configHashMap.get(CONFIG_KEY_NAME.DB_NAME))
    except Exception as e:
        logger.critical("getDatabaseConnection() Exception is : "+ str(e))
        logger.critical(traceback.format_exc())
        #raise Exception(f"raise from getDatabaseConnection() {e}")
    return dbConnection

# def initializeLogger():
#     global configHashMap
#     try:
#         ''' Initializing Logger '''
#         logger = logging.getLogger("Insightzz")
#         logger.setLevel(logging.DEBUG)
        
#         # Define the log file format
#         formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
#         # Create a TimedRotatingFileHandler for daily rotation
#         log_file = configHashMap.get(CONFIG_KEY_NAME.LOG_FILE_PATH)+os.path.basename(__file__[:-2])+"log"
#         file_handler = TimedRotatingFileHandler(log_file, when='midnight', backupCount=configHashMap.get(CONFIG_KEY_NAME.LOG_BACKUP_COUNT))
#         file_handler.setLevel(logging.DEBUG)
#         file_handler.setFormatter(formatter)
#         logger.addHandler(file_handler)
#         logger.debug("Algorithm logger Module Initialized")
#         return logger
#     except Exception as e:
#         print(f"initializeLogger() Exception is {e}")
#         print(traceback.format_exc())
#         return 0

class MaskRCNN_Conveyor:
    def __init__(self):
        self.predictor=None
        self.mrcnn_config_fl= configHashMap.get(CONFIG_KEY_NAME.CONFIG_YAML_FL_PATH)
        self.mrcnn_model_loc= configHashMap.get(CONFIG_KEY_NAME.MASK_MODEL_PATH)
        self.mrcnn_model_fl= configHashMap.get(CONFIG_KEY_NAME.MODEL )
        self.detection_thresh= configHashMap.get(CONFIG_KEY_NAME.DETECTTHRESH)
        self.register_modeldatasets()

    def register_modeldatasets(self):
        tag="conveyor_inf"
        MetadataCatalog.get(tag).set(thing_classes=configHashMap.get(CONFIG_KEY_NAME.ALL_CLASS_NAMES))

        self.conveyor_metadata = MetadataCatalog.get(tag)
        cfg = get_cfg()
        cfg.merge_from_file(self.mrcnn_config_fl)
        if debugMode is True:
            cfg.merge_from_list(["MODEL.DEVICE","cpu"]) #cpu
        cfg.MODEL.ROI_HEADS.NUM_CLASSES =len(configHashMap.get(CONFIG_KEY_NAME.ALL_CLASS_NAMES))  # only has one class (ballon)
        cfg.OUTPUT_DIR=self.mrcnn_model_loc
        cfg.MODEL.WEIGHTS =os.path.join(cfg.OUTPUT_DIR,self.mrcnn_model_fl)
        cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = self.detection_thresh
        self.predictor = DefaultPredictor(cfg)

    def run_inference(self,img):
        output = self.predictor(img)
        predictions=output["instances"].to("cpu")
        classes = predictions.pred_classes.numpy()

        class_list =[]
        for i in classes:
            class_list.append(self.conveyor_metadata.get("thing_classes")[i])
        class_dict = {} 
        for item in class_list: 
            if (item in class_dict): 
                class_dict[item] += 1
            else: 
                class_dict[item] = 1 
       
        boxes_surface = output["instances"].pred_boxes.tensor.to("cpu").numpy()
        pred_class_surface = output["instances"].pred_classes.to("cpu").numpy()
        scores_surface = output["instances"].scores.to("cpu").numpy()
        # mask_surface = output['instances'].pred_masks.to("cpu").numpy()
        labellist = []
        # masklist = []
        # masks = None  
        # if predictions.has("pred_masks"):
        #     masks = predictions.pred_masks.numpy() 

        for i,box in enumerate(boxes_surface):
            # count=1
            if scores_surface[i] > 0.8:
                score = scores_surface[i]
                box = boxes_surface[i]
                ymin = int(box[1])
                xmin = int(box[0])
                ymax = int(box[3])
                xmax = int(box[2])
                class_name = self.conveyor_metadata.get("thing_classes")[pred_class_surface[i]]
                # congig_labels=configHashMap.get(CONFIG_KEY_NAME.DETECT_LABEL_NAMES)
                # if class_name not in configHashMap.get(CONFIG_KEY_NAME.DETECT_LABEL_NAMES):
                #     #print(class_name)
                #     continue
                labellistsmall = []
                labellistsmall.append(class_name)
                labellistsmall.append(xmin)
                labellistsmall.append(ymin)
                labellistsmall.append(xmax)
                labellistsmall.append(ymax) 
                labellistsmall.append(score)
                cx, cy = get_centroid(xmin,xmax, ymin, ymax)
                labellistsmall.append(cx)
                labellistsmall.append(cy)
                labellist.append(labellistsmall)
                  
        return labellist
    

class MaskRCNN_MARKING_MODEL:
    def __init__(self):
        self.predictor=None
        self.mrcnn_config_fl= configHashMap.get(CONFIG_KEY_NAME.MARKING_CONFIG_YAML_FL_PATH)
        self.mrcnn_model_loc= configHashMap.get(CONFIG_KEY_NAME.MARKING_MASK_MODEL_PATH)
        self.mrcnn_model_fl= configHashMap.get(CONFIG_KEY_NAME.MARKING_MODEL )
        self.detection_thresh= configHashMap.get(CONFIG_KEY_NAME.MARKING_DETECTTHRESH)
        self.register_modeldatasets()

    def register_modeldatasets(self):
        tag="marking_conveyor_inf"
        MetadataCatalog.get(tag).set(thing_classes=configHashMap.get(CONFIG_KEY_NAME.MARKING_ALL_CLASS_NAMES))

        self.conveyor_metadata = MetadataCatalog.get(tag)
        cfg = get_cfg()
        cfg.merge_from_file(self.mrcnn_config_fl)
        if debugMode is True:
            cfg.merge_from_list(["MODEL.DEVICE","cpu"]) #cpu
        cfg.MODEL.ROI_HEADS.NUM_CLASSES =len(configHashMap.get(CONFIG_KEY_NAME.MARKING_ALL_CLASS_NAMES))  # only has one class (ballon)
        cfg.OUTPUT_DIR=self.mrcnn_model_loc
        cfg.MODEL.WEIGHTS =os.path.join(cfg.OUTPUT_DIR,self.mrcnn_model_fl)
        cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = self.detection_thresh
        self.predictor = DefaultPredictor(cfg)

    def run_inference(self,img):
        output = self.predictor(img)
        predictions=output["instances"].to("cpu")
        classes = predictions.pred_classes.numpy()

        class_list =[]
        for i in classes:
            class_list.append(self.conveyor_metadata.get("thing_classes")[i])
        class_dict = {} 
        for item in class_list: 
            if (item in class_dict): 
                class_dict[item] += 1
            else: 
                class_dict[item] = 1 
       
        boxes_surface = output["instances"].pred_boxes.tensor.to("cpu").numpy()
        pred_class_surface = output["instances"].pred_classes.to("cpu").numpy()
        scores_surface = output["instances"].scores.to("cpu").numpy()
        # mask_surface = output['instances'].pred_masks.to("cpu").numpy()
        labellist = []
        # masklist = []
        # masks = None  
        # if predictions.has("pred_masks"):
        #     masks = predictions.pred_masks.numpy() 

        for i,box in enumerate(boxes_surface):
            # count=1
            if scores_surface[i] > 0.8:
                score = scores_surface[i]
                box = boxes_surface[i]
                ymin = int(box[1])
                xmin = int(box[0])
                ymax = int(box[3])
                xmax = int(box[2])
                class_name = self.conveyor_metadata.get("thing_classes")[pred_class_surface[i]]
                # congig_labels=configHashMap.get(CONFIG_KEY_NAME.DETECT_LABEL_NAMES)
                # if class_name not in configHashMap.get(CONFIG_KEY_NAME.DETECT_LABEL_NAMES):
                #     #print(class_name)
                #     continue
                labellistsmall = []
                labellistsmall.append(class_name)
                labellistsmall.append(xmin)
                labellistsmall.append(ymin)
                labellistsmall.append(xmax)
                labellistsmall.append(ymax) 
                labellistsmall.append(score)
                cx, cy = get_centroid(xmin,xmax, ymin, ymax)
                labellistsmall.append(cx)
                labellistsmall.append(cy)
                labellist.append(labellistsmall)
                  
        return labellist

def get_centroid(xmin, xmax, ymin, ymax):
    cx = int((xmin + xmax) / 2.0)
    cy = int((ymin + ymax) / 2.0)
    return(cx, cy)

def drawCV2Box(frame,labelName, xmin,ymin,xmax,ymax,logger):
    try:
        cv2.putText(frame, labelName, (xmin,ymin-10), cv2.FONT_HERSHEY_SIMPLEX,
                                       2, (255, 255, 255), 3, cv2.LINE_AA)                            
        cv2.rectangle(frame, (xmin,ymin), (xmax,ymax), (0, 255, 0),2)
    except Exception as e:
        logger.critical(f"Exception in drawCV2Box {e}")
        logger.critical(traceback.format_exc())

def addTextOnImage(frame,labelName):
    try:
        width = frame.shape[1]
        cv2.rectangle(frame, (0,0), (width,100), (0, 0, 0),cv2.FILLED)
        cv2.putText(frame, labelName, (50,50), cv2.FONT_HERSHEY_SIMPLEX,
                                       2, (0, 255, 255), 3, cv2.LINE_AA)                            
    except Exception as e:
        pass

def drawCV2BoxRedColor(frame,labelName, xmin,ymin,xmax,ymax):
    try:
        cv2.putText(frame, labelName, (xmin,ymin-10), cv2.FONT_HERSHEY_SIMPLEX,
                                       2, (0, 0, 255), 3, cv2.LINE_AA)                            
        cv2.rectangle(frame, (xmin,ymin), (xmax,ymax), (0, 0, 255),2)
    except Exception as e:
        pass
        # print("Exception in drawCV2BoxRedColor : "+ str(e))   

def draw_rectangle(img_rd, class_name, ymin, ymax, xmin, xmax, color, score):
    class_name = class_name+" "+str(score)[:4] 
    fontsize_x = cv2.getTextSize(class_name, cv2.FONT_HERSHEY_SIMPLEX, 3, 1)[0][0]
    fontsize_y = cv2.getTextSize(class_name, cv2.FONT_HERSHEY_SIMPLEX, 3, 1)[0][1] 

    #for label rectangle
    cv2.rectangle(img_rd, (xmin,ymin), (xmax,ymax), (int(color[0]),int(color[1]),int(color[2])),2) 

    #for defect text
    cv2.rectangle(img_rd, (xmin,ymin), ((xmin+fontsize_x),int(ymin-25)), (int(color[0]),int(color[1]), int(color[2])),-1)
    cv2.putText(img_rd, class_name,(xmin,int(ymin)) ,cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0,0,0), 2, cv2.FILLED)

    return img_rd

def insertDefectDetailsIntoImageProcessingTable(DEFECT_TYPE,DEFECT_LOCATION,DEFECT_SIZE_X,DEFECT_SIZE_Y,DEFECT_SIDE,IMAGE_PATH, xmin, ymin, xmax, ymax, defectDistance, logger, cur, dbConn):
    global error_flag
    try:
        logger.debug("Fetching data from image processing table")
        query = f"insert into DALLA_CONVEYOR_DB.IMAGE_PROCESSING_TABLE(DEFECT_TYPE,DEFECT_LOCATION,DEFECT_SIZE_X,DEFECT_SIZE_Y,DEFECT_SIDE,IMAGE_PATH, XMIN, YMIN, XMAX, YMAX, DISTANCE_IN_METER) values('{DEFECT_TYPE}','{DEFECT_LOCATION}','{DEFECT_SIZE_X}','{DEFECT_SIZE_Y}','{DEFECT_SIDE}','{IMAGE_PATH}', '{xmin}', '{ymin}', '{xmax}', '{ymax}','{defectDistance}')"
        cur.execute(query)
        dbConn.commit()
    except Exception as e:
        error_flag = 1
        logger.critical("insertDefectDetailsIntoImageProcessingTable() Exception is : "+ str(e))
        #raise Exception(f"raise error from insertDefectDetailsIntoImageProcessingTable {e}")

        
def getDirectoryPath(ParentDirName):
    mydir = os.path.join(ParentDirName, datetime.datetime.now().strftime('%Y%m%d')+"/")
    if os.path.isdir(mydir) is not True:
        os.makedirs(mydir)
    return mydir

def getSubDirectoryPath(path, ParentDirName):
    mydir = os.path.join(path, ParentDirName+"/")
    if os.path.isdir(mydir) is not True:
        os.makedirs(mydir)
    return mydir

def popFromList(tmp_list):
    if len(tmp_list) >= LIST_LENGTH:
        tmp_list.pop(-1)
    return tmp_list

def appendAtIndex(tmp_list,value):
    tmp_list.insert(0,value)
    return tmp_list

def getConveyorSpeed(logger):
    conveyorSpeed = 2.5 # meter per sec
    try:
        speed =  100 #readFromPlc(READ_REGISTERS.SPEED_REGISTER)
        if speed == -1:
            conveyorSpeed = 2.5
        else:
            conveyorSpeed = float(speed)
            if conveyorSpeed > 9:
                conveyorSpeed = 9
    except Exception as e:
        logger.critical(f"getConveyorSpeed : Exception is : {e}")
    
    return conveyorSpeed

# def checkIfDefectIsRepeated(SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax, NonRepeatedDefectList):
#     isRepeatedDefect = False
#     THRESHOLD_PIXEL_VALUE = 80
#     try:
#         for item in NonRepeatedDefectList:
#             defectName = item[2]
#             defect_xmin = item[3]
#             defect_ymin = item[4]
#             defect_xmax = item[5]
#             defect_ymax = item[6]
            
#             defect_xlen_max_th = (defect_xmax-defect_xmin) + THRESHOLD_PIXEL_VALUE
#             defect_xlen_min_th = (defect_xmax-defect_xmin) - THRESHOLD_PIXEL_VALUE
#             defect_ylen_max_th = (defect_ymax-defect_ymin) + THRESHOLD_PIXEL_VALUE
#             defect_ylen_min_th = (defect_ymax-defect_ymin) - THRESHOLD_PIXEL_VALUE
            
#             if defectName == DEFECT_TYPE:
#                 if SIDE == "TOP":
#                     if xmin > (defect_xmin - THRESHOLD_PIXEL_VALUE) and xmax < (defect_xmax + THRESHOLD_PIXEL_VALUE):
#                         xlen = xmax-xmin
#                         ylen = ymax-ymin
                        
#                         if xlen > defect_xlen_min_th and xlen < defect_xlen_max_th and ylen > defect_ylen_min_th and ylen < defect_ylen_max_th:
#                             isRepeatedDefect = True
#                         else:
#                             print(f"SIDE is : {SIDE} : {xlen} > {defect_xlen_min_th} and {xlen} < {defect_xlen_max_th} and {ylen} > {defect_ylen_min_th} and {ylen} < {defect_ylen_max_th}")
#                     else:
#                         print(f"SIDE is : {SIDE} : {xmin} > ({defect_xmin} - {THRESHOLD_PIXEL_VALUE}) and {xmax} < ({defect_xmax} + {THRESHOLD_PIXEL_VALUE})")
#                 elif SIDE == "BOTTOM":
#                     if ymin > (defect_ymin - THRESHOLD_PIXEL_VALUE) and ymax < (defect_ymax + THRESHOLD_PIXEL_VALUE):
#                         xlen = xmax-xmin
#                         ylen = ymax-ymin
                        
#                         if xlen > defect_xlen_min_th and xlen < defect_xlen_max_th and ylen > defect_ylen_min_th and ylen < defect_ylen_max_th:
#                             isRepeatedDefect = True
#                         else:
#                             print(f"SIDE is : {SIDE} : {xlen} > {defect_xlen_min_th} and {xlen} < {defect_xlen_max_th} and {ylen} > {defect_ylen_min_th} and {ylen} < {defect_ylen_max_th}")
#                     else:
#                         print(f"SIDE is : {SIDE} : {ymin} > ({defect_ymin} - {THRESHOLD_PIXEL_VALUE}) and {ymax} < ({defect_ymax} + {THRESHOLD_PIXEL_VALUE})")
                    
#     except Exception as e:
#         logger.critical("checkIfDefectIsRepeated Exception is : "+ str(e))
    
#     return isRepeatedDefect

# def processImageResultFunction(imageObj, labelList, NonRepeatedDefectList, cameraSide, logger):
#     global configHashMap
#     try:
#         labellistCam1 = []
#         labellistCam2 = []
        
#         DETECT_LABEL_NAMES = configHashMap.get(CONFIG_KEY_NAME.DETECT_LABEL_NAMES)
#         DEFECTIVE_IMG_FILE_PATH = configHashMap.get(CONFIG_KEY_NAME.DEFECTIVE_IMAGE_PATH)
    
#         if cameraSide == "TOP":
#             labellistCam1 = labelList
#             camOneImage = imageObj
#         else:
#             labellistCam2 = labelList
#             camTwoImage = imageObj
        
#         if len(labellistCam1) > 0:
#             ''' Processing for Camera 1 Images '''
#             for item in labellistCam1:
#                 labelName = item[0]

#                 xmin = item[1]
#                 ymin = item[2]
#                 xmax = item[3]
#                 ymax = item[4]
#                 cx = item[6]
#                 cy = item[7]
#                 DEFECT_SIDE = "TOP"
#                 DEFECT_LOCATION = ""
#                 if labelName in DETECT_LABEL_NAMES:
#                     DEFECT_TYPE=labelName
#                     if cy <= 350:
#                         DEFECT_LOCATION = "LEFT"
#                     elif cy > 350 and cy <= 600:
#                         DEFECT_LOCATION = "MIDDLE"
#                     elif cy > 600:
#                         DEFECT_LOCATION = "RIGHT"                            
                    
#                     DEFECT_SIZE_X = xmax - xmin
#                     DEFECT_SIZE_Y = ymax - ymin 
                    
#                     isDefect = checkIfDefectSizeIsValid(DEFECT_SIDE,DEFECT_SIZE_X,DEFECT_SIZE_Y,labelName,logger,cur, dbConn)
                    
#                     defectDistance = 0
#                     if isDefect is True: 
#                         currDatePath = getDirectoryPath(DEFECTIVE_IMG_FILE_PATH)
#                         currSideDirPath = getSubDirectoryPath(currDatePath, DEFECT_SIDE)
#                         currSideDirPathWithLabel = getSubDirectoryPath(currSideDirPath, labelName)
#                         isRepeatedDefect = checkIfDefectIsRepeated(DEFECT_SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax, NonRepeatedDefectList,logger)
                        
#                         IMAGE_PATH_TMP = f"{currSideDirPathWithLabel}DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.jpg"
#                         IMAGE_PATH_TMP_RAW = f"{currSideDirPathWithLabel}DEFECT_RAW_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.jpg"
#                         cv2.imwrite(IMAGE_PATH_TMP_RAW, camOneImage)
#                         drawCV2Box(camOneImage,labelName,xmin,ymin,xmax,ymax,logger) 
#                         cv2.imwrite(IMAGE_PATH_TMP, camOneImage)
#                         insertDefectDetailsIntoImageProcessingTable(labelName,DEFECT_LOCATION,DEFECT_SIZE_X,DEFECT_SIZE_Y,DEFECT_SIDE,IMAGE_PATH_TMP, defectDistance, logger, cur, dbConn))
                    
#         if len(labellistCam2) > 0:           
#             for item in labellistCam2:
#                 labelName = item[0]
#                 xmin = item[1]
#                 ymin = item[2]
#                 xmax = item[3]
#                 ymax = item[4]
#                 cx = item[6]
#                 cy = item[7]
#                 DEFECT_SIDE = "BOTTOM"
#                 DEFECT_LOCATION = ""
#                 if labelName in DETECT_LABEL_NAMES:
#                     if cy <= 350:
#                         DEFECT_LOCATION = "LEFT"
#                     elif cy >350 and cy <= 600:
#                         DEFECT_LOCATION = "MIDDLE"
#                     elif cy > 600:
#                         DEFECT_LOCATION = "RIGHT"                            
                    
#                     DEFECT_SIZE_X = xmax - xmin
#                     DEFECT_SIZE_Y = ymax - ymin 
                    
#                     isDefect = checkIfDefectSizeIsValid(DEFECT_SIDE,DEFECT_SIZE_X,DEFECT_SIZE_Y,logger,cur, dbConn)
#                     defectDistance = 0
#                     if isDefect is True:
#                         currDatePath = getDirectoryPath(DEFECTIVE_IMG_FILE_PATH)
#                         currSideDirPath = getSubDirectoryPath(currDatePath, DEFECT_SIDE)
#                         currSideDirPathWithLabel = getSubDirectoryPath(currSideDirPath, labelName)
                        
#                         IMAGE_PATH_TMP = f"{currSideDirPathWithLabel}DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.jpg"
#                         IMAGE_PATH_TMP_RAW = f"{currSideDirPathWithLabel}DEFECT_RAW_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.jpg"
#                         cv2.imwrite(IMAGE_PATH_TMP_RAW, camTwoImage)
#                         drawCV2Box(camTwoImage,labelName,xmin,ymin,xmax,ymax,logger) 
#                         cv2.imwrite(IMAGE_PATH_TMP, camTwoImage)
#                         insertDefectDetailsIntoImageProcessingTable(labelName,DEFECT_LOCATION,DEFECT_SIZE_X,DEFECT_SIZE_Y,DEFECT_SIDE,IMAGE_PATH_TMP, defectDistance, logger, cur, dbConn))
            
#     except Exception as e:
#         logger.critical(f"processImageResultFunction Exception is : {e}")
#         logger.critical(f"processImageResultFunction Traceback Exception is : {traceback.format_exc()}")


def getFrameGrabbingStartDatetime(logger):
    global configHashMap
    file_path = configHashMap.get(CONFIG_KEY_NAME.FRAME_START_INFO_FILE_PATH)
    value = ""
    try:
        with open(file_path, 'r') as file:
            value = str(file.readline().strip())
    except Exception as e:
        logger.critical(f"getFrameGrabbingStartDatetime() Exception is : {e}")
        logger.critical(f"getFrameGrabbingStartDatetime() Traceback Exception is : {traceback.format_exc()}")
    return value

def delete_zero_size_jpg(file_path, logger):
    try:
        if os.path.exists(file_path):
            if file_path.endswith(".jpg") and os.path.getsize(file_path) == 0:
                os.remove(file_path)
                logger.debug(f"Deleted zero size image: {file_path}")
                return True           
            else:
                logger.debug(f"File is not zero-size .jpg file: {file_path}")
        else:
            logger.debug(f"File is does not exist : {file_path}")
        return False
    except Exception as e:
        logger.critical(f"Delete zero size image exception is {e}")
        logger.critical(traceback.format_exc()) 
        return False

def fetchRepeatedDefectList(logger, cur, dbConn):
    global error_flag
    data_set = []
    try:
        query = f"select * from REPEATED_DEFECT_TABLE order by ID desc;"
        cur.execute(query)
        data_set = cur.fetchall()
    except Exception as e:
        error_flag = 1
        logger.critical("fetchRepeatedDefectList Exception is : "+ str(e))
        logger.critical(traceback.format_exc())
        #raise Exception(f"raise error from fetchRepeatedDefectList {e}")
    finally:
        return data_set

def is_small_roi_inside_big_roi(small_roi, big_roi,percentage):
    xmin_small, ymin_small, xmax_small, ymax_small = small_roi
    xmin_large, ymin_large, xmax_large, ymax_large = big_roi
    
    # Calculate the areas of the small ROI and the large ROI
    area_small = (xmax_small - xmin_small) * (ymax_small - ymin_small)
    area_large = (xmax_large - xmin_large) * (ymax_large - ymin_large)
    
    # Calculate the coordinates of the intersection ROI
    xmin_inter = max(xmin_small, xmin_large)
    ymin_inter = max(ymin_small, ymin_large)
    xmax_inter = min(xmax_small, xmax_large)
    ymax_inter = min(ymax_small, ymax_large)
    
    # Calculate the area of the intersection ROI
    width_inter = max(0, xmax_inter - xmin_inter)
    height_inter = max(0, ymax_inter - ymin_inter)
    area_inter = width_inter * height_inter
    
    # Check if the ratio of the intersection area to the small ROI area is >= 0.8
    if area_inter / area_small >= (percentage):
        return True
    else:
        return False
    
def checkIfDefectIsRepeated(SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax, NonRepeatedDefectList,logger):
    isRepeatedDefect = False
    THRESHOLD_PIXEL_VALUE = 80
    try:
        for item in NonRepeatedDefectList:
            defectName = item[2]
            defect_xmin = item[3]
            defect_ymin = item[4]
            defect_xmax = item[5]
            defect_ymax = item[6]
            
            defect_xlen_max_th = (defect_xmax-defect_xmin) + THRESHOLD_PIXEL_VALUE
            defect_xlen_min_th = (defect_xmax-defect_xmin) - THRESHOLD_PIXEL_VALUE
            defect_ylen_max_th = (defect_ymax-defect_ymin) + THRESHOLD_PIXEL_VALUE
            defect_ylen_min_th = (defect_ymax-defect_ymin) - THRESHOLD_PIXEL_VALUE
            
            if defectName == DEFECT_TYPE:
                if SIDE == "TOP":
                    if xmin > (defect_xmin - THRESHOLD_PIXEL_VALUE) and xmax < (defect_xmax + THRESHOLD_PIXEL_VALUE):
                        xlen = xmax-xmin
                        ylen = ymax-ymin
                        
                        if xlen > defect_xlen_min_th and xlen < defect_xlen_max_th and ylen > defect_ylen_min_th and ylen < defect_ylen_max_th:
                            isRepeatedDefect = True
                        else:
                            logger.debug(f"SIDE is : {SIDE} : {xlen} > {defect_xlen_min_th} and {xlen} < {defect_xlen_max_th} and {ylen} > {defect_ylen_min_th} and {ylen} < {defect_ylen_max_th}")
                    else:
                        logger.debug(f"SIDE is : {SIDE} : {xmin} > ({defect_xmin} - {THRESHOLD_PIXEL_VALUE}) and {xmax} < ({defect_xmax} + {THRESHOLD_PIXEL_VALUE})")
                elif SIDE == "BOTTOM":
                    if ymin > (defect_ymin - THRESHOLD_PIXEL_VALUE) and ymax < (defect_ymax + THRESHOLD_PIXEL_VALUE):
                        xlen = xmax-xmin
                        ylen = ymax-ymin
                        
                        if xlen > defect_xlen_min_th and xlen < defect_xlen_max_th and ylen > defect_ylen_min_th and ylen < defect_ylen_max_th:
                            isRepeatedDefect = True
                        else:
                            logger.debug(f"SIDE is : {SIDE} : {xlen} > {defect_xlen_min_th} and {xlen} < {defect_xlen_max_th} and {ylen} > {defect_ylen_min_th} and {ylen} < {defect_ylen_max_th}")
                    else:
                        logger.debug(f"SIDE is : {SIDE} : {ymin} > ({defect_ymin} - {THRESHOLD_PIXEL_VALUE}) and {ymax} < ({defect_ymax} + {THRESHOLD_PIXEL_VALUE})")
                    
    except Exception as e:
        logger.critical("checkIfDefectIsRepeated Exception is : "+ str(e))
    
    return isRepeatedDefect    

# def checkDefectInDBForRepeated(DEFECT_SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax,logger,cur, dbConn):
#     global error_flag
#     isRepeatedDefect = False
#     THRESHOLD_PIXEL_VALUE = 150
#     data_set = []
#     try:
#         startDT = datetime.datetime.now() - datetime.timedelta(hours=24)
#         query = f"""select * from DALLA_CONVEYOR_DB.IMAGE_PROCESSING_TABLE where 
#                 TIMESTAMP(CREATE_DATETIME) > TIMESTAMP('{startDT}') and 
#                 DEFECT_TYPE = '{DEFECT_TYPE}' and DEFECT_SIDE = '{DEFECT_SIDE}' and 
#                 {ymin}  < (YMIN + {THRESHOLD_PIXEL_VALUE}) and {ymin} > (YMIN - {THRESHOLD_PIXEL_VALUE}) and 
#                 {ymax}  < (YMAX + {THRESHOLD_PIXEL_VALUE}) and {ymax} > (YMAX - {THRESHOLD_PIXEL_VALUE})
#                 order by ID desc limit 1;"""
#         cur.execute(query)
#         data_set = cur.fetchall()
#         logger.info("Data_set = %s", data_set)
#         if len(data_set) > 0:
#             logger.info("Defect present in DB")
#             isRepeatedDefect = False
#         else:
#             logger.info("Defect not present in DB")
#             isRepeatedDefect = True
#     except Exception as e:
#         error_flag = 1
#         logger.critical("checkDefectInDBForRepeated() Exception is : "+ str(e))
#         #raise Exception(f"raise error from checkDefectInDBForRepeated {e}")
#     finally:
#         return isRepeatedDefect


def checkDefectInDBForRepeated(DEFECT_SIDE, DEFECT_TYPE, defectDistance, logger, cur, dbConn):
    global error_flag
    isRepeatedDefect = False
    THRESHOLD_DISTANCE = 1  # ±1 meters range
    
    try:
        startDT = datetime.datetime.now() - datetime.timedelta(hours=24)  # Check records from the last 24 hours
        
        # Calculate the backward and forward range based on the new defect distance
        lower_range = defectDistance - THRESHOLD_DISTANCE
        upper_range = defectDistance + THRESHOLD_DISTANCE
        
        # SQL query to check for any defects within the range
        query = f"""
            SELECT * FROM DALLA_CONVEYOR_DB.IMAGE_PROCESSING_TABLE 
            WHERE TIMESTAMP(CREATE_DATETIME) > TIMESTAMP('{startDT}') 
            AND DEFECT_TYPE = '{DEFECT_TYPE}' 
            AND DEFECT_SIDE = '{DEFECT_SIDE}'
            AND DISTANCE_IN_METER BETWEEN {lower_range} AND {upper_range};
        """
        
        cur.execute(query)
        data_set = cur.fetchall()  # Get the result
        
        logger.info("Data_set = %s", data_set)

        # If any record is found within the threshold range, it's a repeated defect
        if len(data_set) > 0:
            logger.info(f"Repeated defect found in DB.")
            isRepeatedDefect = True
        else:
            logger.info("No repeated defect found (new defect)")
    
    except Exception as e:
        error_flag = 1
        logger.critical("Error in checking defect: " + str(e))
    
    return isRepeatedDefect

distance_pattern = re.compile(r"_DISTANCE_(\d+)\.jpg$")
def get_distance_from_filename(filename):
    """Extracts the distance value from the image filename."""
    match = distance_pattern.search(filename)
    return int(match.group(1)) if match else None

def mainFunction(logger, cur, dbConn):
    try:
        db_flag = 0
        maskObj = MaskRCNN_Conveyor()
        # marking_cls= MaskRCNN_MARKING_MODEL() ##################
        RAW_IMAGE_DIR_PATH = configHashMap.get(CONFIG_KEY_NAME.RAW_IMAGE_DIR_PATH)
        TEMP_RAW_IMAGE_DIR_PATH = configHashMap.get(CONFIG_KEY_NAME.TEMP_RAW_IMAGE_DIR_PATH)
        DETECT_LABEL_NAMES = configHashMap.get(CONFIG_KEY_NAME.DETECT_LABEL_NAMES)
        DEFECTIVE_IMG_FILE_PATH = configHashMap.get(CONFIG_KEY_NAME.DEFECTIVE_IMAGE_PATH)
        
        try:
            NonRepeatedDefectList = []
            NonRepeatedDefectList = fetchRepeatedDefectList(logger, cur, dbConn)
            #logger.info("Non Repeated DefectList = %s", NonRepeatedDefectList)
        except:
            logger.error("Error in fetching NonRepeatedDefectList")
            #raise Exception(f"raise error from NonRepeatedDefectList {e}")

        ''' Variables for distance calculation '''
        # totalLength = 5000 # Meter
        # conveyorLoopResetTime = datetime.datetime.now()
        # minsInSec = 25*60
        # deltaMins = datetime.timedelta(seconds=minsInSec)
        # printDistanceLog = True
        # defectDistance = 0
        while True:
            top_fls=sorted(glob.glob(os.path.join(RAW_IMAGE_DIR_PATH,"TOP*.jpg")))
            bottom_fls=sorted(glob.glob(os.path.join(RAW_IMAGE_DIR_PATH,"BOTTOM*.jpg")))

            while len(top_fls) > 0 or len(bottom_fls) > 0:
                t1_start = time.perf_counter()
                try:
                    if len(top_fls) > 0:
                        imgFilePath=top_fls[0]
                        if (time.time()-os.path.getmtime(imgFilePath))>30 and os.path.exists(imgFilePath):
                            isDeleted = delete_zero_size_jpg(imgFilePath, logger)
                            if isDeleted is False:
                                # imgFilePath="/home/insightzz-conveyor-02/actual_defect/belt patch image/IMG_2024_10_01_21_26_47.jpg"
                                camImage = cv2.imread(imgFilePath)

                                defectDistance = get_distance_from_filename(imgFilePath)
                                print("distance Top:-",defectDistance)

                                # print("Top Image",imgFilePath)
                                #camImageCopy = camImage.copy()  #''' COmments : Can be copy after defect is found'''
                                # marking_labellist=marking_cls.run_inference(camImage)

                                # for item in marking_labellist:
                                #     labelName = item[0]
                                #     if labelName == "J":
                                #         ''' Resetting the distance based on Number Detection '''
                                #         timeDiff = datetime.datetime.now() - conveyorLoopResetTime
                                #         if timeDiff > deltaMins:
                                #             conveyorLoopResetTime = datetime.datetime.now()
                                #             logger.debug(f"Conveyor Time Reset Done at : {conveyorLoopResetTime}")
                                        
                                #     if labelName in ["J","dash","SIX","ONE","TWO","ZERO","FOUR","FIVE","THREE","SEVEN","EIGHT"]:
                                #         dt=datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
                                #         cv2.imwrite(f"{configHashMap.get(CONFIG_KEY_NAME.MARKING_DETECT_LABEL_NAMES)}/IMG_MARKING_{dt}.jpg",camImage)
                                        
                                # ''' Calculate Distance '''
                                # speed = getConveyorSpeed(logger)/60
                                # defectTimeDiff = datetime.datetime.now() - conveyorLoopResetTime
                                # defectTimeDiffSec = defectTimeDiff.total_seconds()
                                # defectDistance = int(speed * defectTimeDiffSec)
                                # if printDistanceLog is True:
                                #     logger.debug(f"Distance Calculated is : {defectDistance}, Speed is : {speed}, Time Diff : {defectTimeDiffSec}")
                                #     logger.debug(f"Distance Calculated is : {defectDistance}, Speed is : {speed}, Time Diff : {defectTimeDiffSec}")
                                

                                # if defectDistance > totalLength:
                                #     defectDistance = 1
                                #     conveyorLoopResetTime = datetime.datetime.now()

                                try:
                                    labellist = maskObj.run_inference(camImage)
                                    if len(labellist) > 0:
                                        db_flag += 1
                                        logger.debug("db__flag inside = %s",db_flag)
                                        tmpList = []
                                        for tmpItem in labellist:
                                            tmpList.append(tmpItem[0])
                                        logger.info(f"Defects Found in Image  {os.path.basename(imgFilePath)}: {tmpList}")
                                        for item in labellist:
                                            labelName = item[0]
                                            xmin = item[1]
                                            ymin = item[2]
                                            xmax = item[3]
                                            ymax = item[4]
                                            cx = item[6]
                                            cy = item[7]
                                            roi = xmin,ymin,xmax,ymax
                                            DEFECT_SIDE = "TOP"
                                            DEFECT_LOCATION = ""
                                            if labelName in DETECT_LABEL_NAMES:
                                                DEFECT_TYPE = labelName
                                                if cy <= 350:
                                                    DEFECT_LOCATION = "LEFT"
                                                elif cy >350 and cy <= 600:
                                                    DEFECT_LOCATION = "MIDDLE"
                                                elif cy > 600:
                                                    DEFECT_LOCATION = "RIGHT"                            
                                                
                                                DEFECT_SIZE_X = xmax - xmin
                                                DEFECT_SIZE_Y = ymax - ymin 

                                                isDefect = checkIfDefectSizeIsValid(DEFECT_SIDE,DEFECT_SIZE_X,DEFECT_SIZE_Y,labelName,logger, cur, dbConn)
                                                if isDefect is True :
                                                    currDatePath = getDirectoryPath(DEFECTIVE_IMG_FILE_PATH)
                                                    currSideDirPath = getSubDirectoryPath(currDatePath, DEFECT_SIDE)
                                                    isRepeatedDefect = checkIfDefectIsRepeated(DEFECT_SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax, NonRepeatedDefectList,logger)
                                                    camImageCopy_img = camImage.copy()
                                                    drawCV2Box(camImageCopy_img,labelName,xmin,ymin,xmax,ymax,logger)
                                                    if isRepeatedDefect is True:
                                                        ParentDirName = getSubDirectoryPath(currSideDirPath, "REPEATED_DEFECT")
                                                        IMAGE_PATH_TMP_RAW = f"{ParentDirName}REPEATED_DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                        cv2.imwrite(IMAGE_PATH_TMP_RAW, camImageCopy_img)
                                                        logger.debug(f"Repeated defect found is : Defect Name {DEFECT_TYPE}, Coordinates : ({xmin}, {ymin}) , ({xmax}, {ymax})")
                                                    else:
                                                        # isRepeatedDBDefect = checkDefectInDBForRepeated(DEFECT_SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax, logger, cur, dbConn)
                                                        isRepeatedDBDefect = checkDefectInDBForRepeated(DEFECT_SIDE, DEFECT_TYPE, defectDistance,logger, cur, dbConn)

                                                        if isRepeatedDBDefect is False:
        
                                                            if not os.path.exists(DEFECTIVE_IMG_FILE_PATH):
                                                                mkdir(DEFECTIVE_IMG_FILE_PATH)
                                                            ParentDirName = getSubDirectoryPath(currSideDirPath, "DEFECT")
                                                            #ParentDirName = currSideDirPath + "DEFECT/"
                                                            IMAGE_PATH_TMP = f"{ParentDirName}DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                            logger.info("Image name = %s", IMAGE_PATH_TMP)
                                                            logger.info("DEFECT_SIDE = %s", DEFECT_SIDE)
                                                            logger.info("DEFECT_TYPE = %s", DEFECT_TYPE)
                                                            logger.info("xmin = %s", xmin)
                                                            logger.info("xmax = %s", xmax)
                                                            logger.info("ymin = %s", ymin)
                                                            logger.info("ymax= %s", ymax)
                                                            cv2.imwrite(IMAGE_PATH_TMP, camImageCopy_img)
                                                            ParentDirName = getSubDirectoryPath(currSideDirPath, "DEFECT_RAW")
                                                            #ParentDirName = currSideDirPath + "DEFECT_RAW/"
                                                            IMAGE_PATH_TMP_RAW = f"{ParentDirName}DEFECT_RAW_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                            cv2.imwrite(IMAGE_PATH_TMP_RAW, camImage)
                                                            
                                                            insertDefectDetailsIntoImageProcessingTable(DEFECT_TYPE,DEFECT_LOCATION,DEFECT_SIZE_X,DEFECT_SIZE_Y,DEFECT_SIDE,IMAGE_PATH_TMP, xmin, ymin, xmax, ymax, defectDistance, logger, cur, dbConn)
                                                        else:
                                                            ParentDirName = getSubDirectoryPath(currSideDirPath, "REPEATED_DEFECT")
                                                            #ParentDirName = currSideDirPath + "REPEATED_DEFECT/"
                                                            IMAGE_PATH_TMP_RAW = f"{ParentDirName}REPEATED_DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                            cv2.imwrite(IMAGE_PATH_TMP_RAW, camImageCopy_img)
                                                            logger.debug(f"Repeated DB defect found is : Defect Name {DEFECT_TYPE}, Coordinates : ({xmin}, {ymin}) , ({xmax}, {ymax})")
                                                else:
                                                    logger.debug("No Defects Were Found in Top Side !!!!")
                                            else:
                                                logger.debug("No Defects Were Found in Top Side !!!!")
                                    else:
                                        logger.debug("No Detection found in the Top image ")

                                    tmpFileName=imgFilePath[imgFilePath.rfind("/")+1:]
                                    ''' Save Raw Image in a Temp Directory '''
                                    frameCycleStartDT = getFrameGrabbingStartDatetime(logger)
                                    currDatePath = getDirectoryPath(TEMP_RAW_IMAGE_DIR_PATH)
                                    currFrameCycleStartDirPath = getSubDirectoryPath(currDatePath, frameCycleStartDT)
                                    os.rename(imgFilePath,os.path.join(currFrameCycleStartDirPath,tmpFileName))        
                                except Exception as e:
                                    logger.error(f"mainFunction() Image is not Complete : {e}")
                                    #raise Exception(f"mainFunction 2 {e}")
                            else:
                                logger.error("Image size is zero for top camera %s", imgFilePath)
                                

                    t1_stop = time.perf_counter()
                    logger.info("Time for Top = %s",t1_stop-t1_start)
                    
                    t2_start = time.perf_counter()
                    if len(bottom_fls) > 0:
                        imgFilePath=bottom_fls[0]
                        if (time.time()-os.path.getmtime(imgFilePath))>30 and os.path.exists(imgFilePath):
                            isDeleted = delete_zero_size_jpg(imgFilePath, logger)
                            if isDeleted is False:
                                camImage = cv2.imread(imgFilePath)
                                defectDistance = get_distance_from_filename(imgFilePath)
                                print("Distance: Bottom -",defectDistance)

                                camImageCopy = camImage.copy()
                                logger.info("Fetching data in main loop")
                                try:
                                    NonRepeatedDefectList = []
                                    NonRepeatedDefectList = fetchRepeatedDefectList(logger, cur, dbConn)
                                except:
                                    logger.error("Error in fetching NonRepeatedDefectList for bottom")
                                    #raise Exception(f"raise error from NonRepeatedDefectList for bottom {e}")

                                try:
                                    labellist = maskObj.run_inference(camImage)
                                    logger.debug(labellist)
                                    if len(labellist) > 0:                                            
                                        tmpList = []
                                        for tmpItem in labellist:
                                            tmpList.append(tmpItem[0])
                                        logger.info(f"Defects Found in Image  {os.path.basename(imgFilePath)}: {tmpList}")

                                        for item in labellist:
                                            labelName = item[0]
                                            xmin = item[1]
                                            ymin = item[2]
                                            xmax = item[3]
                                            ymax = item[4]
                                            cx = item[6]
                                            cy = item[7]
                                            roi=xmin,ymin,xmax,ymax
                                            DEFECT_SIDE = "BOTTOM"
                                            DEFECT_LOCATION = ""
                                            if labelName in DETECT_LABEL_NAMES:
                                                DEFECT_TYPE = labelName
                                                if cy <= 350:
                                                    DEFECT_LOCATION = "LEFT"
                                                elif cy >350 and cy <= 600:
                                                    DEFECT_LOCATION = "MIDDLE"
                                                elif cy > 600:
                                                    DEFECT_LOCATION = "RIGHT"                            
                                                
                                                DEFECT_SIZE_X = xmax - xmin
                                                DEFECT_SIZE_Y = ymax - ymin 

                                                isDefect = checkIfDefectSizeIsValid(DEFECT_SIDE,DEFECT_SIZE_X,DEFECT_SIZE_Y,labelName,logger, cur, dbConn)
                                                if isDefect is True:
                                                    currDatePath = getDirectoryPath(DEFECTIVE_IMG_FILE_PATH)
                                                    currSideDirPath = getSubDirectoryPath(currDatePath, DEFECT_SIDE)
                                                    isRepeatedDefect = checkIfDefectIsRepeated(DEFECT_SIDE, DEFECT_TYPE, xmin, ymin, xmax, ymax, NonRepeatedDefectList,logger)
                                                    camImageCopy_img = camImage.copy()
                                                    drawCV2Box(camImageCopy_img,labelName,xmin,ymin,xmax,ymax,logger)
                                                    if isRepeatedDefect is True:
                                                        ParentDirName = getSubDirectoryPath(currSideDirPath, "REPEATED_DEFECT")
                                                        #ParentDirName = currSideDirPath + "REPEATED_DEFECT/"
                                                        IMAGE_PATH_TMP_RAW = f"{ParentDirName}REPEATED_DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                        cv2.imwrite(IMAGE_PATH_TMP_RAW, camImageCopy_img)

                                                        logger.debug(f"Repeated defect found is : Defect Name {DEFECT_TYPE}, Coordinates : ({xmin}, {ymin}) , ({xmax}, {ymax})")
                                                    else:
                                                        isRepeatedDBDefect = checkDefectInDBForRepeated(DEFECT_SIDE, DEFECT_TYPE, defectDistance,logger,cur, dbConn)
                                                        if isRepeatedDBDefect is False:
                                                            if not os.path.exists(DEFECTIVE_IMG_FILE_PATH):
                                                                mkdir(DEFECTIVE_IMG_FILE_PATH)
                                                            
                                                            ParentDirName = getSubDirectoryPath(currSideDirPath, "DEFECT")
                                                            #ParentDirName = currSideDirPath + "DEFECT/"
                                                            IMAGE_PATH_TMP = f"{ParentDirName}DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                            logger.info("Image name = %s", IMAGE_PATH_TMP)
                                                            logger.info("DEFECT_SIDE = %s", DEFECT_SIDE)
                                                            logger.info("DEFECT_TYPE = %s", DEFECT_TYPE)
                                                            logger.info("xmin = %s", xmin)
                                                            logger.info("xmax = %s", xmax)
                                                            logger.info("ymin = %s", ymin)
                                                            logger.info("ymax= %s", ymax)
                                                            cv2.imwrite(IMAGE_PATH_TMP, camImageCopy_img)
                                                            ParentDirName = getSubDirectoryPath(currSideDirPath, "DEFECT_RAW")
                                                            #ParentDirName = currSideDirPath + "DEFECT_RAW/"
                                                            IMAGE_PATH_TMP_RAW = f"{ParentDirName}DEFECT_RAW_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                            cv2.imwrite(IMAGE_PATH_TMP_RAW, camImage)
                                                            
                                                            insertDefectDetailsIntoImageProcessingTable(DEFECT_TYPE,DEFECT_LOCATION,DEFECT_SIZE_X,DEFECT_SIZE_Y,DEFECT_SIDE,IMAGE_PATH_TMP, xmin, ymin, xmax, ymax, defectDistance, logger, cur, dbConn)
                                                        else:
                                                            ParentDirName = getSubDirectoryPath(currSideDirPath, "REPEATED_DEFECT")
                                                            #ParentDirName = currSideDirPath + "REPEATED_DEFECT/"
                                                            IMAGE_PATH_TMP_RAW = f"{ParentDirName}REPEATED_DEFECT_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}.jpg"
                                                            cv2.imwrite(IMAGE_PATH_TMP_RAW, camImageCopy_img)
                                                            logger.debug(f"Repeated DB defect found is : Defect Name {DEFECT_TYPE}, Coordinates : ({xmin}, {ymin}) , ({xmax}, {ymax})")
                                                else:
                                                    logger.debug("No Defects Were Found in Bottom Side !!!!")
                                            else:
                                                logger.debug("No Defects Were Found in Bottom Side !!!!")
                                    else:
                                        logger.debug("No detection found in Bottom image")

                                    tmpFileName=imgFilePath[imgFilePath.rfind("/")+1:]
                                    ''' Save Raw Image in a Temp Directory '''
                                    frameCycleStartDT = getFrameGrabbingStartDatetime(logger)
                                    currDatePath = getDirectoryPath(TEMP_RAW_IMAGE_DIR_PATH)
                                    currFrameCycleStartDirPath = getSubDirectoryPath(currDatePath, frameCycleStartDT)
                                    os.rename(imgFilePath,os.path.join(currFrameCycleStartDirPath,tmpFileName))        
                                except Exception as e:
                                    logger.error(f"mainFunction() Image is not Complete : {e}")
                                    #raise Exception(f"mainfunction 1 {e}")
                            else:
                                logger.error("Image size is zero for bottom camera %s", imgFilePath)
                                
                                
                    t2_stop = time.perf_counter()
                    logger.info("Time for bottom = %s",t2_stop-t2_start)
                    
                    ''' Reloading the images '''
                    top_fls=sorted(glob.glob(os.path.join(RAW_IMAGE_DIR_PATH,"TOP*.jpg")))
                    bottom_fls=sorted(glob.glob(os.path.join(RAW_IMAGE_DIR_PATH,"BOTTOM*.jpg")))
                    
                    if error_flag == 1:
                        break
                    if db_flag == 1000:
                        break
                except Exception as e:
                    logger.error(f"mainFunction() Inner Exception in Image inference : {e}")
                    #raise Exception(f"raise error from main loop image process {e}")
            else:
                db_flag += 1
                time.sleep(1)
            logger.info("db_flag = %s",db_flag)
            if error_flag == 1:
                break
            if db_flag == 1000:
                logger.info("Closing DB Connection")
                break
            time.sleep(1)
    except Exception as e:
        logger.error(f"mainFunction() Exception is : {e}")
        logger.critical(f"mainFunction() Traceback Exception is : {traceback.format_exc()}")
        #raise Exception(f"raise error from main function {e}")
        
def fetchDefectList(logger, cur, dbConn):
    global error_flag
    data_set = []
    try:
        query = f"SELECT * FROM DEFECT_TYPE_DETAIL_TABLE order by ID asc;"
        cur.execute(query)
        data_set = cur.fetchall()
    except Exception as e:
        error_flag = 1
        logger.error(f"fetchDefectList() Exception is : {e}")
        #raise Exception(f"raise error from fetchDefectList {e}")
    finally:
        return data_set

def convertListToMap(data_set, logger):
    try:
        top_dict = {}
        bottom_dict = {}
        
        for item in data_set:
            DEFECT_SIDE = item[1]
            DEFECT_TYPE = item[2]
            DEFECT_SIZE_X = item[3]
            DEFECT_SIZE_Y = item[4]
            
            if DEFECT_SIDE == "TOP":
                top_dict[DEFECT_TYPE] = [DEFECT_SIZE_X,DEFECT_SIZE_Y]
            else:
                bottom_dict[DEFECT_TYPE] = [DEFECT_SIZE_X,DEFECT_SIZE_Y]
            
    except Exception as e:
        logger.debug(f"convertListToMap() Exception is : {e}")
    
    return top_dict, bottom_dict

def checkIfDefectSizeIsValid(defectSide,sizeX,sizeY,labelName,logger, cur, dbConn):
    isDefect = False
    try:
        data_set = fetchDefectList(logger, cur, dbConn)
        # print(data_set)
        top_dict, bottom_dict = convertListToMap(data_set, logger)
        # print(top_dict,bottom_dict)
        if defectSide == "TOP":
            config_values = top_dict.get(labelName)
            if config_values is None:
                isDefect = True
            elif config_values[0] == "ANY" or config_values[1] == "ANY":
                isDefect = True
            elif sizeX > int(config_values[0]) or sizeY > int(config_values[1]):
                isDefect = True
        else:
            config_values = bottom_dict.get(labelName)
            if config_values is None:
                isDefect = True
            elif config_values[0] == "ANY" or config_values[1] == "ANY":
                isDefect = True
            elif sizeX > int(config_values[0]) or sizeY > int(config_values[1]):
                isDefect = True
                
    except Exception as e:
        logger.error(f"checkIfDefectSizeIsValid() Exception is : {e}")
        #raise Exception(f"raise error from checkIfDefectSizeIsValid {e}")
    return isDefect

def updateProcessID(logger, cur, dbConn):
    try:
        query = f"update PROCESS_ID_TABLE set PROCESS_ID='{ALGO_PROCESS_ID}' where PROCESS_NAME ='ALGORITHM'"
        cur.execute(query)
        dbConn.commit()
    except Exception as e:
        logger.critical("updateProcessID() Exception is : "+ str(e))
        #raise Exception(f"raise error from updateProcessID {e}")
    finally:
        cur.close()

# def on_message(client, userdata, message):
#     """Handles incoming MQTT messages and updates defectDistance."""
#     global defectDistance
#     try:
#         data = json.loads(message.payload.decode().strip())
#         distance_value = data.get("distance")

#         if isinstance(distance_value, (int, float)) and distance_value > 0:
#             defectDistance = distance_value
#             logging.debug(f"Distance updated: {defectDistance} meters")
#         else:
#             logging.warning("Invalid distance received")

#     except json.JSONDecodeError:
#         logging.error("Error decoding JSON")

# def initMqttClient():
#     """Initializes and starts the MQTT client."""
#     global configHashMap
#     client = mqtt.Client()
#     client.on_message = on_message

#     try:
#         client.connect(configHashMap.get(CONFIG_KEY_NAME.MQTT_BROKER), configHashMap.get(CONFIG_KEY_NAME.MQTT_PORT), 60)
#         logging.info("MQTT Connected")
#         client.subscribe(configHashMap.get(CONFIG_KEY_NAME.MQTT_TOPIC))
#         client.loop_start()
#         return client
#     except Exception as e:
#         logging.error(f"MQTT Connection Error: {e}")
#         return None         

def main():
    global configHashMap
    global error_flag
    configure_db = 0
    loadConfiguration()

    ''' Initializing Logger '''
    logger = logging.getLogger("Insightzz")  # Create logger instance
    logger.setLevel(logging.INFO)  # Set the logging level to DEBUG
    log_file = configHashMap.get(CONFIG_KEY_NAME.LOG_FILE_PATH) + os.path.basename(__file__[:-2]) + "log"  # Path for the log file
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = TimedRotatingFileHandler(
        log_file, when='midnight', backupCount=7)  # Rotate log files at midnight, keep 7 backups
    MY_HANDLER = logging.StreamHandler()

    # MY_HANDLER.setFormatter(formatter)
    # logger.addHandler(MY_HANDLER)

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("Algorithm logger Module Initialized")
    logger.info("Algorithm Module version : %s", version)

    db_configuration = 0
    dbConn = None
    cur = None

    while True:
        try:
            if db_configuration == 0:
                dbConn = getDatabaseConnection(logger)
                if dbConn != None:
                    cur = dbConn.cursor()
                    db_configuration = 1
            else:
                #updateProcessID(logger, cur, dbConn)
                # initGEmailModule()
                mainFunction(logger, cur, dbConn)
                if error_flag == 1:
                    error_flag = 0

                logger.critical("DB error. Creating new instance.")                    
                if cur is not None:
                    cur.close()
                if dbConn is not None:
                    dbConn.close()
                db_configuration = 0
            time.sleep(1)
        except Exception as e:
            if cur is not None:
                cur.close()
            if dbConn is not None:
                dbConn.close()
            db_configuration = 0
            logger.critical("Main Exception is : "+ str(e))
            logger.critical(traceback.format_exc())


if __name__=="__main__":
    main()
