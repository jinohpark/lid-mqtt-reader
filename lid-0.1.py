
from struct import unpack_from
import sys
import os
import time
import json
import base64
import binascii
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import sqlalchemy
import shutil
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from pathlib import Path
from influxdb import InfluxDBClient
from collections import namedtuple

import logging
logger = logging.getLogger('cable_log')

logging.basicConfig(filename='cable.log', level=logging.INFO)

# MQTT_HOST = '222.122.197.15'
# MQTT_PORT = 1883

PACKET_HEADER = '5351'
C_END     = "\033[0m"
C_BOLD    = "\033[1m"
C_INVERSE = "\033[7m"
 
C_BLACK  = "\033[30m"
C_RED    = "\033[31m"
C_GREEN  = "\033[32m"
C_YELLOW = "\033[33m"
C_BLUE   = "\033[34m"
C_PURPLE = "\033[35m"
C_CYAN   = "\033[36m"
C_WHITE  = "\033[37m"
 
C_BGBLACK  = "\033[40m"
C_BGRED    = "\033[41m"
C_BGGREEN  = "\033[42m"
C_BGYELLOW = "\033[43m"
C_BGBLUE   = "\033[44m"
C_BGPURPLE = "\033[45m"
C_BGCYAN   = "\033[46m"
C_BGWHITE  = "\033[47m"

MQTT_HOST = 'localhost'
MQTT_PORT = 1883

DB_HOST = 'localhost'
MEASURE_LOG_FILE = 'influx.log'
PACKET_LENGTH = 44




merged = ""
pre_FCnt = -1 


PACKET_START = 1
PACKET_MIDDLE = 2
PACKET_COMPLETE = 3
PACKET_ERROR = 99


# class SensorNode:
#     def __init__(self, name, eui, leff, mass):
#         self.eui = eui
#         self.name = name
#         self.leff = leff
#         self.mass = mass
#         self.packet_str = ""
#         self.bytes = b''
#         self.f_cnt = -1

class SensorNode:
    def __init__(self, name, eui):
        self.eui = eui
        self.packet_str = ""
        self.name = name

sn_lid_03 = SensorNode('lid_03', 'ac1f09fffe015675')
sn_lid_07 = SensorNode('lid_07', 'ac1f09fffe01566c')
sn_lid_09 = SensorNode('lid_09', 'ac1f09fffe015673')
sn_lid_11 = SensorNode('lid_11', '60c5a8fffe798f4e')


sn_list = [sn_lid_03, sn_lid_07, sn_lid_09, sn_lid_11]

def get_target_node(eui):

    global sn_list
    #print("In get_target_node for device[{}]".format(eui))
    for obj in sn_list:
        if eui == obj.eui:
            print("Found Node!!", obj.eui, obj.name, obj.packet_str)
            return obj
    return

def connect_influxDB():
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database
    return

def packet_decode(packet_data):
    dec_data = base64.b64decode(packet_data)
    len_dec_data = len(dec_data)
    return dec_data

def mqtt_reader_start():
    print("Started reading MQTT Message")
#    test_influx_input()
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(host=MQTT_HOST, port=MQTT_PORT)
    client.loop_forever()
    pass

def on_connect(client, userdata, flags, rc):
    print("MQTT Connected with result code " + str(rc))
    #client.subscribe("application/+/device/+/event/up")
    client.subscribe("application/+/device/+/rx")
    #client.subscribe("gateway")
    #client.subscribe("cable/data")
    #client.subscribe("application/1/devicet/+/event/up")
    print("Subscription Done.")


def write_device_status(rx_datetime, node,dev_rssi, dev_dr):
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('lid1')
    json_body = [
        {
            "measurement": "lora_dev_stats",
            "tags": {
                "NODE": node,
                },
                "time": rx_datetime,
                "fields": {
                    "DR": dev_dr,
                    "RSSi" : dev_rssi
                }
            }
        ]
    client.write_points(json_body)
    return

def is_packet_completed(packet):
    if packet[0:4] == PACKET_HEADER and len(packet) is  PACKET_LENGTH:
        return True
    else: 
        return False

def is_start_of_packet(packet):
    if packet[0:4] == PACKET_HEADER and len(packet) < PACKET_LENGTH:
        return True
    else:
        return False

def check_packet_status(packet, name):

    packet_satus = 99

    if len(packet) == PACKET_LENGTH:
        if packet[0:4] == PACKET_HEADER:
            print(C_BGBLUE+"[{}]Packet Completed".format(name)+C_END, packet)
            packet_status = PACKET_COMPLETE
            return packet_status
        else:
            packet_status = PACKET_ERROR
            print(C_BGBLUE+"[{}]Packet ERROR".format(name)+C_END, packet)
            return packet_status
    elif len(packet) < PACKET_LENGTH:
        if packet[0:4] == PACKET_HEADER:
            packet_satus = PACKET_START
        else:
            pass
    return packet_satus



def temp_get_from_packet(packet, dev_eui):
    print(C_BGPURPLE+"in get_data_from_packet and packet length is"+C_END, len(packet)) 

# def get_data_from_packet(packet, dev_eui, rxdatetime, target_node):
def get_data_from_packet(packet, dev_eui, target_node):

    #now = datetime.utcnow()
    now = datetime.now()
    nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
    header_packet = packet[0:4]
    print(packet)
    print("[{}]in get_data_from_packet and packet length is".format(nowDatetime), len(packet), packet) 
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('lid1')

    idx = int(packet[6:8] +   packet[4:6], 16)
    soil1_temp = int(packet[10:12] + packet[8:10], 16)
    soil1_vwc = int(packet[14:16] + packet[12:14], 16)
    soil2_temp = int(packet[18:20] + packet[16:18], 16)
    soil2_vwc = int(packet[22:24] + packet[20:22], 16)
    water_level = int(packet[26:28] + packet[24:26], 16)
    battery_level = int(packet[30:32] + packet[28:30], 16)
    nError = int(packet[34:36] + packet[32:34], 16)
    status = int(packet[38:40] + packet[36:38], 16)
    check_sum = int(packet[42:44] + packet[40:42], 16)


    # Water Level Value in mm

    water_level_v = int(0.814725 * water_level + -453.476)
    soil1_temp_v = soil1_temp / 100
    soil2_temp_v = soil2_temp / 100

    json_body = [
        {
            "measurement": "lid"+dev_eui,
            "tags": {
            "eui": target_node.eui
            },
            "time": now,
            "fields": {
            "s1_temp": soil1_temp_v, 
            "s1_vwc": soil1_vwc,
            "s12_temp": soil2_temp_v,
            "s12_vwc": soil2_vwc,
            "water_level": water_level_v,
            "battery_level": battery_level,
            }
        }
    ]

    client.write_points(json_body)
    print("--------------------------json body for {}[{}]--------------------------- ".format(target_node.name, dev_eui))

    print("[{}].soil1.temp: {}".format(idx, soil1_temp))
    print("[{}].soil1.humidity: {}".format(idx, soil1_vwc))
    print("[{}].soil2.temp: {}".format(idx, soil2_temp))
    print("[{}].soil2.humidity: {}".format(idx, soil2_vwc))
    print("[{}].water_level: {}".format(idx, water_level))
    print("[{}].battery_level: {}".format(idx, battery_level))


    return


def on_message(client, userdata, msg):
    global merged
    global pre_FCnt
    payload = msg.payload.decode('utf8')
    j_msg = json.loads(payload)
    gw_timestamp = datetime.now().timestamp()

    payload_data = j_msg['data']
    dev_eui = j_msg['devEUI']
    dev_adr = j_msg['adr']
    dev_fCnt = j_msg['fCnt']
    dev_rxinfo = j_msg['rxInfo']
    dev_rssi = j_msg['rxInfo'][0]['rssi']
    dev_txInfo = j_msg['txInfo']
    dev_txInfo_dr = j_msg['txInfo']['dr']
    dec_data = ""
    tmp_packet_str = ""

    #print("Got message for ", dev_eui, j_msg)

    # packet data header sting 식별 기능 포함되야 
    # packet 이 손실되는 케이스 발생하는지? error 의 경우 앞이 손실되는 것으로 보임
    # packet size 가 커질 때는 reset 해줄 수 있어야.


    obj = get_target_node(dev_eui)
    if obj: 
        #now = datetime.utcnow()
        now = datetime.now()
        nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
        write_device_status(now, obj.name, dev_rssi, dev_txInfo_dr)

        print("Get A Node ")

        if payload_data != None:
            # write_device_status(now, obj.name, dev_rssi, dev_txInfo_dr)
            dec_data = packet_decode(payload_data).hex()


            print("Payload  ")

            logging.info("[{}][{}.({})]:{}".format(nowDatetime, obj.name,dev_fCnt, dec_data))
            # Removing dummy packet
            if '44756d6d79' in dec_data:
                print("[{}-{}]Dummy Packet included: {}".format(dev_eui,dev_fCnt, dec_data))
                dec_data = dec_data.replace('44756d6d79','')
                print("[{}-{}]Dummy removed and dec_data[{}]".format(dev_eui, dev_fCnt, len(dec_data), dec_data))
            else:
                pass

            if '756d6d79' in dec_data:
                print("[{}-{}]Dummy Packet substring included: {}".format(dev_eui,dev_fCnt, dec_data))
                dec_data = dec_data.replace('756d6d79','')
                print("[{}-{}]Dummy substring removed and dec_data[{}]".format(dev_eui, dev_fCnt, len(dec_data), dec_data))
            else:
                pass

            if '6d6d79' in dec_data:
                print("[{}-{}]Dummy Packet substring included: {}".format(dev_eui,dev_fCnt, dec_data))
                dec_data = dec_data.replace('6d6d79','')
                print("[{}-{}]Dummy substring removed and dec_data[{}]".format(dev_eui, dev_fCnt, len(dec_data), dec_data))
            else:
                pass


            if '44756d6d' in dec_data:
                print("[{}-{}]Dummy Packet substring included: {}".format(dev_eui,dev_fCnt, dec_data))
                dec_data = dec_data.replace('44756d6d','')
                print("[{}-{}]Dummy substring removed and dec_data[{}]".format(dev_eui, dev_fCnt, len(dec_data), dec_data))
            else:
                pass

            if '44756d' in dec_data:
                print("[{}-{}]Dummy Packet substring included: {}".format(dev_eui,dev_fCnt, dec_data))
                dec_data = dec_data.replace('44756d','')
                print("[{}-{}]Dummy substring removed and dec_data[{}]".format(dev_eui, dev_fCnt, len(dec_data), dec_data))
            else:
                pass

            if '4475' in dec_data:
                print("[{}-{}]Dummy Packet substring included: {}".format(dev_eui,dev_fCnt, dec_data))
                dec_data = dec_data.replace('4475','')
                print("[{}-{}]Dummy substring removed and dec_data[{}]".format(dev_eui, dev_fCnt, len(dec_data), dec_data))
            else:
                pass

            if len(dec_data) > 0:

                print(C_CYAN+"[{}]in packet:{}".format(nowDatetime, dec_data)+C_END )
                print(C_CYAN+"obj str is"+C_END,obj.packet_str)

                if len(obj.packet_str) > 0:
                    if dec_data[0:4] == PACKET_HEADER:
                        print(C_CYAN+"replacing previous packet"+C_END)
                        obj.packet_str = dec_data
                    else:
                        print(C_CYAN+"Merging..........."+C_END)
                        obj.packet_str = obj.packet_str + dec_data
                else:
                    if dec_data[0:4] == PACKET_HEADER:
                        print(C_CYAN+"Start of new packet string"+C_END)
                        obj.packet_str = dec_data
                    else:
                        pass
            else:
                pass

            print(C_BLUE+"obj string of length[{}] is".format(len(obj.packet_str))+C_END, obj.packet_str )
            # obj.packet_str = obj.packet_str + dec_data
            packet_status = check_packet_status(obj.packet_str, obj.name)


            print("[{}]:Payload not dummy:{} merged:{}".format(dev_fCnt, dec_data, obj.packet_str))

            if packet_status == PACKET_COMPLETE:
                print("Packet Merging completed and length is {}".format(len(obj.packet_str)))
                # rx_sec = int(obj.packet_str[6:8], 16)
                # rx_min = int(obj.packet_str[8:10], 16)
                # rx_hour = int(obj.packet_str[10:12], 16)
                # rx_h12 = int(obj.packet_str[12:14], 16)
                # rx_date = int(obj.packet_str[14:16], 16)
                # rx_month = int(obj.packet_str[16:18], 16)
                # rx_year = int(obj.packet_str[18:20], 16)


                # rx_datetime_str = str(rx_year)+'/'+str(rx_month)+'/'+str(rx_date)+' ' + \
                # str(rx_hour)+':'+str(rx_min)+':'+str(rx_sec)
                # print("rx_datetime_str:", rx_datetime_str)


                # rx_datetime = datetime.strptime(rx_datetime_str,"%y/%m/%d %H:%M:%S%z+09:00")
                # rx_datetime = datetime.strptime(rx_datetime_str,"%y/%m/%d %H:%M:%S") - timedelta(hours=8)

                # print("rx_datetime is", rx_datetime)
                get_data_from_packet(obj.packet_str, dev_eui, obj)
                # Packet reset
                obj.packet_str = ""
                merge_finished = True
            else:
                if packet_status == PACKET_ERROR:
                    print("Packet Err({}):{}".format(len(obj.packet_str), obj.packet_str))
                    obj.packet_str = ""
                else:
                    if packet_status == PACKET_START:
                        print("New Packet Started[{}]:{}".format(dev_fCnt, dec_data))

                    print("Packet Merging[{}] size[{}]:{}:".format(packet_status, len(obj.packet_str), obj.packet_str))
            
            obj.f_cnt= dev_fCnt
        else:
            print(dev_eui, C_RED+"[{}]No Payload Data".format(dev_eui)+C_END)

    
mqtt_reader_start()

