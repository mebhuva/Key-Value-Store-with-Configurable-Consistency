import sys
import socket
import pickle
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import configkeyvalue_pb2
 

if __name__ == '__main__':
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('128.226.114.203', 9000))
	putrequestmessage = configkeyvalue_pb2.ClientPutRequest()
        putrequestmessage.id = 1
        putrequestmessage.key = 61
	putrequestmessage.value = "HUM"
        putrequestmessage.consistencylevel = 1
        message = configkeyvalue_pb2.KeyValueMessage()
        message.clientputrequest.CopyFrom(putrequestmessage)
	s.sendall(pickle.dumps(message))
	s.close()
		