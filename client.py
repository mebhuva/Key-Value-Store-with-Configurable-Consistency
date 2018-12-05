import sys
import socket
import time
import pickle
import logging
import re
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import configkeyvalue_pb2
class Client(object):
	def sendputrequest(self,key,value,consistency,HOST,PORT):
		print HOST
		print PORT
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	s.connect((HOST, int(PORT)))
		putrequestmessage = configkeyvalue_pb2.ClientPutRequest()
        	putrequestmessage.id = 1000
        	putrequestmessage.key = key
		putrequestmessage.value = value
        	putrequestmessage.consistencylevel = consistency
        	message = configkeyvalue_pb2.KeyValueMessage()
        	message.clientputrequest.CopyFrom(putrequestmessage)
		s.sendall(pickle.dumps(message))
		s.close()
	def sendgetrequest(self,key,consistency,HOST,PORT):
		print HOST
		print PORT
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	s.connect((HOST, int(PORT)))
		getrequestmessage = configkeyvalue_pb2.ClientGetRequest()
        	getrequestmessage.id = 1000
        	getrequestmessage.key = key
        	getrequestmessage.consistencylevel = consistency
        	message = configkeyvalue_pb2.KeyValueMessage()
        	message.clientgetrequest.CopyFrom(getrequestmessage)
		s.sendall(pickle.dumps(message))
		data = pickle.loads(s.recv(10000))
		print data
		s.close()
		

	 

if __name__ == '__main__':
	if len(sys.argv) != 3:
        	print "Invalid Parameters: <Replica IP> <Replica Port>"
        	sys.exit(0)
        else:
        	HOST = sys.argv[1]
        	PORT = sys.argv[2]

	print "Connected to replica " + str(HOST) + ":" + str(PORT)
	print "\nAvailable commands:"
	print "PUT <consistency_level> <key> <value>	(Ex. PUT 2 60 'my data')"
	print "GET <consistency_level> <key> <value>	(Ex. GET 2 60)"
	print "EXIT"
	# console for user input
	while True:

		rawuserinput = raw_input("\nPlease enter one of the above commands:\n")

		if rawuserinput != "" :
			userinput = rawuserinput.split()
	        	if userinput[0] == "GET":
        	        	try:
                	        	key = int(userinput[2])
                	        	consistency = int(userinput[1])
					Client().sendgetrequest(key,consistency,HOST,PORT)
					#print "Replica Response " + str(Client().sendgetrequest(key,consistency,HOST,PORT))
                		except:
                	        	print "ERROR! Invalid GET command."

	        	elif userinput[0] == "PUT":
				try:
        	                	key = int(userinput[2])
        	                	value = re.search('[\'"](.*)[\'"]', rawuserinput).group(1)
        	                	consistency = int(userinput[1])
					print value
					Client().sendputrequest(key,value,consistency,HOST,PORT)
					#print "Replica Response " + str(obj.sendputrequest(key,value,consistency))
				except:
					print "ERROR! Invalid PUT command."
        		elif userinput[0] == "EXIT":
				sys.exit(0)
		else:
                	print "ERROR! Invalid command entered."

	
		