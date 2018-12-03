import sys
import os
import socket
import pickle
import time
import logging
import re
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import configkeyvalue_pb2
replicaList = {}
value = 0
timestamp = 1

class replica(object):
	global value
	global timestamp
	global replicaList
    	KeyValueDict = {}

	def sendMessage(self,replicaname):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	s.connect((replicaList[replicaname][1], int(replicaList[replicaname][2])))
		putrequestmessage = configkeyvalue_pb2.ReplicaPutRequest()
        	putrequestmessage.id = data.clientputrequest.id
        	putrequestmessage.key = data.clientputrequest.key
		putrequestmessage.value = data.clientputrequest.value
        	message = configkeyvalue_pb2.KeyValueMessage()
        	message.replicaputrequest.CopyFrom(putrequestmessage)
		s.sendall(pickle.dumps(message))
		s.close()
		

	def replicahandle(self,data,nodeName,clientsocket,consistencytype):
		firstreplica = list(replicaList.keys())[0]
		secondreplica = list(replicaList.keys())[1]
		thirdreplica = list(replicaList.keys())[2] 
		fourthreplica = list(replicaList.keys())[3]
		print firstreplica 
		print nodeName[0]
		print "hi"
		if data.HasField("clientputrequest") :
			if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(firstreplica) == int(nodeName[0])) :
				self.KeyValueDict[data.clientputrequest.key] = []
        			self.KeyValueDict[data.clientputrequest.key].insert(value, data.clientputrequest.value)
        			self.KeyValueDict[data.clientputrequest.key].insert(timestamp, int(time.time()))
				self.sendMessage(secondreplica)
				self.sendMessage(thirdreplica)
				print "a"
			elif(int(firstreplica) == int(nodeName[0])) :
				self.sendMessage(secondreplica)
				self.sendMessage(thirdreplica)
				self.sendMessage(fourthreplica)
				print "b"


			elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(secondreplica) == int(nodeName[0])) :
				self.KeyValueDict[data.clientputrequest.key] = []
        			self.KeyValueDict[data.clientputrequest.key].insert(value, data.clientputrequest.value)
        			self.KeyValueDict[data.clientputrequest.key].insert(timestamp, int(time.time()))
				self.sendMessage(thirdreplica)
				self.sendMessage(fourthreplica)
				print "c"
			elif(int(secondreplica) == int(nodeName[0])) :
				self.sendMessage(firstreplica)
				self.sendMessage(thirdreplica)
				self.sendMessage(fourthreplica)
				print "d"


			elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(thirdreplica) == int(nodeName[0])) :
				self.KeyValueDict[data.clientputrequest.key] = []
        			self.KeyValueDict[data.clientputrequest.key].insert(value, data.clientputrequest.value)
        			self.KeyValueDict[data.clientputrequest.key].insert(timestamp, int(time.time()))
				self.sendMessage(fourthreplica)
				self.sendMessage(firstreplica)
				print "e"
			elif(int(thirdreplica) == int(nodeName[0])) :
				self.sendMessage(fourthreplica)
				self.sendMessage(firstreplica)
				self.sendMessage(secondreplica)
				print "f"

			elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(fourthreplica) == int(nodeName[0])) :
				self.KeyValueDict[data.clientputrequest.key] = []
        			self.KeyValueDict[data.clientputrequest.key].insert(value, data.clientputrequest.value)
        			self.KeyValueDict[data.clientputrequest.key].insert(timestamp, int(time.time()))
				self.sendMessage(firstreplica)
				self.sendMessage(secondreplica)
				print "g"
			elif(int(fourthreplica) == int(nodeName[0])) :
				self.sendMessage(firstreplica)
				self.sendMessage(secondreplica)
				self.sendMessage(thirdreplica)
				print "h"

		if data.HasField("replicaputrequest") :
			print data

		print self.KeyValueDict
			

		
if __name__ == '__main__':
	#global replicaList
	if len(sys.argv) != 5:
                print "Invalid Parameters: <NodeName> <Port> <replicas.txt> <CONSISTENCYTYPE : READ-REPAIR / HINTED-HANDOFF>"
                sys.exit(0)
	serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serversocket.bind((socket.gethostbyname(socket.gethostname()), int(sys.argv[2])))
	serversocket.listen(5)
	logger.debug("\nWaiting for connection... Listening on "+str(socket.gethostbyname(socket.gethostname())+":"+ sys.argv[2]))
	replicalisttemp = {}
	if not os.path.exists(sys.argv[3]):
               	print "ERROR ! Input file not found"
               	sys.exit(0)
        else:
               	try:
			print sys.argv[3]
                       	with open(str(sys.argv[3])) as file:
                        	for line in file:
                                	replicadata = line.strip().split(" ")
                                       	#if not replicadata[0].strip() == sys.argv[1] :
					nodeNumber = re.findall('\d+',replicadata[0].strip())
					#print nodeNumber[0]
					replicalisttemp[int(nodeNumber[0])] = [replicadata[0].strip(), replicadata[1].strip() , replicadata[2].strip() ]
		except:
                       	print "ERROR ! Not able to read input file, please check the format"
			sys.exit(0)
	for key in sorted(replicalisttemp.iterkeys()):
		replicaList[key] = replicalisttemp[key]
	print replicaList

	while 1:
        	(clientsocket, address) = serversocket.accept()
		data = pickle.loads(clientsocket.recv(1024))
		replica().replicahandle(data,re.findall('\d+',sys.argv[1].strip()),clientsocket,sys.argv[4])
		
		
			





