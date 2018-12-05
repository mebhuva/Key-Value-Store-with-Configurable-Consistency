import sys
import os
import socket
import pickle
import time
import logging
from threading import Lock
import re
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import configkeyvalue_pb2
replicaList = {}
value = 0
timestamp = 1
nodeName = []
firstreplica = 0
secondreplica = 0
thirdreplica  = 0 
fourthreplica = 0

class replica(object):
	global value
	global timestamp
	global replicaList
	global nodeName
	global firstreplica
	global secondreplica
	global thirdreplica
	global fourthreplica
    	KeyValueDict = {}
	logfilename = ""
	KeyValueDictLock = Lock()
	minkeyvalue = 0
   	maxkeyvalue = 255
 
	def setDictionary(self, key , newvalue , newtimestamp , WriteLog = True):
		if key < self.minkeyvalue or key > self.maxkeyvalue:
            		print "ERROR! Unsupported key " + str(key) + " given."
            		return False

		# check if key already in dict
		if self.get(key) != None and self.get(key) != False:
			if self.KeyValueDict[key][timestamp] > newtimestamp:
				return True


        	# Write to log file first
        	if WriteLog:
            		try:
                		logfilehandle = open(self.logfilename, "a")
                		logfilehandle.write(str(key) + "::" + newvalue+ "::" + str(newtimestamp) + "\n")
                		logfilehandle.close()
            		except IOError:
                		print "ERROR! The log file " + self.logfilename + " was not found!"
                		return False

		#with self.KeyValueDict:
	        # Then write to in-memory key-value store
		print "here"
        	self.KeyValueDict[key] = []
        	self.KeyValueDict[key].insert(value,newvalue)
		self.KeyValueDict[key].insert(timestamp, newtimestamp)
		print self.KeyValueDict
        	return True
	def sendMessage(self,data,replicaname,replicatimestamp):
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        		s.connect((replicaList[replicaname][1], int(replicaList[replicaname][2])))
			putrequestmessage = configkeyvalue_pb2.ReplicaPutRequest()
        		putrequestmessage.id = data.clientputrequest.id
        		putrequestmessage.key = data.clientputrequest.key
			putrequestmessage.value = data.clientputrequest.value
			putrequestmessage.timestamp = replicatimestamp
        		message = configkeyvalue_pb2.KeyValueMessage()
        		message.replicaputrequest.CopyFrom(putrequestmessage)
			s.sendall(pickle.dumps(message))
			s.close()
		except:
                	return None

	def sendupdaterequestreplica(self,replicaname,originalkey,originalvalue,sendupdatetimestamp):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	s.connect((replicaList[replicaname][1], int(replicaList[replicaname][2])))
		putrequestmessage = configkeyvalue_pb2.ReplicaPutRequest()
        	putrequestmessage.id = 1
        	putrequestmessage.key = originalkey
		putrequestmessage.value = originalvalue
		putrequestmessage.timestamp = sendupdatetimestamp
        	message = configkeyvalue_pb2.KeyValueMessage()
        	message.replicaputrequest.CopyFrom(putrequestmessage)
		s.sendall(pickle.dumps(message))
		s.close()


	def sendgetrequestreplica(self,replicaname,key):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        	s.connect((replicaList[replicaname][1], int(replicaList[replicaname][2])))
		getrequestmessage = configkeyvalue_pb2.ReplicaGetRequest()
        	getrequestmessage.id = 1
        	getrequestmessage.key = key
        	getrequestmessage.timestamp= 1
        	message = configkeyvalue_pb2.KeyValueMessage()
        	message.replicagetrequest.CopyFrom(getrequestmessage)
		s.sendall(pickle.dumps(message))
		data = pickle.loads(s.recv(10000))
		s.close()
		return data
		

	def checkts(self,data1,data2,data3):
		if(data1.replicaresponse.timestamp >= data2.replicaresponse.timestamp and data1.replicaresponse.timestamp >= data3.replicaresponse.timestamp):
			return 1
		if(data2.replicaresponse.timestamp >= data1.replicaresponse.timestamp and data2.replicaresponse.timestamp >= data3.replicaresponse.timestamp):
			return 2
		if(data3.replicaresponse.timestamp >= data2.replicaresponse.timestamp and data3.replicaresponse.timestamp >= data1.replicaresponse.timestamp):
			return 3


	def handleputreplica(self,replica1,replica2,replica3,clientkey,clientConsistency):
		if(replica1 == 0):
			responce1value = self.KeyValueDict[clientkey][value]
			responce1ts = self.KeyValueDict[clientkey][timestamp]
			readrepairlist = {}
			if clientConsistency == 1:
				return responce1value
			data2 = self.sendgetrequestreplica(replica2,clientkey)
			print data2.replicaresponse.value

			print data2.replicaresponse.timestamp
			
			data3 = self.sendgetrequestreplica(replica3,clientkey)
			print data3.replicaresponse.value

			print data3.replicaresponse.timestamp
			if(data2.replicaresponse.timestamp > self.KeyValueDict[clientkey][timestamp]):
				self.setDictionary(clientkey,data2.replicaresponse.value,data2.replicaresponse.timestamp)
			if(data3.replicaresponse.timestamp > self.KeyValueDict[clientkey][timestamp]):
				self.setDictionary(clientkey,data3.replicaresponse.value,data3.replicaresponse.timestamp)
			if(self.KeyValueDict[clientkey][timestamp] > data2.replicaresponse.timestamp):
				self.sendupdaterequestreplica(replica2,clientkey,self.KeyValueDict[clientkey][value] ,self.KeyValueDict[clientkey][timestamp])
			if(self.KeyValueDict[clientkey][timestamp] > data3.replicaresponse.timestamp):
				self.sendupdaterequestreplica(replica3,clientkey,self.KeyValueDict[clientkey][value] ,self.KeyValueDict[clientkey][timestamp])
			#print data
			return responce1value
		else :
			data1 = self.sendgetrequestreplica(replica1,clientkey)
			if clientConsistency == 1:
				return data1.replicaresponse.value
			data2 = self.sendgetrequestreplica(replica2,clientkey)
			data3 = self.sendgetrequestreplica(replica3,clientkey)
			checkvalue  = 0
			checkvalue = self.checkts(data1,data2,data3)
			if(checkvalue == 1):
				self.sendupdaterequestreplica(replica2,clientkey,data1.replicaresponse.value,data1.replicaresponse.timestamp)
				self.sendupdaterequestreplica(replica3,clientkey,data1.replicaresponse.value,data1.replicaresponse.timestamp)
			if(checkvalue == 2):
				self.sendupdaterequestreplica(replica1,clientkey,data2.replicaresponse.value,data2.replicaresponse.timestamp)
				self.sendupdaterequestreplica(replica3,clientkey,data2.replicaresponse.value,data2.replicaresponse.timestamp)
			if(checkvalue == 3):
				self.sendupdaterequestreplica(replica1,clientkey,data3.replicaresponse.value,data3.replicaresponse.timestamp)
				self.sendupdaterequestreplica(replica2,clientkey,data3.replicaresponse.value,data3.replicaresponse.timestamp)

			#print data
			return data1.replicaresponse.value


	def getvaluefromowner(self,clientkey,clientConsistency):
		#first replica functioning
		if(clientkey <= 63 and clientkey >= 0 and int(firstreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,secondreplica,thirdreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(firstreplica) == int(nodeName[0])) :
			return self.handleputreplica(secondreplica,thirdreplica,fourthreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(firstreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,thirdreplica,fourthreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(firstreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,secondreplica,fourthreplica,clientkey,clientConsistency)
		#second replica functioning
		if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(secondreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,thirdreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(secondreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,thirdreplica,fourthreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(secondreplica) == int(nodeName[0])) :
			return self.handleputreplica(firstreplica,thirdreplica,fourthreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(secondreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,fourthreplica,clientkey,clientConsistency)
		#third replica functioning
		if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(thirdreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,secondreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(thirdreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,thirdreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(thirdreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,fourthreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(thirdreplica) == int(nodeName[0])) :
			return self.handleputreplica(firstreplica,secondreplica,fourthreplica,clientkey,clientConsistency)

		#fourth replica functioning
		if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(fourthreplica) == int(nodeName[0])) :
			return self.handleputreplica(firstreplica,secondreplica,thirdreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(fourthreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,secondreplica,thirdreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(fourthreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,thirdreplica,clientkey,clientConsistency)
		elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(fourthreplica) == int(nodeName[0])) :
			return self.handleputreplica(0,firstreplica,secondreplica,clientkey,clientConsistency)

	def get(self, key):

        	if key not in self.KeyValueDict:
            		return False

        	return self.KeyValueDict[key]

	
					

		

	def replicahandle(self,data,clientsocket,consistencytype):
		self.logfilename = "writelog"+"-"+ str(nodeName[0])
		print self.logfilename
		if os.path.exists(self.logfilename):
			# Populate the in-memory key-value store from log file
                	logfilehandle = open(self.logfilename, "r")
            		for line in logfilehandle:
                		split = line.split('::')
                		self.setDictionary(int(split[0]), split[1], int(split[2]), False)
        	else:
            		# Just create the log file
            		logfilehandle = open(self.logfilename, "w+")
		
		if data.HasField("clientputrequest") :
			#first replica functioning
			print "inside client put"
			print self.KeyValueDict
			if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(firstreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,secondreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
				print "a"
			elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(firstreplica) == int(nodeName[0])) :
				self.sendMessage(data,secondreplica,int(time.time()))
				self.sendMessage(data,thirdreplica,int(time.time()))
				self.sendMessage(data,fourthreplica,int(time.time()))
			elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(firstreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,fourthreplica,self.KeyValueDict[data.clientputrequest.key][1])
			elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(firstreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,secondreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,fourthreplica,self.KeyValueDict[data.clientputrequest.key][1])

			#second replica functioning
			if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(secondreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
				print "a"
			elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(secondreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,fourthreplica,self.KeyValueDict[data.clientputrequest.key][1])
			elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(secondreplica) == int(nodeName[0])) :
				self.sendMessage(data,firstreplica,int(time.time()))
				self.sendMessage(data,thirdreplica,int(time.time()))
				self.sendMessage(data,fourthreplica,int(time.time()))
			elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(secondreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,fourthreplica,self.KeyValueDict[data.clientputrequest.key][1])

			#third replica functioning
			if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(thirdreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,secondreplica,self.KeyValueDict[data.clientputrequest.key][1])
				print "a"
			elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(thirdreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
			elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(thirdreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,fourthreplica,self.KeyValueDict[data.clientputrequest.key][1])
			elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(thirdreplica) == int(nodeName[0])) :
				self.sendMessage(data,firstreplica,int(time.time()))
				self.sendMessage(data,secondreplica,int(time.time()))
				self.sendMessage(data,fourthreplica,int(time.time()))

			#fourth replica functioning
			if(data.clientputrequest.key <= 63 and data.clientputrequest.key >= 0 and int(fourthreplica) == int(nodeName[0])) :
				self.sendMessage(data,firstreplica,int(time.time()))
				self.sendMessage(data,secondreplica,int(time.time()))
				self.sendMessage(data,thirdreplica,int(time.time()))
			elif(data.clientputrequest.key <= 127 and data.clientputrequest.key >= 64 and int(fourthreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,secondreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
			elif(data.clientputrequest.key <= 191 and data.clientputrequest.key >= 128 and int(fourthreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,thirdreplica,self.KeyValueDict[data.clientputrequest.key][1])
			elif(data.clientputrequest.key <= 255 and data.clientputrequest.key >= 192 and int(fourthreplica) == int(nodeName[0])) :
				self.setDictionary(data.clientputrequest.key,data.clientputrequest.value,int(time.time()))
				self.sendMessage(data,firstreplica,self.KeyValueDict[data.clientputrequest.key][1])
				self.sendMessage(data,secondreplica,self.KeyValueDict[data.clientputrequest.key][1])



			

		if data.HasField("replicaputrequest") :
			print data
			self.setDictionary(data.replicaputrequest.key,data.replicaputrequest.value,data.replicaputrequest.timestamp)
			print self.KeyValueDict

		

		if data.HasField("clientgetrequest") :

       			clientid = data.clientgetrequest.id
     			clientkey = data.clientgetrequest.key
     			clientconsistency = data.clientgetrequest.consistencylevel

       			#return value for particular key
    			returnval = self.getvaluefromowner(clientkey,clientconsistency)

      			# Start packing response to client
     			ownerResponcemsg = configkeyvalue_pb2.ClientResponse()
    			ownerResponcemsg.id = clientid
    			ownerResponcemsg.key = clientkey


     		        if returnval == None:
       	     			ownerResponcemsg.status = False
       	   			ownerResponcemsg.value = "None"
    			else:
        	     		ownerResponcemsg.status = True
        	   		ownerResponcemsg.value = returnval
	
	     		clientResponcemsg = configkeyvalue_pb2.KeyValueMessage()
	   		clientResponcemsg.clientresponse.CopyFrom(ownerResponcemsg)
    			try:
      				# send response to client
        	     		clientsocket.sendall(pickle.dumps(clientResponcemsg))
      			except:
        	    		print "ERROR ! socket exception while sending get val response to client"


		if data.HasField("replicagetrequest") :
			ownerreplicaResponcemsg = configkeyvalue_pb2.ReplicaResponse()
    			ownerreplicaResponcemsg.id = data.replicagetrequest.id
    			ownerreplicaResponcemsg.key = data.replicagetrequest.key
			ownerreplicaResponcemsg.status = True
			ownerreplicaResponcemsg.value = self.KeyValueDict[data.replicagetrequest.key][value]
			#ownerreplicaResponcemsg.value = "me ayaaa"
			ownerreplicaResponcemsg.timestamp= self.KeyValueDict[data.replicagetrequest.key][timestamp]
			ownerreplicaResponcemsg.nodeid = nodeName[0]
			replicaResponcemsg = configkeyvalue_pb2.KeyValueMessage()
	   		replicaResponcemsg.replicaresponse.CopyFrom(ownerreplicaResponcemsg)

    			try:
        	     		clientsocket.sendall(pickle.dumps(replicaResponcemsg))
      			except:
        	    		print "ERROR ! socket exception while sending get val response to client"

		#print self.KeyValueDict
			

		
if __name__ == '__main__':
	global replicaList
	global firstreplica
	global secondreplica
	global thirdreplica
	global fourthreplica
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
	firstreplica = list(replicaList.keys())[0]
	secondreplica = list(replicaList.keys())[1]
	thirdreplica= list(replicaList.keys())[2] 
	fourthreplica = list(replicaList.keys())[3]
	#print firstreplica
	#print secondreplica
	print replicaList
	nodeName = re.findall('\d+',sys.argv[1].strip())
	while 1:
        	(clientsocket, address) = serversocket.accept()
		data = pickle.loads(clientsocket.recv(1024))
		replica().replicahandle(data,clientsocket,sys.argv[4])





