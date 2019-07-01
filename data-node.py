###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# sock.connect((meta_ip, meta_port))

	print "meta_ip ", meta_ip
	print "data_ip ", data_ip

	try:
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			sp.BuildRegPacket(data_ip, data_port)
			sock.connect((meta_ip, meta_port))
			sock.sendall(sp.getEncodedPacket())
			response = sock.recv(1024)
			print "response ", response

			if response == "DUP":
				print "Duplicate Registration"

		 	if response == "NAK":
				print "Registratation ERROR"
			
	finally:
		sock.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):

		"""Receives a block of data from a copy client ;
		   Opens file to write/store the data blocks ; 
		   Save each block with a unique ID ;
		   The ID is sent back to the copy client ;
		"""

		fname, fsize = p.getFileInfo()
		print "fname ", fname
		print "fsize ", fsize
		self.request.send("OK")

		# Generates an unique block id.
		blockid = str(uuid.uuid1())

		# Open the dir/file for the new data block.
		file = open(DATA_PATH+ '/' + blockid, 'w')
		
		# Receive the data block len
		if fsize > 100000000:
			msp = 100000000
		else:
			msp = 1024
		dblock_size = self.request.recv(msp)
		print "recieved dblock size from copy ", dblock_size
		# print "size of dblock sent by copy ", len(dblock)
		self.request.send("OK") # send ok to copy after recieving dblock
		# print "MAX SIZE PACKET ", msp 

		dblock = ""  # string to store the info to be written to the new file
		while len(dblock) < int(dblock_size):
			print "LEN DBLOCK ", len(dblock)
			print "DBLOCK SIZE ", int(dblock_size)
			# if int(dblock_size) > 100000000:
				# dblock_chunk = self.request.recv(100000000)
			# else:
				# dblock_chunk = self.request.recv(1024)	
			dblock_chunk = self.request.recv(msp)	
	
			dblock = dblock + dblock_chunk
		

			self.request.send("OK")

		file.write(dblock)
		test = self.request.recv(1024)
		print "test ", test 
		# file.close()	

		# Send the block id back to copy client
		self.request.sendall(blockid) 
		self.request.close()
		

	def handle_get(self, p):
		print "inside handle get"
		# Get the block id from the packet
		blockid = p.getBlockID()


		# Read the file with the block id data
		# file = open(DATA_PATH + blockid, 'rb')
		with open(DATA_PATH + '/' + blockid, 'rb') as f:
			file = f.read()
		f.close()
		# print "file ", file
		# Send size first to copy client
		self.request.sendall(str(len(file)))
		ack = self.request.recv(1024)
		print "ack ", ack

		# Send it back to the copy client	.
		# self.request.sendall()

		sent = False
		while not sent: 
			print "not sent"

			file_chunk = file[0:1024]  # first chunk of data block to be sent
			# print "dblock chunk ", file_chunk
			file = file[1024:]   # datablock without the sent chunk
			self.request.sendall(file_chunk)
			ack = self.request.recv(1024)
			print "acknowledge ", ack
			
			if len(file) <= 0:
				sent = True

			# print "dblock after chunking ", file

		self.request.close()
		# f.close()




	def handle(self):
		print "do we enter handle? "
		msg = self.request.recv(1024)
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) < 4:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])
		print os.path.abspath(str(DATA_PATH))
		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()
	except:
		usage()

	print "DATAPATH ", DATA_PATH
	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
 	server.serve_forever()
