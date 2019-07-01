###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

import socket
from mds_db import *
from Packet import *
import sys
import SocketServer
		
	# return c

def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)


class MetadataTCPHandler(SocketServer.BaseRequestHandler):


	def handle_reg(self, db, p):
		"""Register a new client to the DFS  ACK if successfully REGISTERED
			NAK if problem, DUP if the IP and port already registered
		"""

		try:
			if db.AddDataNode(p.getAddr(), p.getPort()):
				self.request.sendall("ACK") 
			else:
				self.request.sendall("DUP")
		except:
			self.request.sendall("NAK")

	def handle_list(self, db):
		"""Get the file list from the database and send list to client"""
		try:
			print "inside handle list func"
			p = Packet()
			# print db.GetFiles()
			p.BuildListResponse(db.GetFiles())
			self.request.sendall(p.getEncodedPacket())

		except:
			self.request.sendall("NAK NAK")	

	def handle_put(self, db, p):
		"""Insert new file into the database if it doesn't exist ;
		   Send data node list to copy client to save the file ;
		"""
		print "indside put function"
	       
		data = p.getFileInfo()
		print "esto --> ", data
	
		if db.InsertFile(data[0], data[1]): # returns 0 if file already exists
			print "handle put"
			
			# servers = p.getDataNodes()  # not to be confused with db.GetDataNodes() 
			servers = db.GetDataNodes()   # creates a list of available datanode servers 
			print "servers ", servers
			p.BuildPutResponse(servers)   # get the list ready to send through the net
			
			self.request.sendall(p.getEncodedPacket())
		else:
			self.request.sendall("DUP")
	
	def handle_get(self, db, p):
		"""Check if file is in database ;
		    return list of server nodes that contain the file ;1
		"""
		print "inside handle get function"
		# Fill code to get the file name from packet
		fname = p.getFileName()
		print "fname ", fname 
		# get the fsize and array of metadata server
		fsize, metalist = db.GetFileInode(fname)
		# print "fname ", fname 
		print "inodes ", metalist

		if fsize:
			p.BuildGetResponse(metalist, fsize)
			self.request.sendall(p.getEncodedPacket())
		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Fill code to get file name and blocks from packet
		print "inside handle blocks function"
		fname = p.getFileName()
		blocks = p.getDataBlocks()

		# Fill code to add blocks to file inode
		db.AddBlockToInode(fname, blocks)
		
	def handle(self):
		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)

		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files
			print "call hanlde list"
			self.handle_list(db)
		
		elif cmd == "put":
			# Client asking for servers to put data
			print "call handle put"
			self.handle_put(db, p)
		
		elif cmd == "get":
			# Client asking for servers to get data
			print "call handle get"
			self.handle_get(db, p)

		elif cmd == "dblks":
			# Client sending data blocks for file
			print "call send data block handler"
			self.handle_blocks(db, p)

		db.Close()

if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(sys.argv) > 1:
    	try:
    		PORT = int(sys.argv[1])
    	except:
    		usage()

    server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()