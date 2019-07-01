###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server and ask to copy file fname(check if file is available/exists) ;
	    get a list of data nodes from meta data server if file doesn't already exist ; 
		Open the file in path to read ;
	    Divide file in blocks ;
		Send blocks distributedly to the data nodes ; 

		Recieves:
			address:
			fname:
			path:
	"""
	# Create a connection to the data server
	print "address ", address
	print "fname ", fname
	print "path full ", path	
	print "path cut", path[:len(path)-4]
	
	fsize = os.path.getsize(path)
	print "fsize", fsize
	p = Packet()

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((address[0], address[1]))

	# Read file
	# file = ""
	# f = open(path, 'rb')
	with open(path, 'rb') as f:
		# for line in f:
		file = f.read()
		# file = f.readlines()
	f.close()
	
	
	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 

	# print os.path.getsize(path)
	p.BuildPutPacket(fname, fsize)
	sock.sendall(p.getEncodedPacket()) # package must be encoded before sending throught the net
	data = sock.recv(1024)             # recieve data sent from meta-data server
	# print "data ", data                         # list files
	sock.close()
	
	# If no error or duplicate file exists 
	# Get the list of data nodes.
	if data == "DUP":
		print "File exists aready. Exiting copy client. " # Would you like to keep or replace?
		return 0 # exit
	else: # recieve data node server list
		# if dup, DecodePacket returns an error saying it cant decode the JSON object ;
		p.DecodePacket(data)
		servers = p.getDataNodes()
		print "servers ", servers
		print "server count ", len(servers)
	# Divide the file in blocks
		fblocks = []
		
		block_size = fsize/len(servers)
		print "block size ", block_size


		for i in range(0,fsize,block_size):
			print "i ", i
			# print "len of block of file to copy ", /len(file[i:i+block_size])
				# if i/block_size + 1 = 
				# print "TRY"
			print "JJJ ", len(file[i:i+block_size])
			fblocks.append(file[i:i+block_size])
			# if len(file[i:i+block_size]) < block_size:
			# 	print "se acabo el file se supone ", file[i:]
			# 	print "len del file q se acabo. ", len(file[i:])
			# 	fblocks.append(file[i:])
			# else:
			# 	print "parte del fokin file ", file[i:i+block_size]
			# 	print "len parte del fokin file ", len(file[i:i+block_size])

			# 	fblocks.append(file[i:i+block_size])
			# 	break

				
		
		

	# Send the blocks to the data servers.
	for dserver in servers:
		
		print "dserver ", dserver

		pack = Packet()
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((dserver[0], dserver[1]))
		pack.BuildPutPacket(fname, fsize)
		s.sendall(pack.getEncodedPacket())
		ack = s.recv(1024)
		# print ss
		if ack == "OK":
			# if response ok, send block size, wait aknowledge, send dblocks, wait acknowledge, 
			# # send block id's to meta

			print "RESPONSE OK WOOO"
			dblock = fblocks.pop(0) # get first dblock, remove from block list and send to data node servers
			s.sendall(str(len(dblock)))
			ack = s.recv(1024)
			print "Ack el data node recibio el size del bloque ", ack
		

			sent = False
			while not sent: #len(dblock) > 0: # 

				"""
								 # if block vigger than 300 megs
					  # first chunk of data block to be sent
					   # datablock without the sent chunk
				else:
					dblock_chunk = dblock[0:1024]  # first chunk of data block to be sent
					dblock = dblock[1024:]   # datablock without the sent chunk
				if len(dblock) <= 0:
					sent = True
				s.sendall(dblock_chunk)

				ack = s.recv(1024)
				print ("acknowledge ", ac


				"""
				if len(dblock) > 100000000:
					dblock_chunk = dblock[0:100000000]
					print "len big ", len(dblock_chunk)
					dblock = dblock[100000000:]
				else:
					dblock_chunk = dblock[0:1024]  # first chunk of data block to be sent
					print "len 1024 ", len(dblock_chunk)
					dblock = dblock[1024:]   # datablock without the sent chunk
				if len(dblock) <= 0:
					sent = True
				s.sendall(dblock_chunk)

				ack = s.recv(1024)
				print "acknowledge ", ack	
		

			s.sendall("OK")
			block_id = s.recv(1024)
			s.close() 

			dserver.append(block_id)  # ugh this took me a while to realize. 
			
			print "id of block sent to dnode ", block_id
	# Notify the metadata server where the blocks are saved.
		
	metasock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	metasock.connect((address[0], address[1])) 
	p.BuildDataBlockPacket(fname, servers)
	metasock.sendall(p.getEncodedPacket())

	metasock.close()

	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""
	print "copy from dfs"
   	# Contact the metadata server to ask for information of fname
	print "meta address ", address
	print "fname of file i want to copy ", fname 
	print "path of new file (block destiny) ", path 
	p = Packet()
	p.BuildGetPacket(fname)  # request metadata fname's inode


	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((address[0], address[1]))
	sock.sendall(p.getEncodedPacket())

	metadata = sock.recv(1024) # recieve 

	file = open(path, 'wb')

	# Ifs there is no error response Retreive the data blocks
	print "estamoredi ", metadata
	
	p.DecodePacket(metadata)
	servers = p.getDataNodes()

	print "servers ", servers

	for dserver in servers:
		print "dserver ", dserver

		pack = Packet()
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((dserver[0], dserver[1]))

		pack.BuildGetDataBlockPacket(dserver[2])
		s.sendall(pack.getEncodedPacket())

		dblock_size = s.recv(1024)
		print "dblock-size ", dblock_size

		s.sendall("OK") # send ok to copy after recieving dblock

		dblock = ""  # string to store the info to be written to the new file
		while len(dblock) < int(dblock_size):
			dblock_chunk = s.recv(1024)
			print "len of recieved dblock chunk ", len(dblock_chunk)
			print "dblock chunk recieved"
			dblock = dblock + dblock_chunk
			# print "dblock after chunk appended ", dblock
			print "dblock len after chunk appended ", len(dblock) 

			s.sendall("OK")
	   	# Save the file
		file.write(dblock)
		s.close()
	
	file.close()

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")
	# print "file_from ", file_from
	# print "file_to ", file_to 

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()
		# print "copy to dfs"
		print "fileto ", ip
		print "port fileto ", port
		print "to path ", to_path
		print from_path

		copyToDFS((ip, port), to_path, from_path)


