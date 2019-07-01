###############################################################################
#
# Filename: ls.py
# Author: Jose R. Ortiz and Omar Rosado Ramirez / 801128150
#
# Description:
# 	List client for the DFS
#   This program contacts the meta-data server and asks it to access the
#   database and return all file names and their respective sizes
#



import socket
import os
import sys
from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.
	p = Packet()

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))

	p.BuildListPacket()                # build list request package
	sock.sendall(p.getEncodedPacket()) # package must be encoded before sending throught the net
	data = sock.recv(1024)             # recieve data sent from meta-data server
	p.DecodePacket(data)			   # recieved data must be decoded before reading
	files = p.getFileArray()           # get array of files after packet decoded

	for file in files:
		print os.path.abspath(file[0]), file[1], "bytes"
		
if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)
