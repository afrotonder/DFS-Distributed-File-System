Omar Rosado Ramirez
801 12 8150

CCOM4017-0U1
Prof. J. Ortiz-Ubarri

%% OS PROJECT: DISTRIBUTED FILE SYSTEM %%

# INTRODUCTION

This project is a simple DISTRIBUTED FILE SYSTEM made in Python 2.7. It emulates a linux-based file system
and has the following capabilities:
    - copy files from the local file system into the distributed file system ;
    - list the files in the file system, displaying full file path and size ;
    - copy files from the distributed file system to the local file system ;


# FILES USED TO RUN THE DISTRIBUTED FILE SYSTEM
    - meta-data.py 
    - copy.py 
    - ls.py 
    - data-node.py
    
# DESCRIPTION OF FILES AND DISTRIBUTED FILE SYSTEM


This simple distributed file system is composed of a file server that communicates via sockets
with file clients and data clients. The metadata server is responsible for handling the communication
between all clients. The file clients or file system functions are the list client, which is responsible
for listing the files stored in the file system and the copy client, which copies files from the local 
filesystem into itself and copies those files back to the local filesystem. The files are stored in the 
data clients, servers, data servers, or data nodes(all are used during this project).


Files in order described above:


    meta-data.py: Handles communication between data-nodes and copy/list clients. As each data-node
                  registers, the meta-data will store its info and keep track of each file assosiated
                  with each data-node ;


    ls.py: Communicates with the metadata server and asks for a list of all files stored in the file system.
           Displays all files with its full path and the size of the file ;


    copy.py: This file has two functionalities:
                
                - copy to file system: 

                  This function opens the file chosen to copy(if exists), 
                  sends a packet with the file name and info to the metadata server which stores it for 
                  further use; the metadata server then sends the copy client a list of data-node servers to 
                  have the file stored distributedly over them. If the file isn’t already registered in our
                  file system, the file that will be copied to our file system is then separated into blocks 
                  and sent to the data-nodes. The data-nodes will then receive the file chunk by chunk, 
                  storing each chunk in its respective block and then communicates to the metadata server 
                  the name of the file where the blocks will be stored and the location of each block(data-nodes).  

                - copy from file system:

                This function communicates with the metadata server asking it for the list of data-nodes storing the 
                data blocks of the file being requested to copy. If the file is in the dfs, a destination file is
                opened to store the data blocks, the data-servers return their respective blocks and they are written
                in the destination file. 


    data-node.py: The data-node is in charge of storing the data blocks sent by the copy client. Each data-node
                  opens a file, writes the data blocks to the chosen destination file and sends back the block
                  id to the copy client.
                  The data-node is also responsible for handling data requests. When data is requested(asked to copy
                  from the dfs to the local dile system) the data-server opens the wanted file and sends its data blocks
                  to the copy client. 


# HOW TO RUN FILE SYSTEM

To run this project open a terminal and open about 5 tabs. For this example I will assume 5 are opened.

    - TERMINAL 1
        $ python meta-data.py localhost <metaport, leave black for default 8000>

    - TERMINALS 2, 3 & 4
        $ python data-node.py localhost 8080 ~/datadir0
        $ python data-node.py localhost 8081 ~/datadir1
        $ python data-node.py localhost 8082 ~/datadir2

    Up to now you have started up the metadata server and 3 data-node servers for file storage. 
    The file system is empty though, so lets copy some files into it:

    - TERMINAL 5
        $ python copy.py ~/<FILE OF YOUR CHOOSING> localhost:<metaport>:<NEW FILE NAME>

        NOTE: THIS MIGHT TAKE A WHILE IF ITS A LARGE ENOUGH FILE. 

    This process might take a while if the file is large enough. Once its done, try listing the file or files
    you copied to the file system.

    - TERMINAL 5
        $ python ls.py localhost

    If youve seen your files listed, try copying them back to your system now.

    - TERMINAL 5
        $ python copy.py localhost:<metaport>:<FILE NAME> ~/<DESTINATION OF NEW FILE>  


# ERRORS ENCOUNTERED DURRING PROCESS

The most scumtious error I encountered was pythons MemoryError. This occurred while trying to open
a file with the copy client. My computer has 4GB RAM and the DFS halted as soon as it read the line
opening a large file(a movie in this case). This took me a couple of hours to solve, but I finally got
it to run smoothly on a 8GB RAM Macbook and on my 4GB Asus Notebook(although not as smoothly as the Mac).

