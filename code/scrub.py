# scrub parallel - Anna Tse #
from __future__ import print_function
noise = open('noise.txt', 'w')
signal = open('signal.txt', 'w')
logFile = open('scrub-session-log.txt', 'w')
import logging
logging.basicConfig(level=logging.INFO)
import sys
import datetime
import time
from mpi4py import MPI
import numpy as np
import itertools
import os
import psutil
import re

def rexClean(data):
    timePattern = re.compile(r'^\d\d\d\d\d\d\d\d:\d\d:\d\d:\d\d.\d\d\d\d\d\d$') # Time format: must be digits others are malformed #        
    pricePattern = re.compile(r'^[0-9]{3,4}[\.0-9]{0,3}$') # Price format +ve, 3-4 digits (ignore price > 4 digits) #                          
    unitPattern = re.compile(r'^[0-9\r]{3,7}$') # Unit format +ve, 3-6 digits (ignore units > 6 digits) #          
    t = re.match(timePattern, data[0]) 
    p = re.match(pricePattern, data[1])
    u = re.match(unitPattern, data[2])
    if t == None or p == None or u == None:
        return (None)
    else:
        return ('match') # Matches all format requirements #       

def chunks(dataList, size=100): # Read data array into chunks - 100 rows per chunk #
    for i in xrange(0, len(dataList), size):
        yield dataList[i:i + size]

def close(fh): # Define close function to close and finalize file #
    MPI.File.Close(fh)
    MPI.Finalize()

def read(buffer): # Define read function to block of characters #
    row, block, dataList = ([] for i in xrange(3))
    for m in xrange(len(buffer)): # Convert Char into rows of data #      
        if not row: # If row is empty, append first char from buffer to row array #
            row.append(buffer[m])        
        elif buffer[m] != '\n': # If row is not empty and char is not new line (\n), add char to row array #
            row.append(buffer[m])        
        elif buffer[m] == '\n' and len(row) > 0: # If row is not empty but char is new line (\n), # 
            block.append(''.join(row))#(.strip('\n')) # Join all char from row, add row to block, and reset row to empty #
            row = []
    for n in range(len(block)): # split row of string by comma and store in data list #
        dataList.append(block[n].split(','))
    return (dataList)
    
BUFFER_SIZE = 100 # Define buffer size for \n at file block boundaries
buff = np.empty(BUFFER_SIZE, dtype = str) # Define empty buffer array
block, row, data, noiseFile, signalFile, priceList, sesLog = ([] for i in xrange(7)) # Define lists
comm = MPI.COMM_WORLD
nprocs = comm.Get_size()
myid = comm.Get_rank()
fh = MPI.File
filesize = blocksize = block_start = block_end = MPI.OFFSET
process = psutil.Process(os.getpid())

if __name__ == '__main__':
    if (len(sys.argv) != 2):
        logging.info('Program requires path to file for reading!')
        sesLog.append('Output logfile: Program did not receive path to file, system exit 1')        
        sys.exit(1)

    try: # Open file to read #
        fh = fh.Open(comm, sys.argv[1]) # OR # fh = MPI.File.Open(comm,sys.argv[1],mode,info) #info = MPI.INFO_NULL #mode = MPI.MODE_RDONLY
    except IOError:
        logging.info('Process %d: Open File Error: Message ' % (myid) + 'Call to MPI.File.Open() failed')
        sesLog.append('Process %d: Open File Error: Message ' % (myid) + 'Call to MPI.File.Open() failed')
    
    blockAdjustStart = time.time()
    
    filesize = fh.Get_size() # Get the size of the file #
    if filesize <= 0:
        logging.info('Process %d: Get File Size Error %d: Message ' % (myid, filesize) + 'Call to MPI.File.Get_size() failed')
        sesLog.append('Process %d: Get File Size Error %d: Message ' % (myid, filesize) + 'Call to MPI.File.Get_size() failed')
        close(fh)
        MPI.Free_mem(filesize)
    
    blocksize = filesize/nprocs # Calculate the nominal block size and block offset for each process #
    block_start = blocksize * myid
    if myid == nprocs-1:
         block_end = filesize
    else:
         block_end = block_start + blocksize - 1
    logging.info('Process %d: block = [%d , %d]' % (myid, block_start, block_end))
    sesLog.append('Process %d: Block = [%d , %d]' % (myid, block_start, block_end))
    
    if myid != 0: # Read buffer of chars at beginning of block, only need to adjust block_start for processes beyond 1 #
        fh.Seek(block_start) # OR #MPI.File.Seek(fh, block_start, MPI.SEEK_SET)
        try: 
            fh.Read([buff, MPI.CHAR])  # OR #MPI.File.Read(fh,buff)                     
        except IOError: 
            logging.info('Process %d: Read File Error : Message ' % (myid) + 'Call to MPI.File.Read() failed')
            sesLog.append('Process %d: Read File Error : Message ' % (myid) + 'Call to MPI.File.Read() failed')
            close(fh)
            MPI.Free_mem(buff)
        
    for i in xrange(BUFFER_SIZE): # Adjust block start #
        if i == BUFFER_SIZE:
            logging.info('Process %d: Adjust Block Start Error %d: Message ' % (myid, -999) + 'Unable to find newline character within buffer at start of block')
            sesLog.append('Process %d: Adjust Block Start Error %d: Message ' % (myid, -999) + 'Unable to find newline character within buffer at start of block')
            close(fh)
            MPI.Free_mem(buff)
        if buff[i] == '\n':
            block_start += i + 1
            break     

    if myid != nprocs-1: # Read buffer of chars at end of block #
        fh.Seek(block_end)
        try:
            fh.Read([buff, MPI.CHAR])
        except IOError:
            logging.info('Process %d: Read File Error : Message ' % (myid) + 'Call to MPI.File.Read() failed')
            sesLog.append('Process %d: Read File Error : Message ' % (myid) + 'Call to MPI.File.Read() failed')
            close(fh)
            MPI.Free_mem(buff)            

    for j in xrange(BUFFER_SIZE): # Adjust block end #
        if j == BUFFER_SIZE:
             logging.info('Process %d: Adjust Block End Error %d: Message ' % (myid, -999) + 'Unable to find newline character within buffer at end of block')
             sesLog.append('Process %d: Adjust Block End Error %d: Message ' % (myid, -999) + 'Unable to find newline character within buffer at end of block')
             close(fh)
             MPI.Free_mem(buff)
        if buff[j] == '\n':
            block_end += j
            break
    logging.info('Process %d: Adjusted block = [%d , %d]' % (myid, block_start, block_end)) 
    sesLog.append('Process %d: Adjusted block = [%d , %d]' % (myid, block_start, block_end))    
    
    blockAdjustEnd = time.time()
    blockAdjustTime = blockAdjustEnd - blockAdjustStart
    logging.info('Process %d: Time used to adjust block start & end = %d seconds' % (myid, blockAdjustTime))
    sesLog.append('Process %d: Time used to adjust block start & end = %d seconds' % (myid, blockAdjustTime))
    
    totalMem1 = process.memory_info().rss
    adjustBlockMem = totalMem1
    logging.info('Process %d: Memory usage for adjusting block start & end = %d bytes' % (myid, adjustBlockMem)) 
    sesLog.append('Process %d: Memory usage for adjusting block start & end = %d bytes' % (myid, adjustBlockMem))
         
    MPI.File.Seek(fh, block_start, MPI.SEEK_SET) # Read block into memory, first local block start # 
    buffer = np.empty((block_end - block_start +1), dtype =str) # Create a new buffer with size of this current block #
    fh.Read([buffer, MPI.CHAR])
 
    # Log start reading data and time #
    logging.info('Process %d: Block data being read in.' % myid)
    sesLog.append('Process %d: Block data being read in.' % myid)      
    readStart = time.time() # Read start time #
    
    data = read(buffer)  
    chunks = list(chunks(data))
#    print ('length data:', len(data))
#    print ('Num of chunks: ',len(chunks))
    
    # Log finished reading data and time #   
    readDone = time.time() # Read end time #   
    logging.info('Process %d: Finished reading block of data.' % myid)
    sesLog.append('Process %d: Finished reading block of data.' % myid)
    ReadTime = readDone - readStart # Read time #     
    logging.info('Process %d: Time used to read block of data = %d seconds' % (myid, ReadTime))
    sesLog.append('Process %d: Time used to read block of data = %d seconds' % (myid, ReadTime))
    
    #process = psutil.Process(os.getpid())
    totalMem2 = process.memory_info().rss
    readMem = totalMem2 - totalMem1
    logging.info('Process %d: Memory usage for reading block of data = %d bytes' % (myid, readMem))
    sesLog.append('Process %d: Memory usage for reading block of data = %d bytes' % (myid, readMem))
    
    # Report data size #  
    logging.info('Process %d: Total number of data = %d' % (myid, len(data))) 
    sesLog.append('Process %d: Total number of data = %d' % (myid, len(data)))
    
    # Log start srubbing data and time #
    logging.info('Process %d: Data being srubbed.' % myid)
    sesLog.append('Process %d: Data being srubbed.' % myid)
    scrubStart = time.time() # Scrub start time #
    
    for i in xrange(len(chunks)):
        #chunk_num = i 
        chunkList = chunks[i]
        for j in xrange(len(chunkList)):
            if rexClean(chunkList[j]) == None:
                noiseFile.append(chunkList[j][0])      
            try: # Convert string to timestamp #
                dt = datetime.datetime.strptime(chunkList[j][0], '%Y%m%d:%H:%M:%S.%f')
                epoch = datetime.datetime.fromtimestamp(0)
                (dt - epoch).total_seconds()
                chunkList[j][0] = dt
            except ValueError: 
                continue
        
        chunkList.sort() # Put data into time order to check repeating time#
        for m in xrange(len(chunkList)-1):
            if chunkList[m+1][0] == chunkList[m][0]: # Find repeating timestamp #
                noiseFile.append(chunkList[m][0])
                noiseFile.append(chunkList[m+1][0])
            elif (chunkList[m+1][0]-chunkList[m][0]).total_seconds() > 1: # Timestamps difference greater 1s # - Assume first timestamp is correct tho?!
                noiseFile.append(chunkList[m+1][0])
            
    # Log finished scrubbing data and time #
    scrubDone = time.time() # Scrub end time #    
    logging.info('Process %d: Finished scrubbing data.' % myid)
    sesLog.append('Process %d: Finished scrubbing data.' % myid)   
    ScrubTime = scrubDone - scrubStart   
    logging.info('Process %d: Time used to scrub data = %d seconds' % (myid, ScrubTime))
    sesLog.append('Process %d: Time used to scrub data = %d seconds' % (myid, ScrubTime))   
    
    totalMem3 = process.memory_info().rss
    scrubMem = totalMem3 - totalMem2
    logging.info('Process %d: Memory usage for scrubbing data = %d bytes' % (myid,scrubMem))
    sesLog.append('Process %d: Memory usage for scrubbing data = %d bytes' % (myid,scrubMem))

    for n in xrange(len(data)-1): # Add Timestamp which is not in noiseFile to signalFile array #
        if data[n][0] not in noiseFile:
            signalFile.append(data[n][0])

    # Process 0 gather broadcasted noiseFile from all processes and print to text file # 
    noiseGather = comm.gather(noiseFile)
    if myid == 0:
        # Log start writing noise file and time #
        logging.info('Start writing noise file.')
        sesLog.append('Start writing noise file.')
        writeNoiseStart = time.time() # Write noise start time #
        
        noiseGather = list(itertools.chain.from_iterable(noiseGather))
        # note: there're duplicated noise key values since using timestamp
        logging.info('Report Total Number of Noise: ' + str(len(noiseGather)))
        sesLog.append('Report Total Number of Noise: ' + str(len(noiseGather)))
        for y in xrange(len(noiseGather)): # Write noise.txt #
            print(noiseGather[y], file = noise)
        noise.close()
       
        # Log finished writing noise.txt file and time #
        writeNoiseDone = time.time()   
        logging.info('Finished writing noise.txt file.')
        sesLog.append('Finished writing noise.txt file.')  
        NoiseOutputTime = writeNoiseDone - writeNoiseStart 
        logging.info('Time used to write noise.txt file = %d seconds' % NoiseOutputTime)		
        sesLog.append('Time used to write noise.txt file = %d seconds' % NoiseOutputTime)
        
        totalMem4 = process.memory_info().rss
        writeNoiseMem = totalMem4 - totalMem3
        logging.info('Memory usage for writing noise.txt = %d bytes' % writeNoiseMem)
        sesLog.append('Memory usage for writing noise.txt = %d bytes' % writeNoiseMem)
    
    # Process 0 gathers broadcasted signalFile from all processes and print to text file # 
    signalGather = comm.gather(signalFile)
    if myid == 0:
        # Log start writing signal file and time #
        logging.info('Start writing signal file.')
        sesLog.append('Start writing signal file.')
        writeSignalstart = time.time() # Write signal start time #
        
        signalGather = list(itertools.chain.from_iterable(signalGather))
        # note: noise + signal > total data since duplicated timestamps
        logging.info('Report Total Number of Signal: ' + str(len(signalGather))) 
        sesLog.append('Report Total Number of Signal: ' + str(len(signalGather)))
        for x in xrange(len(signalGather)): # Write signal.txt #
            print(signalGather[x], file = signal)
        signal.close()
            
        # Log finished writing signal.txt file and time #
        writeSignaldone = time.time()   
        logging.info('Finished writing signal.txt file.')
        sesLog.append('Finished writing signal.txt file.')
        SignalOutputTime = writeSignaldone - writeSignalstart
        logging.info('Time used to write signal.txt file = %d seconds' % SignalOutputTime)
        sesLog.append('Time used to write signal.txt file = %d seconds' % SignalOutputTime)
        
        totalMem5 = process.memory_info().rss
        writeSignalMem = totalMem5 - totalMem4
        logging.info('Memory usage for writing signal.txt = %d bytes' % writeSignalMem)
        sesLog.append('Memory usage for writing signal.txt = %d bytes' % writeSignalMem)
    
    # Process 0 gathers broadcasted sesLog lists from all processes and print to text file #
    logGather = comm.gather(sesLog)
    if myid == 0:
        logGather = list(itertools.chain.from_iterable(logGather))
        for x in xrange(len(logGather)): # Write signal.txt #
            print(logGather[x], file = logFile)
        logFile.close()
        
    close(fh)
    MPI.Free_mem(buffer)