# scrub parallel - Anna Tse #
from __future__ import print_function
logFile = open('normal-session-log.txt', 'w')
import logging
logging.basicConfig(level=logging.INFO)
import sys
import time
import itertools
from mpi4py import MPI
import numpy as np
from scipy.stats import mstats
import os
import psutil
#import matplotlib.pyplot as plt
def chunks(dataList, size=100): # Read data array into chunks - 100 rows per chunk #
    for i in xrange(0, len(dataList), size):
        yield dataList[i:i + size]

def close(fh): # Define close fucntion to close and finalize file #
    MPI.File.Close(fh)
    MPI.Finalize()

def read(buffer): # Define read funciton to read block of characters #
    row, block, dataList = ([] for i in range(3))
    for m in xrange(len(buffer)): # Convert Char into rows of data #      
        if not row: # If row is empty, append first char from buffer to row array #
            row.append(buffer[m])        
        elif buffer[m] != '\n': # If row is not empty and char is not new line (\n), add char to row array #
            row.append(buffer[m])        
        elif buffer[m] == '\n' and len(row) > 0: # If row is not empty but char is new line (\n), # 
            block.append(''.join(row))#(.strip('\n')) # Join all char from row, add row to block, and reset row to empty #
            row = []
    for n in xrange(len(block)): # split row of string by comma and store in data list #
        dataList.append(block[n].split(','))
    return (dataList)
    
BUFFER_SIZE = 100 # Define buffer size for \n at file block boundaries
buff = np.empty(BUFFER_SIZE, dtype = str) # Define empty buffer array
block, row, data, noiseFile, signalFile, returns, sesLog = ([] for i in range(7)) # Define lists
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
        
    for i in range(BUFFER_SIZE): # Adjust block start #
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

    for j in range(BUFFER_SIZE): # Adjust block end #
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
    
    readEnd = time.time()
    logging.info('Process %d: Finished reading block of data.' % myid)
    sesLog.append('Process %d: Finished reading block of data.' % myid)
    readBlock = readEnd - readStart
    logging.info('Process %d: Time used to read block of data = %d seconds' % (myid, readBlock))
    sesLog.append('Process %d: Time used to read block of data = %d seconds' % (myid, readBlock))
    
    totalMem2 = process.memory_info().rss
    readMem = totalMem2 - totalMem1
    logging.info('Process %d: Memory usage for reading block of data = %d bytes' % (myid, readMem)) 
    sesLog.append('Process %d: Memory usage for reading block of data = %d bytes' % (myid, readMem))
    
    # Read noise.txt file as list #
    logging.info('Process %d: Noise file being read in.' % myid)
    sesLog.append('Process %d: Noise file being read in.' % myid)
    readStartN = time.time()
    
    f = open('noise.txt', 'r')
    for line in f.read().split('\n'):
            noiseFile.append(line)
    
    readEndN = time.time()
    logging.info('Process %d: Finished reading noise.txt.' % myid)
    sesLog.append('Process %d: Finished reading noise.txt.' % myid)
    readNoise = readEndN - readStartN
    logging.info('Process %d: Time used to read noise.txt = %d seconds' % (myid, readNoise))
    sesLog.append('Process %d: Time used to read noise.txt = %d seconds' % (myid, readNoise))
    
    totalMem3 = process.memory_info().rss
    readNoiseMem = totalMem3 - totalMem2
    logging.info('Process %d: Memory usage for reading noise.txt = %d bytes' % (myid, readNoiseMem)) 
    sesLog.append('Process %d: Memory usage for reading noise.txt = %d bytes' % (myid, readNoiseMem))
    
    
    # Create a list of signal data #
    logging.info('Process %d: Creating Signal list.' % myid)
    sesLog.append('Process %d: Creating Signal list.' % myid)
    createStart = time.time()
    
    for i in xrange(len(chunks)):
        #chunk_num = i 
        chunkList = chunks[i]
        for j in xrange(len(chunkList)):
            if chunkList[j][0] not in noiseFile:
                signalFile.append(chunkList[j])      
    
#    for i in xrange(len(data)):
#        if i == len(data)-1:
#            break
#        # Timestamps values not in noise, should be in signal file, note: noise has repeated timestamps #
#        if data[i][0] not in noiseFile: 
#            signalFile.append(data[i])

    createEnd = time.time()
    createTime = createEnd - createStart
    logging.info('Process %d: Time used to create signal list = %d seconds' % (myid, createTime))
    sesLog.append('Process %d: Time used to create signal list = %d seconds' % (myid, createTime))
    
    totalMem4 = process.memory_info().rss
    createSignalMem = totalMem4 - totalMem3
    logging.info('Process %d: Memory usage for creating signal list = %d bytes' % (myid, createSignalMem)) 
    sesLog.append('Process %d: Memory usage for creating signal list = %d bytes' % (myid, createSignalMem))

    # Process 0 gathers signalFile from all other processes #
    signalGather = comm.gather(signalFile)
    if myid == 0:
        signalGather = list(itertools.chain.from_iterable(signalGather))        
        signalGather.sort()
        
        calStart = time.time()        
        # Calculate returns of price #
        for j in xrange(len(signalGather)-1):
            p1 = float(signalGather[j][1])
            p2 = float(signalGather[j+1][1])
            result = ((p2-p1)/p1)           
            returns.append(result)
        
        # Doing normal test on returns values #
        z, pval = mstats.normaltest(returns)
        if (pval < 0.05):
            logging.info('P value < 0.05. Hence, not normally distributed')
            sesLog.append('P value < 0.05. Hence, not normally distributed')
        
        # Plot graph to confirm normality test answer "
        #plt.hist(returns, bins=np.arange(min(returns),max(returns)))
        #plt.show()
        
        calEnd = time.time()
        calTime = calEnd - calStart
        logging.info('Time used to execute normality test = %d seconds' % calTime)
        sesLog.append('Time used to execute normality test = %d seconds' % calTime)
        
        totalMem5 = process.memory_info().rss
        calMem = totalMem5 - totalMem4
        logging.info('Memory usage for executing normality test = %d bytes' % createSignalMem) 
        sesLog.append('Memory usage for executing normality test = %d bytes' % createSignalMem)
    
    # Gathering sesLog lists from all processes and print to text file #
    logGather = comm.gather(sesLog)
    if myid == 0:
        logGather = list(itertools.chain.from_iterable(logGather))
        for x in xrange(len(logGather)): # Write signal.txt #
            print(logGather[x], file = logFile)
        logFile.close()

    close(fh)
    MPI.Free_mem(buffer)
