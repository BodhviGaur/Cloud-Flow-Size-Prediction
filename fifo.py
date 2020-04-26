from queue import Queue
import logging
import threading
import time
import pandas

fifo_queue = Queue(maxsize = 0)

KMeans_iterator = 0
SGD_iterator = 0
Pagerank_iterator = 0
KMeans_num = 1
SGD_num = 1
Pagerank_num = 1

count_KMeans = 0
count_Pagerank = 0
count_SGD = 0

def worker(lock):
    #This is a worker thread. It fetches the topmost task and sleeps for the shortest job. 
    #print("Worker started")
    while not (KMeans_num > count_KMeans and SGD_num > count_SGD and Pagerank_num > count_Pagerank and fifo_queue.empty()):
        #print("fetching task")
        lock.acquire()
        t = sjf()
        lock.release()
        #print(t)
        if(t == -1):
            #print(KMeans_num, SGD_num, Pagerank_num)
            if(KMeans_num > count_KMeans and SGD_num > count_SGD and Pagerank_num > count_Pagerank and fifo_queue.empty()):
                #This is to avoid deadlock. So, if a number of threads entered 
                return
            else:
                time.sleep(5)
        else:
            #print(t)
            t = t/1000
            time.sleep(t)
    return
        
    
    
def job_incoming():
    global KMeans_iterator
    global KMeans_num
    global SGD_iterator
    global SGD_num
    global Pagerank_iterator
    global Pagerank_num
    
    for row in file1.index:
        #print(file1.head(2))
        task_type = file1['job_type'][row]
        sleep_time = file1['incoming_time'][row] / 1000
        time.sleep(sleep_time)
        
        # task = KMeans
        list_to_send = []
        if(task_type == 1):
            while(KMeans_iterator < len(fileKMeans.index) and fileKMeans['job'][KMeans_iterator] == KMeans_num):
                list_to_send.append(fileKMeans['flow_size'][KMeans_iterator])
                #Send all flows mapped to the KMeans_iterator job.Increment the job number.
                KMeans_iterator+=1
            KMeans_num+=1
               
        # task = SGD
        if(task_type == 2):
            while(SGD_iterator < len(fileSGD.index) and fileSGD['job'][SGD_iterator] == SGD_num):
                list_to_send.append(fileSGD['flow_size'][SGD_iterator])
                #Send all flows mapped to the SGD_iterator job.Increment the job number.
                SGD_iterator+=1
            SGD_num+=1
            
        # task = Pagerank
        if(task_type == 3):
            while(Pagerank_iterator < len(filePagerank.index) and filePagerank['job'][Pagerank_iterator] == Pagerank_num):
                list_to_send.append(filePagerank['flow_size'][Pagerank_iterator])
                #Send all flows mapped to the Pagerank_iterator job.Increment the job number.
                Pagerank_iterator+=1
            Pagerank_num+=1
        #print(list_to_send)
        create_heap(list_to_send)
        
    return
            
        

def create_heap(new_times):
    '''
    Creates the heap using current tasks

    format of new_times: [flow size]
    '''
    #create heap from all current items
    global fifo_queue
    for tim in new_times:
        fifo_queue.put(tim)
    return


def sjf():
    '''
    This function simply returns the top most entry on the heap i.e. the shortest job 
    '''
    #global fifo_queue
    try:
        t = fifo_queue.get_nowait()
        return t
    except:
        return -1


if __name__ == "__main__": 
    
    file1 = pandas.read_csv('job_incoming.csv', header=None, names=['job_type', 'incoming_time'])
    fileSGD = pandas.read_csv('SGD_test_jb.csv')
    filePagerank = pandas.read_csv('PageRank_test_jb.csv')
    fileKMeans = pandas.read_csv('KMeans_test_jb.csv')
    
    global count_KMeans
    global count_Pagerank
    global count_SGD
    count_KMeans = fileKMeans.iloc[-1]["job"]
    count_SGD = fileSGD.iloc[-1]["job"]
    count_Pagerank = filePagerank.iloc[-1]["job"]
    print(count_KMeans, count_SGD, count_Pagerank)
    
    tick1 = time.time()
    t_jobs = threading.Thread(target=job_incoming)
    t_jobs.start()
    
    lock = threading.Lock()
    workers = []
    for i in range(100):
        w = threading.Thread(target=worker, args=(lock,))
        workers.append(w)
        w.start()
    
    t_jobs.join()
    for w in workers:  # iterates over the threads
        w.join() 
    tick2 = time.time()
    print("WE are done!")
    print(tick2 - tick1)
    #file1.close()
    #fileSGD.close()
    #filePagerank.close()
    #fileKMeans.close()

'''
t_jobs is responsible to fetch new flows following the job_incoming file.
workers are the worker threads that supposedly execute the flows
We wait for both to complete using join
'''
