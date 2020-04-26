import heapq
import logging
import threading
import time
import pandas

sjf_heap = []

KMeans_iterator = 0
SGD_iterator = 0
Pagerank_iterator = 0
KMeans_num = 1
SGD_num = 1
Pagerank_num = 1

aging_time = 1000

count_KMeans = 0
count_Pagerank = 0
count_SGD = 0

def worker(lock):
    global aging_time
    #This is a worker thread. It fetches the topmost task and sleeps for the shortest job. 
    #print("Worker started")
    while not (KMeans_num > count_KMeans and SGD_num > count_SGD and Pagerank_num > count_Pagerank and len(sjf_heap) == 0):
        #print("fetching task")
        lock.acquire()
        t = sjf()
        lock.release()
        #print(t)
        if(t[0] == -1 and t[1] == -1):
            #print(KMeans_num, SGD_num, Pagerank_num)
            if(KMeans_num > count_KMeans and SGD_num > count_SGD and Pagerank_num > count_Pagerank and len(sjf_heap) == 0):
                #This is to avoid deadlock. So, if a number of threads entered 
                return
            else:
                time.sleep(5)
        else:
            sleep_time = 0
            to_add = False
            t_add = [t[0] + 1, 0]
            
            if(aging_time>=t[1]):
                sleep_time = t[1]
            else:
                sleep_time = aging_time
                to_add = True
                t_add[1] = t[1] - aging_time
                
            sleep_time = sleep_time/100
            time.sleep(sleep_time)
            if(to_add):
                lock.acquire()
                create_heap([t_add])
                lock.release()
            
    return
        
    
    
def job_incoming(lock):
    global KMeans_iterator
    global KMeans_num
    global SGD_iterator
    global SGD_num
    global Pagerank_iterator
    global Pagerank_num
    
    for row in file1.index:
        #print(file1.head(2))
        task_type = file1['job_type'][row]
        sleep_time = file1['incoming_time'][row] / 100
        time.sleep(sleep_time)
        
        # task = KMeans
        list_to_send = []
        if(task_type == 1):
            while(KMeans_iterator < len(fileKMeans.index) and fileKMeans['job'][KMeans_iterator] == KMeans_num):
                new_flow = [1, fileKMeans['flow_size'][KMeans_iterator]]
                list_to_send.append(new_flow)
                #Send all flows mapped to the KMeans_iterator job.Increment the job number.
                KMeans_iterator+=1
            KMeans_num+=1
               
        # task = SGD
        if(task_type == 2):
            while(SGD_iterator < len(fileSGD.index) and fileSGD['job'][SGD_iterator] == SGD_num):
                new_flow = [1, fileSGD['flow_size'][SGD_iterator]]
                list_to_send.append(new_flow)
                #Send all flows mapped to the SGD_iterator job.Increment the job number.
                SGD_iterator+=1
            SGD_num+=1
            
        # task = Pagerank
        if(task_type == 3):
            while(Pagerank_iterator < len(filePagerank.index) and filePagerank['job'][Pagerank_iterator] == Pagerank_num):
                new_flow = [1, filePagerank['flow_size'][Pagerank_iterator]]
                list_to_send.append(new_flow)
                #Send all flows mapped to the Pagerank_iterator job.Increment the job number.
                Pagerank_iterator+=1
            Pagerank_num+=1
        #print(list_to_send)
        lock.acquire()
        create_heap(list_to_send)
        lock.release()
        
    return
            
        

def create_heap(new_times):
    '''
    Creates the heap using current tasks

    format of new_times: [flow size]
    '''
    #create heap from all current items
    global sfj_heap
    #print(new_times)
    for tim in new_times:
        #sort by completion time, then by FIFO
        heapq.heappush(sjf_heap, tim)
    return


def sjf():
    '''
    This function simply returns the top most entry on the heap i.e. the shortest job 
    '''
    try:
        t = heapq.heappop(sjf_heap)
        #return the actual time
        return t
    except:
        return [-1, -1]


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
    
    lock = threading.Lock()
    
    tick1 = time.time()
    t_jobs = threading.Thread(target=job_incoming, args=(lock,))
    t_jobs.start()
    
    
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
