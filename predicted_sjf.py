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

def worker():
    #This is a worker thread. It fetches the topmost task and sleeps for the shortest job. 
    print("Worker started")
    while not (KMeans_num == 92 and SGD_num == 35 and Pagerank_num == 77 and len(sjf_heap) == 0):
        print("fetching task")
        t = sjf()
        if(t == -1):
            if(KMeans_num == 92 and SGD_num == 35 and Pagerank_num == 77 and len(sjf_heap) == 0):
                #This is to avoid deadlock. So, if a number of threads entered 
                return
            else:
                time.sleep(5)
        else:
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
            while(fileKMeans['job'][KMeans_iterator] == KMeans_num):
                new_flow = [fileKMeans['predicted_flow_size'][KMeans_iterator], fileKMeans['flow_size'][KMeans_iterator]]
                list_to_send.append(new_flow)
                #Send all flows mapped to the KMeans_iterator job.Increment the job number.
                KMeans_iterator+=1
            KMeans_num+=1
               
        # task = SGD
        if(task_type == 2):
            while(fileSGD['job'][SGD_iterator] == SGD_num):
                new_flow = [fileSGD['predicted_flow_size'][SGD_iterator], fileSGD['flow_size'][SGD_iterator]]
                list_to_send.append(new_flow)
                #Send all flows mapped to the SGD_iterator job.Increment the job number.
                SGD_iterator+=1
            SGD_num+=1
            
        # task = Pagerank
        if(task_type == 3):
            while(filePagerank['job'][Pagerank_iterator] == Pagerank_num):
                new_flow = [filePagerank['predicted_flow_size'][Pagerank_iterator], filePagerank['flow_size'][Pagerank_iterator]]
                list_to_send.append(new_flow)
                #Send all flows mapped to the Pagerank_iterator job.Increment the job number.
                Pagerank_iterator+=1
            Pagerank_num+=1
        print(list_to_send)
        create_heap(list_to_send)
        
    return
            
        

def create_heap(new_times):
    '''
    Creates the heap using current tasks

    format of new_times: [flow size]
    '''
    #create heap from all current items
    global sfj_heap
    #tim = [predicted, actual] flow sizes
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
        return t[1]
    except:
        return -1


if __name__ == "__main__": 
    
    file1 = pandas.read_csv('job_incoming.csv', header=None, names=['job_type', 'incoming_time'])
    fileSGD = pandas.read_csv('SGD_test_jb.csv')
    filePagerank = pandas.read_csv('PageRank_test_jb.csv')
    fileKMeans = pandas.read_csv('KMeans_test_jb.csv')
    
    t_jobs = threading.Thread(target=job_incoming)
    t_jobs.start()
    
    workers = []
    for i in range(100):
        w = threading.Thread(target=worker)
        workers.append(w)
        w.start()
    
    t_jobs.join()
    for w in workers:  # iterates over the threads
        w.join() 

    print("WE are done!")
    #file1.close()
    #fileSGD.close()
    #filePagerank.close()
    #fileKMeans.close()

'''
t_jobs is responsible to fetch new flows following the job_incoming file.
workers are the worker threads that supposedly execute the flows
We wait for both to complete using join
'''
