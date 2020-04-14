#!/usr/bin/python
from random import randint
import heapq
from datetime import datetime
import time

numOfWorkers=2
busyWorker=[False,False]   # for showing if the worker is busy working on something
flag=True
heapEntry=[] # a list of lists. Each entry of this list will be of the form -[priority, date time of creation, task name]
allTasksFinished=False
totalTime=0
#Every datapoint has a time associated with it using which we will schedule the tasks that are given.
#Assume every 1 second some n (random number between 1 to 10) number of tasks are added to the queue (priority queue)
#initialStartTime=datetime.now()
intialTime=time.time()
print("intialTime",intialTime)


while True: #repeats every second
	print("Inside first while")
	starttime=time.time()
	if allTasksFinished==True:
		#totalTime=datetime.now()-initialStartTime
		#print("Total time:", totalTime.strftime("%H-%M-%S"))
		print("All tasks finished")
		break

	listOfTasks = {} 
	numOfNewTasks=randint(1,5)
	currLenDict=len(listOfTasks)
	# when new entries enter the queue:
	print("number of new tasks:", numOfNewTasks)
	print(" initial listOfTasks",listOfTasks)

	for x in range(currLenDict+1,currLenDict+numOfNewTasks+1):

	    key="Task"+str(x)
	    val=1
	    now = datetime.now() # time object -- now.strftime("%Y%m%d%H%M%S") 
	    timeToCompletion=randint(1,10)
	    listOfTasks[key]=[val,now.strftime("%Y%m%d%H%M%S"),timeToCompletion]

	print(" final listOfTasks",listOfTasks)

	#create a heap for the first time:
	if flag==True:
		print(" creating heap for the fisrt time")
		for keyTaskName in listOfTasks:
		    tempList=[]
		    tempList.append(listOfTasks[keyTaskName][0])      #priority
		    tempList.append(int(listOfTasks[keyTaskName][1])) #time of creation
		    tempList.append(listOfTasks[keyTaskName][2])	  #time required to complete this task
		    tempList.append(keyTaskName)                      #task name
		    heapEntry.append(tempList)
		heapq.heapify(heapEntry) 
		print(" created heap for the fisrt time")
		print("heap is:", heapEntry)
		flag=False
	else:
		print(" adding elements to heap")
		for keyTaskName in listOfTasks:
		    tempList=[]
		    tempList.append(listOfTasks[keyTaskName][0])      #priority
		    tempList.append(int(listOfTasks[keyTaskName][1])) #time of creation
		    tempList.append(listOfTasks[keyTaskName][2])	  #time required to complete this task
		    tempList.append(keyTaskName)                      #task name
		    heapq.heappush(heapEntry,tempList)
		    print('element added to heap:', tempList)
		    print("current heap:", heapEntry)

	while True:
		print("inside 2nd while")
		if len(heapEntry)>0:
			print("heap length:", len(heapEntry))
			if all(boolean==True for boolean in busyWorker):
				print("All workers busy")
				continue

			for x in range(numOfWorkers):
				if busyWorker[x]==False and len(heapEntry)>0:
					print("worker",x,"is free")
					busyWorker[x]=True
					tempTask=heapq.heappop(heapEntry)
					print("popped entry:", tempTask)
					tempTask[2]=tempTask[2]-1 #reduced remaining completion time by 1 sec
					if tempTask[2]==0:
						print("temptask finished:", tempTask)
						continue # will not add that task in the heap again as it is finished
					else:
						
						tempTask[0]=tempTask[0]+1
						print("temptask remaining work:", tempTask)
						heapq.heappush(heapEntry, tempTask)
						print("pushed", tempTask, "back into heap")
						print("new heap:", heapEntry)
			print("going to sleep for a sec")
			print("time.time()%1.0", time.time()%1.0)
			print("starttime%1.0", starttime%1.0)
			print("sleeping for", 1.0-(time.time()-starttime)% 1.0)
			time.sleep(1.0-(time.time()-starttime)%1.0) #wait for one second
			print("going out of sleep")
			for x in range(numOfWorkers):	#release all workers from work after every second
				busyWorker[x]=False
				print("released all workers from work")
			#Add break inner while loop here to get new entries in list every second.
			#break;	

		else:
			print("mark all tasks finished tag as true")
			allTasksFinished=True
			break

