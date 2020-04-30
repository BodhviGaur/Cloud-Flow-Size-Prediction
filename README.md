# Cloud-Flow-Size-Prediction

Paper Followed: Is advance knowledge of flow sizes a plausible assumption?https://www.usenix.org/conference/nsdi19/presentation/dukic

Note:
To compare the effect of knowing flow sizes on the scheduling algorithms, we have to organise the way the tasks reach the scheduler. This must be the same for us to test/compare how the scheduling algorithms are affected. We do this in the following manner:
We will make a list of all tasks for testing by randomly choosing between the (1-KMeans, 2-SGD, 3-Pagerank) and allocate a starting time randomly between (0 and 1,00,000).


We have created a miniature dataset to allow testing (we have a flow in the actual test set amounting to a whooping 93 hrs. This has been doen realising no one will run our test that long).  
This consists of 10 jobs from KMeans, SGD and Pagerank.   
SJF ideal : 4762.1541028.  

FIFO ideal : 5765.56560111.  
Aging (500ms) : 4564.5097878.   
Aging (50ms) : 5082.73901892.  
SJF Predicted with Neural Network : 4672.01515794. 

Runtimes
NN PageRank: 1551 seconds (25 min)   
NN KMeans: 460.17 seconds (7.6 min) 
NN SGD: 5798.993380 seconds (1.6 hours)


