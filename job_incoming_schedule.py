import random

#job_type = []
#job_time = []

#Kmeans = 1, SGD = 2, PageRank = 3

c1 = 0  #lim = 91
c2 = 0  #lim = 34 
c3 = 0  #lim = 76

file1 = open('job_incoming.txt', 'w')

for i in range(201):
    job_type = random.randint(1, 3)
    
    if(c1 == 91):
        if(c2 == 34):
            job_type = 3
            c3 = c3 + 1
        elif(c3 == 76):
            job_type = 2
            c2 = c2 + 1
        else:
            job_type = random.randint(2, 3)
            if(job_type == 2):
                c2 = c2 + 1
            else:
                c3 = c3 + 1
                
            
    elif(c2 == 34):
        if(c1 == 91):
            job_type = 3
            c3 = c3 + 1
        elif(c3 == 76):
            job_type = 1
            c1 = c1 + 1
        else:
            job_type = random.choice([1,3])
            if(job_type == 1):
                c1 = c1 + 1
            else:
                c3 = c3 + 1
                
    
    elif(c3 == 76):
        if(c1 == 91):
            job_type = 2
            c2 = c2 + 1
        elif(c2 == 34):
            job_type = 1
            c1 = c1 + 1
        else:
            job_type = random.choice([1,2])
            if(job_type == 1):
                c1 = c1 + 1
            else:
                c2 = c2 + 1
    
    else:
        job_type = random.randint(1, 3)
        if(job_type == 1):
            c1 = c1 + 1
        elif(job_type == 2):
            c2 = c2 + 1
        else:
            c3 = c3 + 1
    
    
    
    job_time = random.randint(0, 100000)
    S = str(job_type) + ", " + str(job_time)
    file1.write(S)
    file1.write("\n")

file1.close()
print(str(c1) + " " + str(c2) + " " + str(c3)) 

 

