
#hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -file s.py -mapper 'python3 s.py' -file r.py -reducer 'python3 r.py' -input /new -output /output


import os

with open("query.txt", 'w') as file:
    file.write(input("Enter Query: "))                                                  #Accept Query from user
    file.close()                                                                        #and write it locally into query.txt for future reference

fd_query = open('query.txt','r')
in_query= fd_query.read()                                                               #Read query from file
in_query=(in_query.split())                                                             #and convert to 
#print(in_query)



if(in_query[0]=="SELECT"):                                                              #If it is a SELECT Query
    table=in_query[in_query.index('FROM')+1].split('/')[1].split('.')[0]                #Get the Table Name
    database=in_query[in_query.index('FROM')+1].split('/')[0]                           #Get the database Name
   
    locMap=os.popen("readlink -f mapper.py").read()
    locRed=os.popen("readlink -f reducer.py").read()
    os.system("hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -file mapper1.py -mapper 'python3 mapper1.py' -file reducer1_2.py -reducer 'python3 reducer1_2.py' -input /"+database+"/"+table+".csv -output /output/"+table)
    print()
    print("#######################    OUTPUT     ########################\n")
    for i in in_query:                                                                  #Print the Query
    	print(i,end=" ")
    print("\n")
    os.system("hdfs dfs -cat /output/"+table+"/part-00000")                             #Print the output which is stored in a patch file in hdfs
    print("##############################################################\n")
    os.system("hdfs dfs -rm -r /output/"+table)                                         #Delete the output file in hdfs to allow for the consequent file to be written
   
elif(in_query[0]=="LOAD"):                                                              #If LOAD Query

    db_load=in_query[1].split('/')[0]                                                   #Get Database name
    db_table=in_query[1].split('/')[1].split('.')[0]                                    #Get Table Name
   
    os.system("hdfs dfs -mkdir /project/"+db_load)                                      #Create folder in hdfs with database name
    os.system("hdfs dfs -mkdir /project/"+db_load+"/"+db_table)                         
    os.system("hdfs dfs -touchz /project/"+db_load+"/"+db_table+"/"+db_table+".csv")    #create csv file with table name in database folder
  
  
    f1=open(db_table+'.txt','w')
    for i in range(in_query.index('(')+1,in_query.index(')')):                          #create txt file with table name locally storing the schema
        f1.write(in_query[i])
    f1.close()
  
    location=os.popen("readlink -f "+db_table+".txt").read()
  
    #print(location)
    os.system("hdfs dfs -put "+location.rstrip()+" /project/"+db_load)
   

elif(in_query[0]=="DELETE"):                                                            #If DELETE query
   
    db_delete=in_query[1].split('/')[0]                                                 #Get database name
    table_delete=in_query[1].split('/')[1].split('.')[0]                                #Get table name
  
  
    os.system("hdfs dfs -rmr /project/"+db_delete+"/"+table_delete+".txt")              #Delete txt file of schema
    os.system("hdfs dfs -rmr /project/"+db_delete+"/"+table_delete)                     #Delete csv file of table

else:

    print("Invalid syntax")                                                             #If Invalid query submitted
