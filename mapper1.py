#!usr/bin/python3
import os
import sys
import re
import csv

q=open("query.txt","r")                                                                                                 #Read query from locally stored file
com=q.read()
com=com.split()
q.close()

def popleft(x):
	return x[0],x[1:]
	
#if SELECT operation
if com==["exit"]:
	print("Exiting..")
elif com[0]=="SELECT":
	table=com[com.index("FROM")+1].split('/')[1].split('.')[0]                                                      #Get table name from query
	db=com[com.index("FROM")+1].split('/')[0]                                                                       #Get database name from query
	infile = sys.stdin                                                                                              #Recieve data piped from driver file
	line=list()
	for l in infile:
		line.append(l.strip())                                                                                  #clean and store data
	
	for i in range(len(line)):                                                                                      #Remove Empty rows from csv file
		l,line = popleft(line)
		l=l.split(',')
		while "" in l:
			l.remove("")
		line.append(l)
	while [] in line:
		line.remove([])
		
	fd_Sch=open(table+".txt","r")                                                                                   #Access schema file
	sch = fd_Sch.read().strip('\n')
	fd_Sch.close()
	#print(schema)
	sch=sch.split(',')

	out=list()
	arg=
	output=dict()
	if com[1][:3] not in ["MAX","MIN","AVG","SUM"]:                                                                 #If not aggregate query
		if "WHERE" not in com:                                                                                  #If no WHERE clause in query
			ind=list()
			if com[1]=="*":                                                                                 #If select all operation
				for l in line:
					stri=""
					for i in range(len(l)):
						stri+=str(l[i])+","
					out.append(stri[:(len(stri)-1)])                                                #Add to output file
			else:
				for co in com[1].split(','):                                                            #If Select specific columns query
					if (co in sch):
						ind.append(sch.index(co))                                               #get list of columns
				for l in line:
					stri=""
					for i in range(len(ind)):
						stri+=str(l[ind[i]])+','
					out.append(stri[:(len(stri)-1)])                                                #Add to output file
		else:                                                                                                   #If WHERE clause in query
			if "="==com[com.index("WHERE")+2]:                                                              #If Condition operator is =
				condcol=com[com.index("WHERE")+1]                                                       #find condition column
				if "\n" in condcol:
					condcol.remove("\n")
				condval=com[com.index("=")+1]                                                           #find condition value
				ind=list()
				if com[1]=="*":                                                                         #Refer Above
					for l in line:
						if l[sch.index(condcol)]==condval:
							stri=""
							for i in range(len(l)):
								stri+=str(l[i])+','
							out.append(stri[:(len(stri)-1)])
				else:
					for co in com[1].split(','):
						if co in sch:
							ind.append(sch.index(co))
					for l in line:
						if l[sch.index(condcol.rstrip('\n\n'))]==condval:
							stri=""
							for i in range(len(ind)):
								stri+=str(l[ind[i]])+','
							out.append(stri[:(len(stri)-1)])
			if ">" == com[com.index("WHERE")+2]:                                                            #If condition operator is >
				condcol=com[com.index("WHERE")+1]
				condval=com[com.index(">")+1]
				ind=list()
				if com[1]=="*":
					for l in line:
						if float(l[sch.index(condcol)])>float(condval):
							stri=""
							for i in range(1,len(l)):
								stri+=str(l[i])+','
							out.append(stri[:(len(stri)-1)])
				else:
					for co in com[1].split(','):
						if co in sch:
							ind.append(sch.index(co))
					for l in line:
						if float(l[sch.index(condcol)])>float(condval):
							stri=""
							for i in range(len(ind)):
								stri+=str(l[ind[i]])+','
							out.append(stri[:(len(stri)-1)])
			if "<" == com[com.index("WHERE")+2]:                                                            #If Condition operator is <
				condcol=sch.index(com[com.index("WHERE")+1])
				condval=com[com.index("<")+1]
				ind=list()
				if com[1]=="*":
					for l in line:
						if float(l[condcol])<float(condval):
							stri=""
							for i in range(1,len(l)):
								stri+=str(l[i])+','
							out.append(stri[:(len(stri)-1)])
				else:
					for co in com[1].split(','):
						if co in sch:
							ind.append(sch.index(co))
					for l in line:
						if float(l[condcol])<float(condval):
							stri=""
							for i in range(len(ind)):
								stri+=str(l[ind[i]])+','
							out.append(stri[:(len(stri)-1)])
	else:                                                                                                           #if Aggregate query
		if "MIN" in com[1]:                                                                                     #if MIN query
			arg="MIN"                                                                                       #save arg as MIN for reducer to use
		elif "MAX" in com[1]:                                                                                   #if MAX query
			arg="MAX"
		elif "AVG" in com[1]:                                                                                   #if AVG query
			arg="AVG"
		elif "SUM" in com[1]:                                                                                   #if SUM query
			arg="SUM"
		print(arg)
		com[1]=com[1][4:(len(com[1])-1)]                                                                        #Remove Aggregate name from query to perform normal SELECT query
		if "WHERE" not in com:
			ind=list()                                                                                      #Refer Above
			if com[1]=="*":
				for l in line:
					stri=""
					for i in range(len(l)):
						stri+=str(l[i])+","
					out.append(stri[:(len(stri)-1)])
			else:
				for co in com[1].split(','):
					if (co in sch):
						ind.append(sch.index(co))
				for l in line:
					stri=""
					for i in range(len(ind)):
						stri+=str(l[ind[i]])+','
					out.append(stri[:(len(stri)-1)])
		else:
			if "="==com[com.index("WHERE")+2]:
				condcol=com[com.index("WHERE")+1]
				if "\n" in condcol:
					condcol.remove("\n")
				condval=''
				condval=com[com.index("=")+1]
				ind=list()
				if com[1]=="*":
					for l in line:
						if l[sch.index(condcol)]==condval:
							stri=""
							for i in range(len(l)):
								stri+=str(l[i])+','
							out.append(stri[:(len(stri)-1)])
				else:
					for co in com[1].split(','):
						if co in sch:
							ind.append(sch.index(co))
					for l in line:
						if l[sch.index(condcol.rstrip('\n\n'))]==condval:
							stri=""
							for i in range(len(ind)):
								stri+=str(l[ind[i]])+','
							out.append(stri[:(len(stri)-1)])
			if ">" == com[com.index("WHERE")+2]:
				condcol=com[com.index("WHERE")+1]
				condval=com[com.index(">")+1]
				ind=list()
				if com[1]=="*":
					for l in line:
						if float(l[sch.index(condcol)])>float(condval):
							stri=""
							for i in range(1,len(l)):
								stri+=str(l[i])+','
							out.append(stri[:(len(stri)-1)])
				else:
					for co in com[1].split(','):
						if co in sch:
							ind.append(sch.index(co)) 
					for l in line:
						print(l[sch.index(condcol)],condval)
						if float(l[sch.index(condcol)])>float(condval):
							stri=""
							for i in range(len(ind)):
								stri+=str(l[ind[i]])+','
							out.append(stri[:(len(stri)-1)])
			if "<" == com[com.index("WHERE")+2]:
				condcol=sch.index(com[com.index("WHERE")+1])
				condval=com[com.index("<")+1]
				ind=list()
				if com[1]=="*":
					for l in line:
						if float(l[condcol])<float(condval):
							stri=""
							for i in range(1,len(l)):
								stri+=str(l[i])+','
							out.append(stri[:(len(stri)-1)])
				else:
					for co in com[1].split(','):
						if co in sch:
							ind.append(sch.index(co))
					for l in line:
						if float(l[condcol])<float(condval):
							stri=""
							for i in range(len(ind)):
								stri+=str(l[ind[i]])+','
							out.append(stri[:(len(stri)-1)])
					
				
	for ou in out:                                                                                                  #Print output for reducer.py to use
		print("%s"%(ou))
