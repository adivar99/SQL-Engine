#!usr/bin/python3

import os
import sys

aggrflag=0                                              #Initialise variables
data=list()
dat=list()
aggr=["MIN","MAX","AVG","SUM"]

def average(x):
	s=0
	for i in x:
		s+=float(i)
	return s/len(x)



for l in sys.stdin:
	data.append(l.strip())                          #store and clean input from mapper file

while "" in data:                                       #Remove any empty values
	data.remove("")

for i in aggr:                                          #Check if Aggregate function
	if i in data:
		aggrflag=1                              #Set flag
		arg=i
		break

if aggrflag:                                            #If Aggregate query,
	data.remove(arg)                                #remove aggregate name from input
	try:
		for i in data:                          #Try converting to float values
			dat.append(float(i))
		if arg == "MIN":                        #If MIN query
			res = min(dat)
		elif arg == "MAX":                      #If Max query
			res = max(dat)
		elif arg == "AVG":                      #If AVG query
			res = average(dat)
		if arg =="SUM":                         #If SUM query
			res = sum(dat)
	except ValueError:                              #If could not convert values to float,
		res="Invalid Datatype"                  #store error message
	print(res)                                      #Print computed value

else:                                                   #If not Aggregate function
	for i in data:                                  #print input values as they are
		print(i)

