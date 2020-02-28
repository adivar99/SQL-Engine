# SQL Engine
 November 2020
 
# Abstract

This project implements a SQL Engine using HDFS.



# Motivation
  
   HDFS allows for quick and efficient access of large databases from the server. SQL also allows for efficient querying of dataset stored in the form of tables. 
   This project also allowed me to gain a better understanding on curriculum based topics.
   
# Execution
1. Start hadoop on your system
2. Load database file onto hdfs file server using __hdfs dfs -put ~/Desktop/Project/database /Project__
3. Open terminal and navigate to file where files are stored.
4. Run __python3 main.py__
5. You will be prompted to enter a query
6. output should be printed at the end

# Format for Queries
1. SELECT * FROM Project/Student.csv
2. SELECT Name,Roll FROM Project/Staff.csv WHERE Department = EEE
3. SELECT MIN(rating) FROM Project/Interactions.csv
4. LOAD Something/Example AS ( col1:int , col2:str )
5. DELETE Something

# Limitations
1. The operations allowed are SELECT, LOAD, and DELETE
2. In SELECT query, WHERE clause is supported for =,> and <
3. Aggregate functions supported are MIN, MAX, AVG and SUM
4. LOAD queries only support creating of directories for tables and loading schema for the table as specified

# Platforms used
1. Hadoop 2.7.2
2. Python 3.5.2

# Conclusion
Creating the project was a great learning experience. Hope to improve the efficiency of the Engine and eliminate as many limitations as possible. Feel free to reach out to me to create a branch and/or any improvements to the project. 
