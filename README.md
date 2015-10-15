## MongoDB-InsertDocumentApp
* Java application for insert data into MongoDB.
* Data source: http://grouplens.org/datasets/movielens/
* To put it simply, those source files are included inside the project under doc folder.

####Another way to import data into MongoDB: 
Write a .sh/.bash file.

######A sample for import data using .sh file:
```
#!/bin/bash
_dfiles=/Users/hello/Downloads/NYSE/*.csv
for f in $_dfiles
do
mongoimport --type csv -d hw3 -c nyse --headerline --file ${f} --jsonArray
done
```
