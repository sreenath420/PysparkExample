#Map and flatMap Transformation 
#Example:-1
sc.parallelize([3,4,5]).map(lambda x: range(1,x)).collect()


Out[3]: [range(1, 3), range(1, 4), range(1, 5)]


sc.parallelize([3,4,5]).flatMap(lambda x: range(1,x)).collect()

Out[4]: [1, 2, 1, 2, 3, 1, 2, 3, 4]

#Example:-2

sc.parallelize([3,4,5]).map(lambda x: [x,  x*x]).collect() 

Out[5]: [[3, 9], [4, 16], [5, 25]]

sc.parallelize([3,4,5]).flatMap(lambda x: [x,  x*x]).collect() 

Out[6]: [3, 9, 4, 16, 5, 25]

#Example:-3

lines = sc.textFile("dbfs:/FileStore/greetings.txt")
lines.map(lambda line: line.split()).collect()

Out[8]: [['Good', 'Morning'],
 ['Good', 'Evening'],
 ['Good', 'Day'],
 ['Happy', 'Birthday'],
 ['Happy', 'New', 'Year']]
 
 lines.flatMap(lambda line: line.split()).collect()
 
 

Out[9]: ['Good',
 'Morning',
 'Good',
 'Evening',
 'Good',
 'Day',
 'Happy',
 'Birthday',
 'Happy',
 'New',
 'Year']