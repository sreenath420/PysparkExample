𝗪𝗵𝘆 𝗶𝘀 𝗟𝗮𝘇𝘆 𝗲𝘃𝗮𝗹𝘂𝗮𝘁𝗶𝗼𝗻 𝗜𝗺𝗽𝗼𝗿𝘁𝗮𝗻𝘁 𝗶𝗻 𝘀𝗽𝗮𝗿𝗸 ?

Lazy evaluation is a key concept in Spark for optimizing and managing the execution of transformations and actions on distributed datasets. It defers the execution of transformations until an action is called, which allows Spark to optimize the execution plan and improve performance by minimizing unnecessary computations.

𝗪𝗶𝘁𝗵𝗼𝘂𝘁 𝗟𝗮𝘇𝘆 𝗘𝘃𝗮𝗹𝘂𝗮𝘁𝗶𝗼𝗻:
Every transformation (like map and filter) would be immediately executed.
If the dataset is huge, unnecessary computation could happen upfront, consuming a lot of resources and time.
This can lead to performance issues and even out-of-memory errors for large datasets.

𝗪𝗶𝘁𝗵 𝗟𝗮𝘇𝘆 𝗘𝘃𝗮𝗹𝘂𝗮𝘁𝗶𝗼𝗻:
Spark doesn't execute any transformation until an action (like sum) is called.
It builds up a directed acyclic graph (DAG) of transformations, optimizing them along the way.
When an action is invoked, Spark analyzes the DAG and optimizes the execution plan before executing it.
This approach minimizes unnecessary computation and resource usage, making Spark more efficient, especially for big data processing.


data=[1,2,3,4,5]
rdd=sc.parallelize(data)
filter_rdd=rdd.filter(lambda x:x%2==0)
squred_rdd=filter_rdd.map(lambda x:x**2)
result=squred_rdd.reduce(lambda x,y:x+y)
print("Result",result)