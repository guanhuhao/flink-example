import os
import re
import time
import math
current_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_path)

def getTime():
    timestamp = time.time()
    struct_time = time.localtime(timestamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", struct_time)+": "

data_path = "../data/"
dataset_list = [
    "wiki_new.txt",         "wiki_new.txt-swap.txt",\
    "out.dbpedia-location", "out.dbpedia-location-swap.txt",\
    "out.github",           "out.github-swap.txt",\
    "out.actor-movie",      "out.actor-movie-swap.txt",\
    "out.dbpedia-team",     "out.dbpedia-team-swap.txt",\
    ]
dataset_list = [
    "wiki_new.txt",
    "out.github",  
    ]

method_list = ["basic","anti-basic","entropy","MinMax","HYPE","KaHyPar","random"]
method_list = ["random"]
pp = [2,4,8,16]
# pp = [2]
# w = 8
with open("py.log","w") as pylog:
    for p in pp:
        w = p
        for dataset in dataset_list:
            for method in method_list:
                execCmd = "stdbuf -o0 hadoop jar /home/giraph-example/target/giraph-example-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.GHHRunner" 
                function = " com.example.ShortestPathComputation " 
                # function = " com.example.PageRankComputation " 
                # inputFile = "\"hdfs://node1:9003/data/test_data/"+str(p)+"/"+dataset+"/giraph/"+method+".txt\""
                vertFile = "\"hdfs://node1:9003/data/test_data/"+str(p)+"/"+dataset+"/flink/"+method+"-vertex.txt\""
                edgeFile = "\"hdfs://node1:9003/data/test_data/"+str(p)+"/"+dataset+"/flink/"+method+"-vertex.txt\""
                resuFile = "\"hdfs://node1:9003/result/"+dataset+"/"+method+"/"+str(p)+"\""
                # setp = "-p " + str(p)
                # setw = "-w " + str(w)

                logFile =  "./result_log/" + dataset + "/" + method + "/" + str(p) + "/log.txt"
                log = " > " + logFile + " 2>&1"
                if not os.path.exists("./result_log/"+dataset+"/"+method+"/"+str(p)):
                    os.makedirs("./result_log/"+dataset+"/"+method+"/"+str(p))
                # p = 20
                cmd = execCmd + function + " -input " + inputFile + " -output " + outputFile + " -p " + str(p) + " -w " + str(w) + " " + log
                pylog.write(getTime()+"begin task "+dataset+" "+method+" p:"+str(p) +" w:"+str(w)+"\n")
                print(cmd)
                pylog.flush()
                os.system("hadoop dfs -rm -r "+ outputFile)
                os.system(cmd)

                # with open(logFile,"r") as file:
                # pattern = r'application_\d{13}_\d{4}'
                # match = re.search(pattern, input_string)
                
                # pylog.write(getTime()+"finished task "+dataset+" "+method+" p:"+str(p) +" w:"+str(w)+"\n")
            #     break
            # break  