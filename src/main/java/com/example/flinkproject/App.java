import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
// import org.apache.flink.graph.Vertex.ComputeFunction;
import org.apache.flink.graph.pregel.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;



// import org.apache.flink.graph.library.PageRank;
// import org.apache.flink.graph.library.PageRank.Result;
import org.apache.flink.types.NullValue;
import java.util.*; 

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.logging.Logger;

public class App {
    private static final Logger logger = Logger.getLogger(App.class.getName());
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Double>> vertexTuples = env.readCsvFile("/home/flink-example/data/random-vertex.txt").
            types(Long.class, Double.class);
        DataSet<Tuple3<Long, Long, Double>> edgeTuples = env.readCsvFile("/home/flink-example/data/random-edge.txt").
            types(Long.class, Long.class, Double.class);
        
        DataSet<Tuple2<Long, Double>> partitionedVertex = vertexTuples.partitionCustom(new CustomPartitioner(), 0).setParallelism(2);
        DataSet<Tuple3<Long, Long, Double>> partitionedEdge = edgeTuples.partitionCustom(new CustomPartitioner(), 0).setParallelism(2);
        partitionedVertex.writeAsText("/home/flink-example/data/result.txt",WriteMode.OVERWRITE);


        DataSet<Tuple2<Long, Double>> InitpartitionedVertex = partitionedVertex.map(new MapFunction<Tuple2<Long,Double>,Tuple2<Long,Double> >() {
            @Override
            public Tuple2<Long, Double> map(Tuple2<Long,Double> vertix) throws Exception {
                return Tuple2.of(vertix.f0,Double.POSITIVE_INFINITY);
            }
        });
        Long src = 4L;
        DataSet<Tuple2<Long,Double>> message = env.fromElements(new Tuple2<Long,Double>(src,0d));
        message = env.fromElements(new Tuple2<Long,Double>(src,1d));
        Long step = 0L;
        while(message.count() != 0){
            step = step + 1;
            message.writeAsText("/home/flink-example/data/result.txt/step"+Long.toString(step), WriteMode.OVERWRITE);
            System.out.println("step " + Long.toString(step) +" count:" + Long.toString(message.count()));
            DataSet<Tuple2<Long,Double>> change = message.groupBy(0).min(1)
                .join(InitpartitionedVertex).where(0).equalTo(0).with((a,b) -> 
                    new Tuple3<Long,Double,Double>(a.f0, a.f1, b.f1)
                ).returns(Types.TUPLE(Types.LONG, Types.DOUBLE,Types.DOUBLE)).filter(a -> a.f1 < a.f2).map(a -> new Tuple2<Long,Double>(a.f0, a.f1)).returns(Types.TUPLE(Types.LONG, Types.DOUBLE));

            InitpartitionedVertex = InitpartitionedVertex.union(change).groupBy(0).min(1);

            // change.writeAsText("/home/flink-example/data/result.txt/change.txt",WriteMode.OVERWRITE);

            // DataSet<Tuple2<Long, Double>> Testmessage = change.flatMap(
            //     new FlatMapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>>()  {
            //         @Override
            //         public void flatMap(Tuple2<Long,Double> vertex, Collector<Tuple2<Long,Double>> out) {
            //             try {
            //                 partitionedEdge
            //                     .filter(edge -> edge.f0 == vertex.f0).collect()
            //                     .forEach(edge -> 
            //                         out.collect(new Tuple2<Long, Double>(edge.f1, vertex.f1 + 1)) 
            //                     );
            //             } catch (Exception e) {
            //                 System.out.println("Exception get in App.java line 78");
            //             }
            //         }
            //     });

            message = change.join(partitionedEdge).where(0).equalTo(0).with((a,b) -> new Tuple2<Long,Double>(b.f1, a.f1 + 1.0d)).returns(Types.TUPLE(Types.LONG, Types.DOUBLE));
            System.out.println("next step " + Long.toString(step) + " change count:" +  Long.toString(change.count()) +" message count:" + Long.toString(message.count()));
            
            // .flatMap(
            //     new FlatMapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>>()  {
            //         @Override
            //         public void flatMap(Tuple2<Long,Double> vertex, Collector<Tuple2<Long,Double>> out) {
            //             try {
            //                 partitionedEdge
            //                     .filter(edge -> edge.f0 == vertex.f0).collect()
            //                     .forEach(edge -> 
            //                         out.collect(new Tuple2<Long, Double>(edge.f1, vertex.f1 + 1)) 
            //                     );
            //             } catch (Exception e) {
            //                 System.out.println("Exception get in App.java line 78");
            //             }
            //         }
            //     });
            // Testmessage.writeAsText("/home/flink-example/data/result.txt/Test",WriteMode.OVERWRITE);
            // break;

            // message = change.flatMap((Tuple2<Long, Double> vertex, Collector<Tuple2<Long, Double>> out) -> {
            //     partitionedEdge
            //         .filter(edge -> edge.f0 == vertex.f0)
            //         .forEach(edge -> {
            //             out.collect(new Tuple2<>(edge.f1, vertex.f1 + 1));
            //         });
            // });
            // break;
            // if(step == 5) break;
        }
        InitpartitionedVertex.writeAsText("/home/flink-example/data/result.txt/Init",WriteMode.OVERWRITE);
        // DataStream<Integer> mergedStream = initialStream;

        // for (int i = 0; i < 3; i++) {
        //     DataStream<Integer> newStream = env.fromElements(i * 10, i * 10 + 1, i * 10 + 2);

        //     mergedStream = mergedStream.union(newStream);
        // }

        // mergedStream.print();

        // DataSet<Tuple2<Long, Double>> partitionedVertex = vertexTuples
        //         .partitionByHash(0);
    
        // DataSet<Tuple3<Long, Long, Double>> partitionedEdge = edgeTuples
        //         .partitionByHash(0);

        // partitionedVertex.writeAsText("/home/flink-exmaple/data/test.txt");
        // DataSet<Tuple2<Long, Double>> partitionedVertex = vertexTuples.
        //     partitionCustom(new MyVertexPartitioner(), new VertexKeySelector()) 
        //     .map(new IdentityMapper<Vertex<Long, Double>>());


        // Graph<Long, Double, Double> graph = Graph.fromTupleDataSet(partitionedVertex, partitionedEdge, env);
        // graph.
        // // Graph<Long, Double, Double> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);

        // // define the maximum number of iterations
        // int maxIterations = 10;
        // // Execute the vertex-centric iteration
        // Graph<Long, Double, Double> result = graph.runVertexCentricIteration(
        //             new SSSPComputeFunction(), new SSSPCombiner(), maxIterations);

        // DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();
        // singleSourceShortestPaths.printToErr();
        // singleSourceShortestPaths.writeAsText("/home/flink-example/data/result.txt",WriteMode.OVERWRITE);



        // for (Vertex<Long, Double> vertex : singleSourceShortestPaths.map()) {
        //     System.out.println("Vertex ID: " + vertex.getId() + ", Shortest Distance: " + vertex.getValue());
        // }
        System.out.println("Hello, World!");
        // int maxIterations = 10;
        // double dampingFactor = 0.85;
        // Graph<Long, NullValue, Double> resultGraph = graph.run(new PageRank<Long, Double>(maxIterations, dampingFactor));

        // DataSet<Tuple2<Long, Double>> pageRanks = resultGraph.getVertices()
        //     .map(new MapFunction<Vertex<Long, Result>, Tuple2<Long, Double>>() {
        //         @Override
        //         public Tuple2<Long, Double> map(Vertex<Long, Result> vertex) throws Exception {
        //             return new Tuple2<Long, Double>(vertex.f0, vertex.f1.getPageRankScore());
        //         }
        //     });

        // pageRanks.print();vi

        env.execute("Flink Gelly Example");
        
    }

    public static class CustomPartitioner implements Partitioner<Long> {
        @Override
        public int partition(Long key, int numPartitions) {
            int partId = key.intValue() % 2;
            return partId;
            // return 0;
        }
    }

    public static final class SSSPComputeFunction extends ComputeFunction<Long, Double, Double, Double> {
        public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> messages) {
            int superstep = getSuperstepNumber();
            if(superstep == 1) {
                setNewVertexValue(Double.POSITIVE_INFINITY);
                if(vertex.getId() == 4L)
                    sendMessageTo(vertex.getId(),0d);
            }else{
                double minDistance = Double.POSITIVE_INFINITY;
                for (Double msg : messages) {
                    minDistance = Math.min(minDistance, msg);
                }
                if (minDistance < vertex.getValue()) {
                    setNewVertexValue(minDistance);
                    for (Edge<Long, Double> e: getEdges()) {
                        sendMessageTo(e.getTarget(), minDistance + e.getValue());
                    }
                }
            }

        }
    }
    // message combiner
    public static final class SSSPCombiner extends MessageCombiner<Long, Double> {
        public void combineMessages(MessageIterator<Double> messages) {
            double minMessage = Double.POSITIVE_INFINITY;
            for (Double msg: messages) {
                minMessage = Math.min(minMessage, msg);
            }
            sendCombinedMessage(minMessage);
        }
    }
}


