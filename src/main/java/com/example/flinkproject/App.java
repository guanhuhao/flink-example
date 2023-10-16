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
import org.apache.flink.api.java.utils.ParameterTool;


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
        ParameterTool params = ParameterTool.fromArgs(args);
        String partNum = params.get("p", "2");
        String vertFile = params.get("v", "/home/flink-example/data/entropy-vertex.txt");
        String edgeFile = params.get("e", "/home/flink-example/data/entropy-edge.txt");
        String resultFile = params.get("r", "/home/flink-example/data/result");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Double>> vertexTuples = env.readCsvFile(vertFile).
            types(Long.class, Double.class);
        DataSet<Tuple3<Long, Long, Double>> edgeTuples = env.readCsvFile(edgeFile).
            types(Long.class, Long.class, Double.class);
        env.setParallelism(Integer.parseInt(partNum));
        DataSet<Tuple2<Long, Double>> partitionedVertex = vertexTuples.partitionCustom(new CustomPartitioner(), 0);
        // setParallelism(2);
        DataSet<Tuple3<Long, Long, Double>> partitionedEdge = edgeTuples.partitionCustom(new CustomPartitioner(), 0);//.setParallelism(2);
        // partitionedVertex.writeAsText(resultFile ,WriteMode.OVERWRITE);

        DataSet<Tuple3<Long, Long, Double>> Edge =  partitionedEdge.partitionCustom(new CustomPartitioner(), 0);

        DataSet<Tuple2<Long, Double>> Vert =  partitionedVertex.map(
            new MapFunction<Tuple2<Long,Double>,Tuple2<Long,Double> >() {
                @Override
                public Tuple2<Long, Double> map(Tuple2<Long,Double> vertix) throws Exception {
                    return Tuple2.of(vertix.f0,Double.POSITIVE_INFINITY);
                }
            });
        // DataSet<Tuple2<Long, Double>> InitpartitionedVertex = partitionedVertex.map(new MapFunction<Tuple2<Long,Double>,Tuple2<Long,Double> >() {
        //     @Override
        //     public Tuple2<Long, Double> map(Tuple2<Long,Double> vertix) throws Exception {
        //         return Tuple2.of(vertix.f0,Double.POSITIVE_INFINITY);
        //     }
        // });
        Long src = Vert.min(0).getInput().first(1).collect().get(0).f0;
        DataSet<Tuple2<Long,Double>> message1 = env.fromElements(new Tuple2<Long,Double>(src,0d));
        message1 = env.fromElements(new Tuple2<Long,Double>(src,1d));
        DataSet<Tuple2<Long,Double>> hyperEdge = Vert.filter(vertex -> vertex.f0 % 2 == 1);
        DataSet<Tuple2<Long,Double>> hyperVert = Vert.filter(vertex -> vertex.f0 % 2 == 0);
        Long step = 0L;
        while(message1.count() != 0){
            step = step + 1;
            if (step == 2) break;
            // Vert.writeAsText("/home/flink-example/data/result.txt/step"+Long.toString(step), WriteMode.OVERWRITE);
            System.out.println("step " + Long.toString(step));

            // state 1
            DataSet<Tuple2<Long,Double>> aggMessage1 = message1.groupBy(0).min(1).partitionCustom(new CustomPartitioner(), 0);
            DataSet<Tuple2<Long,Double>> updatedVert = aggMessage1.join(Vert)
                .where(0).equalTo(0).with(
                    (message,vertex) -> new Tuple3<Long,Double,Double>(message.f0, message.f1, vertex.f1)
                ).returns(Types.TUPLE(Types.LONG, Types.DOUBLE,Types.DOUBLE))
                .filter(a -> a.f1 < a.f2).map(a -> new Tuple2<Long,Double>(a.f0, a.f1))
                .returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .partitionCustom(new CustomPartitioner(), 0);

            Vert = Vert.union(updatedVert).groupBy(0).min(1);
            DataSet<Tuple2<Long,Double>> message2 = Edge.join(updatedVert)
                .where(0).equalTo(0).with(
                    (edge,vertex) -> new Tuple2<Long,Double>(edge.f1, vertex.f1) 
                ).returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .partitionCustom(new CustomPartitioner(), 0);

            System.out.println(
                "message1 count:" + Long.toString(message1.count()) +
                "   Agg message1 count:" + Long.toString(aggMessage1.count()) + 
                "   updated Vert:" + Long.toString(updatedVert.count())
                );

            // state 2 
            System.out.println("message2 count:" + Long.toString(message2.count()));
            DataSet<Tuple2<Long,Double>> aggMessage2 = message2.groupBy(0).min(1)
                .partitionCustom(new CustomPartitioner(), 0);
            DataSet<Tuple2<Long,Double>> message3 = Edge.join(aggMessage2).where(0).equalTo(0).with(
                (edge, message) -> new Tuple2<Long,Double>(edge.f1, message.f1)
                ).returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .partitionCustom(new CustomPartitioner(), 0);
            DataSet<Tuple2<Long,Double>> aggMessage3 = message3.groupBy(0).min(1)
                .partitionCustom(new CustomPartitioner(), 0);
            DataSet<Tuple2<Long,Double>> updatedEdge = aggMessage3.join(hyperEdge)
                .where(0).equalTo(0).with(
                    (message, edge) -> new Tuple3<Long,Double,Double>(message.f0, message.f1, edge.f1)
                ).returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .filter(a -> a.f1 < a.f2).map(a -> new Tuple2<Long,Double>(a.f0, a.f1))
                .returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .partitionCustom(new CustomPartitioner(), 0);

            System.out.println(
                "message2 count:" + Long.toString(message2.count()) +
                "   Agg message1 count:" + Long.toString(aggMessage2.count()) + 
                "   updated Edge:" + Long.toString(updatedEdge.count())
                );

            // state3 
            System.out.println("message3 count:" + Long.toString(message3.count()));
            message1 = Edge.join(updatedEdge).where(0).equalTo(0).with(
                    (edge, vert) -> new Tuple2<Long,Double>(edge.f1, vert.f1 + 1.0d)
                )
                .returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .partitionCustom(new CustomPartitioner(), 0);
            
        }
        Vert.writeAsText(resultFile+"/Init",WriteMode.OVERWRITE);
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
            int partId = (key.intValue() / 2)% numPartitions;
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


