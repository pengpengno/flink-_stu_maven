import cn.hutool.core.collection.CollectionUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
public class MapOperator {

    public static void main(String[] args) throws Exception {
//        mapOperator();
//        flatMapOperator();
        filterOperator();
    }
    public static void mapOperator() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        ArrayList<Integer> integers = CollectionUtil.newArrayList(1, 2, 4);
        DataStreamSource<Integer> dataStreamSource = environment.fromCollection(integers);
        dataStreamSource.map(e->e+8);
        dataStreamSource.print("Map");
        environment.execute("Map Job");
    }

    public static void flatMapOperator() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        ArrayList<String> integers = CollectionUtil.newArrayList("yyy sdausd dasiod ");
        DataStreamSource<String> dataStreamSource = environment.fromCollection(integers);
        dataStreamSource.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String string, Collector<Object> collector) throws Exception {
                String[] s = string.split(" ");
                for (String sg :s){
                    collector.collect(sg+"\n");
                }
            }
        });

        dataStreamSource.print("flatMap");
        environment.execute("flatMap Job");
    }

    public static void filterOperator() throws Exception {


        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //        environment.setParallelism(1);
//        ArrayList<String> integers = CollectionUtil.newArrayList("yyy sdausd dasiod ","rwerrrr","lllll");
        ArrayList<Integer> integers = CollectionUtil.newArrayList(1,2,4,5,6,6,-2);
//        DataStream<Integer> source =

        DataStreamSource<Integer> dataStreamSource = environment.fromCollection(integers);
//        DataSet<Integer> dataStreamSource = environment.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Integer> filter = dataStreamSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer string) throws Exception {
                return string > 3;
//                if (strin g.contains("y")){
//                    return false;
//                }return true;
            }
        });
//        dataStreamSource.printOnTaskManager("filterMap");
        filter.print("filterMap");
        environment.execute("filter Job");
    }
}
