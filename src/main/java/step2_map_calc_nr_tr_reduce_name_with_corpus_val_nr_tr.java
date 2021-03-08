import java.io.IOException;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr {
    public static Counter counter;


    public static class Mapper_Nr_Tr extends Mapper<LongWritable, Text, Text, Text> {



        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] line_as_string = line.toString().split("\\s+");
            Text key_one ;
            Text key_two ;

            if (line_as_string.length > 3) {




                String occurrences_one = line_as_string[3];
                key_one = new Text( occurrences_one + "_1" + " xxx" );
                context.write(key_one,new Text(line_as_string[4]));

                String occurrences_two = line_as_string[4];
                key_two = new Text( occurrences_two + "_2"+ " xxx" );
                context.write(key_two,new Text(line_as_string[3]));



//                join after the summing
//                three-gram as value
                Text key_corpus_one = new Text(line_as_string[3] + "_1 zzz");
                Text key_corpus_two = new Text(line_as_string[4] + "_2 zzz");
                Text val = new Text(line_as_string[0] + " " +line_as_string[1] + " " +line_as_string[2]);
                context.write(key_corpus_one,val);
                context.write(key_corpus_two,val);



            }
        }

    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        Text value_to_write = null;


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            counter = context.getCounter(all_steps.NCounter.N_COUNTER);
            long nr = 0;
            long tr = 0;

            if (key.toString().contains("zzz")) {
                for (Text value : values) {
                    context.write(value, value_to_write);
                }
            } else {
                for (Text value : values) {
                    String val_string = value.toString();
                    long actual_value = Long.parseLong(val_string);

                        nr += 1;
                        tr += actual_value;

                }

                value_to_write = new Text(nr + " " + tr);
            }
        }

    }

    //    Partitioner
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String part = key.toString().split("\\s+")[0];
            Text newKey = new Text(part);
            return Math.abs(newKey.hashCode()) % numPartitions;
        }
    }



}