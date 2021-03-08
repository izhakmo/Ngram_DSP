import java.io.IOException;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class step3_calculate_probability {



    public static class Mapper_pass_to_reducer extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] line_as_string = line.toString().split("\\s+");

            if (line_as_string.length > 3) {
                Text key = new Text(line_as_string[0] + " " + line_as_string[1] + " " +line_as_string[2]);
                Text val = new Text(line_as_string[3] + " " +line_as_string[4]);
                context.write(key,val);
            }
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {


        String on_going_three_gram = null;
        long nr = 0;
        long tr = 0;
        private long N;



        @Override
        protected void setup(Reducer<Text,Text,Text,Text>.Context context) {
            N = context.getConfiguration().getLong("counter", -1);
            System.out.println("counter: "+N);
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {



            String key_string = key.toString();
            for (Text value : values) {
                String []value_to_string = value.toString().split("\\s+");
                long to_add_nr = Long.parseLong(value_to_string[0]);
                long to_add_tr = Long.parseLong(value_to_string[1]);
                if(key_string.equals(on_going_three_gram)){
                    nr += to_add_nr;
                    tr += to_add_tr;
                }
                else if(on_going_three_gram != null){
                    write_three_gram(context);
                    on_going_three_gram = key_string;
                    nr = to_add_nr;
                    tr = to_add_tr;
                }
//                first run ==> on_going_three_gram == null
                else{
                    on_going_three_gram = key_string;
                    nr = to_add_nr;
                    tr = to_add_tr;
                }

            }

        }

        @Override
        protected void cleanup(Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            if(on_going_three_gram != null) {
                write_three_gram(context);
            }

        }

        private void write_three_gram(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            double den = (double) (N) * (double) (tr);
            double probability = (double)(nr) / den;
            Text probability_text = new Text(String.valueOf(probability));
            System.out.println(on_going_three_gram +" probability: "+ nr + " /" + tr + "=" + probability);

            context.write(new Text(on_going_three_gram), probability_text);
        }

    }


}