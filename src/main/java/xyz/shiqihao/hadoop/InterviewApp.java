package xyz.shiqihao.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.time.*;
import java.util.Iterator;

/**
 * 数据端笔试题
 * <p>
 * 现有如下数据文件需要处理
 * 格式：CSV
 * 位置：hdfs://myhdfs/input.csv
 * 大小：100GB
 * 字段：用户ID，位置ID，开始时间，停留时长(分钟）
 * <p>
 * 4行样例：
 * <p>
 * UserA,LocationA,2018-01-01 08:00:00,60
 * UserA,LocationA,2018-01-01 09:00:00,60
 * UserA,LocationB,2018-01-01 10:00:00,60
 * UserA,LocationA,2018-01-01 11:00:00,60
 * <p>
 * 解读：
 * <p>
 * 样例数据中的数据含义是：
 * 用户UserA，在LocationA位置，从8点开始，停留了60分钟
 * 用户UserA，在LocationA位置，从9点开始，停留了60分钟
 * 用户UserA，在LocationB位置，从10点开始，停留了60分钟
 * 用户UserA，在LocationA位置，从11点开始，停留了60分钟
 * <p>
 * 该样例期待输出：
 * UserA,LocationA,2018-01-01 08:00:00,120
 * UserA,LocationB,2018-01-01 10:00:00,60
 * UserA,LocationA,2018-01-01 11:00:00,60
 * <p>
 * 处理逻辑：
 * 1 对同一个用户，在同一个位置，连续的多条记录进行合并
 * 2 合并原则：开始时间取最早时间，停留时长加和
 * <p>
 * 要求：请使用Spark、MapReduce或其他分布式计算引擎处理
 */
public class InterviewApp extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new InterviewApp(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(EventMapper.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);
        job.setReducerClass(EventReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TextPair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class EventMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split(",");
        if (records.length < 4) {
            return;
        }
        TextPair outputKey = new TextPair(records[0] + "," + records[1], records[2]);
        String[] datetime = records[2].split("\\s+");
        long timestamp = LocalDate.parse(datetime[0]).atTime(LocalTime.parse(datetime[1])).toEpochSecond(ZoneOffset.ofHours(8));
        TextPair outputValue = new TextPair(String.valueOf(timestamp), records[3]);
        context.write(outputKey, outputValue);
    }
}

class EventReducer extends Reducer<TextPair, TextPair, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
        Iterator<TextPair> iterator = values.iterator();
        TextPair headTP = new TextPair(iterator.next());
        long startTime = Long.parseLong(headTP.getFirst().toString());
        long currentTime = startTime;
        int stay = Integer.parseInt(headTP.getSecond().toString()) * 60;
        int totalStay = stay;
        while (iterator.hasNext()) {
            TextPair tp = iterator.next();
            long nextStartTime = Long.parseLong(tp.getFirst().toString());
            int nextStay = Integer.parseInt(headTP.getSecond().toString()) * 60;
            if (currentTime + stay < nextStartTime) {
                LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(startTime), ZoneOffset.ofHours(8));
                context.write(new Text(key.getFirst().toString() + "," + time.toString()), new Text(String.valueOf(totalStay / 60)));
                startTime = nextStartTime;
                currentTime = nextStartTime;
                totalStay = nextStay;
            } else {
                currentTime = nextStartTime;
                totalStay += stay;
                stay = nextStay;
            }
        }
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(startTime), ZoneOffset.ofHours(8));
        context.write(new Text(key.getFirst().toString() + "," + time.toString()), new Text(String.valueOf(totalStay / 60)));
    }
}

class KeyPartitioner extends Partitioner<TextPair, TextPair> {
    @Override
    public int getPartition(TextPair key, TextPair value, int numPartitions) {
        return (key.getFirst().hashCode() ^ Integer.MAX_VALUE) % numPartitions;
    }
}