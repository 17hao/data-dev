package xyz.shiqihao.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * 相同的key对应的value会被划分到同一个reducer中
 *
 * reduce的输入只能保证key是有序的，但无法保证同一个key对应的多个value的顺序
 * 要使value按照一定的顺序排序，可以用secondary sort
 *
 * secondary sort可以使value按照一定的顺序排序，将原始的key和value组成一对新的key，
 * 按组合键的key进行排序使得value也被排序
 *
 * sort comparator和grouping comparator的区别：
 * 1. sort comparator决定了key在进入Reducer之前的排序方式
 * 2. grouping comparator决定了哪一批次的key会出现在同一次reduce方法调用中
 *
 * shuffle 过程中的 partition 和 group
 * - 最简单的模型：输出Text键值对，相同key被partition到一块，并且group到一块，最终进入同一次reduce中，
 * 这些key对应的value在同一个reduce方法的values中
 *
 * 如何决定Reducer的数量？
 *
 * Mapper如何将输入进行分片？
 * 按照不同的输入格式，InputFormat.getSplits()方法决定了按何种大小划分输入格式的字节块，
 * 得到分片后由每种具体的InputFormat.createRecordReader()，e.g. TextInputFormat，决定如何将InputSplit转换成Mapper的key-value。
 * TextInputFormat.createRecordReader()将分片按文件中的行转化成Mapper的输入，key是每行首字母的字节序号，value是每一行内容。
 * ApplicationMaster负责将记录传输给Mapper
 */
public class JoinRecordsWithStationName extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        int exitCode = ToolRunner.run(new JoinRecordsWithStationName(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: ");
            return -1;
        }
        Job job = Job.getInstance(getConf(), "Join weather records with stations");

        job.setJarByClass(getClass());

        Path recordsInput = new Path(args[0]);
        Path stationInput = new Path(args[1]);
        Path output = new Path(args[2]);

        MultipleInputs.addInputPath(job, recordsInput, TextInputFormat.class, JoinRecordMapper.class);
        MultipleInputs.addInputPath(job, stationInput, TextInputFormat.class, JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class KeyPartitioner extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}


class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        String stationId = strings[0];
        String stationName = strings[1];
        context.write(new TextPair(stationId, "0"), new Text(stationName));

    }
}

class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        String stationId = strings[0];
        context.write(new TextPair(stationId, "1"), value);
    }
}

class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        /*
        处于性能考虑，reducer并不会把所有values中的元素加载到内存中，
        每次只有一个值从硬盘上加载到内存中，并且会重用Iterable<VALUEIN>中的对象。
        所以如果不使用new Text(iterator.next())拷贝第一次加载的stationName，
        待下一次加载record时，iterator中的对象会被赋值为record，stationName会被覆盖
         */
        Text stationName = new Text(iterator.next());
        while (iterator.hasNext()) {
            Text record = iterator.next();
            Text outValue = new Text(stationName.toString() + "\t" + record);
            context.write(key.getFirst(), outValue);
        }
    }
}

class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public String toString() {
        return "TextPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    public static class FirstComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }
    }
}
