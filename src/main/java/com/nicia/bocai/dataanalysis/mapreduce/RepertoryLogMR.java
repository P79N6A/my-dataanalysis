package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RepertoryLogMR {


    private static final Set<String> excludeCommand = new HashSet<>();
    static {
        excludeCommand.add("红包不足");
        excludeCommand.add("泡豆不足");
    }

    public static class TokenizerMapper extends MapperBase {


        private Record __keyOut;
        private Record __valueOut;

        @Override
        public void setup(TaskContext context) throws IOException {
            __keyOut = context.createMapOutputKeyRecord();
            __valueOut = context.createMapOutputValueRecord();
        }

        @Override
        public void map(long recordNum, Record __record, TaskContext context) throws IOException {


            String command = __record.getString("command");

            if (excludeCommand.contains(command)){ return; }
            long gold_change = (long) CommonUtil.getValueIfNull(__record.get("gold_change"), 0L);
            double rmb_change_double = (double) CommonUtil.getValueIfNull(__record.get("rmb_change_double"), 0D);
            String log_date = null;
            log_date = CommonUtil.getDateStr("yyyy-MM-dd");
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("rmb_change_double", rmb_change_double);
            __keyOut.set("log_date", log_date);

            context.write(__keyOut, __valueOut);
        }
    }

    /**
     * A combiner class that combines map output by sum them.
     **/
    public static class SumCombiner extends ReducerBase {

        private Record __valueOut;
        private Record __keyOut;

        @Override
        public void setup(TaskContext context) throws IOException {
            __valueOut = context.createMapOutputValueRecord();
            __keyOut = context.createMapOutputKeyRecord();
        }

        @Override
        public void reduce(Record __record, Iterator<Record> __values, TaskContext context) throws IOException {

            long gold_change = 0L;
            double rmb_change_double = 0D;


            while (__values.hasNext()) {
                Record __v = __values.next();
                gold_change += (long) __v.getBigint("gold_change");
                rmb_change_double += (double) __v.getDouble("rmb_change_double");
            }
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("rmb_change_double", rmb_change_double);


            context.write(__record, __valueOut);
        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     **/
    public static class SumReducer extends ReducerBase {

        private Record __valueOut = null;

        @Override
        public void setup(TaskContext context) throws IOException {
            __valueOut = context.createOutputRecord();
        }

        @Override
        public void reduce(Record __record, Iterator<Record> __values, TaskContext context) throws IOException {

            long gold_change = 0L;
            double rmb_change_double = 0D;
            String log_date = (String) CommonUtil.getValueIfNull(__record.get("log_date"), null);

            while (__values.hasNext()) {
                Record __v = __values.next();
                gold_change += (long) __v.getBigint("gold_change");
                rmb_change_double += (double) __v.getDouble("rmb_change_double");
            }
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("rmb_change_double", rmb_change_double);
            __valueOut.set("log_date", log_date);

            context.write(__valueOut);
        }

    }

}

