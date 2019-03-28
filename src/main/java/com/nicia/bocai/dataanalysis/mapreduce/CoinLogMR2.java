package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class CoinLogMR2 {


    static HashSet commands = new HashSet();

    static {
        commands.add("米大师订单");
        commands.add("微信公众号订单");
        commands.add("提现申请");

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


            String command = (String) CommonUtil.getValueIfNull(__record.get("command"), null);
            if (!(commands.contains(command))) {
                return;
            }

            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            Object user_id = (Object) CommonUtil.getValueIfNull(__record.get("user_id"), null);
            double rmb_change_double = Double.valueOf(CommonUtil.getValueIfNull(__record.get("rmb_change"), 0D).toString());
            long game_id = __record.getBigint("game_id");
            double cash_in_double = 0;
            double cash_out_double = 0;
            long gold_change = (long) CommonUtil.getValueIfNull(__record.get("gold_change"), 0L);
            long pay_times = 0;
            long total_order_time = (long) CommonUtil.getValueIfNull(__record.get("total_order_time"), 0L);
            long pay_first = 0;
            long action_time = (long) CommonUtil.getValueIfNull(__record.get("action_time"), 0L);
            String tmp_custom_field1 = __record.getString("custom_field1");
            double custom_field1 = tmp_custom_field1 == null || tmp_custom_field1.isEmpty()
                    ? 0 : Double.parseDouble(tmp_custom_field1);
            cash_in_double = Math.max(0d, custom_field1);
            cash_out_double = Math.min(0d, rmb_change_double);
            if (command.equals("米大师订单") || command.equals("微信公众号订单")) {
                pay_times = 1;
                pay_first = total_order_time == 1 ? 1 : 0;
            }

            __keyOut.set("platform", platform);
            __keyOut.set("user_id", user_id);
            __keyOut.set("game_id", game_id);
            __valueOut.set("rmb_change_double", rmb_change_double);
            __valueOut.set("cash_in_double", cash_in_double);
            __valueOut.set("cash_out_double", cash_out_double);
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("pay_times", pay_times);
            __valueOut.set("pay_first", pay_first);
            __valueOut.set("action_time", action_time);

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


            double rmb_change_double = 0;
            double cash_in_double = 0;
            double cash_out_double = 0;
            long gold_change = 0;
            long pay_times = 0;
            long pay_first = 0;
            long action_time = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                rmb_change_double += (double) __v.getBigint("rmb_change_double");
                cash_in_double += (double) __v.getBigint("cash_in_double");
                cash_out_double += (double) __v.getBigint("cash_out_double");
                gold_change += (long) __v.getBigint("gold_change");
                pay_times += (long) __v.getBigint("pay_times");
                pay_first = Math.max(__v.getBigint("pay_first"), pay_first);
            }


            __valueOut.set("rmb_change_double", rmb_change_double);
            __valueOut.set("cash_in_double", cash_in_double);
            __valueOut.set("cash_out_double", cash_out_double);
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("pay_times", pay_times);
            __valueOut.set("pay_first", pay_first);
            __valueOut.set("action_time", action_time);

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

            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0);
            Object user_id = (Object) CommonUtil.getValueIfNull(__record.get("user_id"), null);
            String log_date = null;
            long game_id = __record.getBigint("game_id");
            double rmb_change_double = 0;
            double cash_in_double = 0;
            double cash_out_double = 0;
            long gold_change = 0;
            long pay_times = 0;
            long pay_first = 0;
            long action_time = 0;
            log_date = CommonUtil.getDateStr("yyyy-MM-dd");
            action_time = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                rmb_change_double += (double) __v.getBigint("rmb_change_double");
                cash_in_double += (double) __v.getBigint("cash_in_double");
                cash_out_double += (double) __v.getBigint("cash_out_double");
                gold_change += (long) __v.getBigint("gold_change");
                pay_times += (long) __v.getBigint("pay_times");
                pay_first = Math.max(__v.getBigint("pay_first"), pay_first);
            }
            __valueOut.set("platform", platform);
            __valueOut.set("user_id", user_id);
            __valueOut.set("game_id", game_id);
            __valueOut.set("log_date", log_date);
            __valueOut.set("rmb_change_double", rmb_change_double);
            __valueOut.set("cash_in_double", cash_in_double);
            __valueOut.set("cash_out_double", cash_out_double);
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("pay_times", pay_times);
            __valueOut.set("pay_first", pay_first);
            __valueOut.set("action_time", action_time);

            context.write(__valueOut);
        }

    }

}

