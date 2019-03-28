package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class GoldLogMR {


    static HashSet commands = new HashSet();

    static {
        commands.add("晋级赛报名费");
        commands.add("晋级赛报名费退还");
        commands.add("晋级赛奖励");
    }

    /*
    * 发奖
    * */
    private static int FIRST_LOGIN_GOLD = -100;
    private static int SIGN_IN_GOLD = -101;
    private static int ACTIVE_GOLD = -102;
    private static int WIN_ROBOT = -103;
    private static int LADDER_GOLD_GIFT_GOLD = -104;
    private static int LADDER_GOLD_PRIZE_GOLD = -105;
    /*
    * 消耗
    * */
    private static int TICKETS = -106;
    private static int LADDER_GOLD_ENTRY_FEE = -107;
    private static int FAIL_ROBOT = -108;

    private static int MEANLESS = -10;

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
            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0L);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            long gold_change = (long) CommonUtil.getValueIfNull(__record.get("gold_change"), 0L);
            long gold_get = 0L;
            String custom_field2 = (String) CommonUtil.getValueIfNull(__record.get("custom_field2"), null);

            long type = MEANLESS;
            double rmb_change = __record.getDouble("rmb_change_double");
            double rmb_get = 0;

            custom_field2 = custom_field2 == null ? "" : custom_field2;
            if (custom_field2 == null || custom_field2.isEmpty()){
                custom_field2 = __record.getString("custom_field1");
                if (custom_field2 == null || custom_field2.isEmpty()){
                    custom_field2 = "-10";
//                    return ;
                }
            }
            if (("晋级赛报名费".equals(command) || "晋级赛报名费退还".equals(command))) {
                type = Integer.parseInt(custom_field2.trim());
            }
            rmb_get = rmb_change;
            if("晋级赛奖励".equals(command)) {
                type = Integer.parseInt(custom_field2.trim());
                gold_get = gold_change;
                gold_change = 0;
                rmb_get = rmb_change;
            }

            __keyOut.set("game_id", game_id);
            __keyOut.set("user_id", user_id);
            __keyOut.set("platform", platform);
            __keyOut.set("source", source);
            __keyOut.set("source_type", source_type);
            __keyOut.set("type", type);

            __valueOut.set("gold_change", gold_change);
            __valueOut.set("gold_get", gold_get);
            __valueOut.set("rmb_get", rmb_get);

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

            long gold_change = 0;
            long gold_get = 0;
            double rmb_get = 0;
            while (__values.hasNext()) {
                Record __v = __values.next();
                gold_change += __v.getBigint("gold_change");
                gold_get += __v.getBigint("gold_get");
                rmb_get += __v.getDouble("rmb_get");
            }
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("gold_get", gold_get);
            __valueOut.set("rmb_get", rmb_get);
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

            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0L);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            long type = __record.getBigint("type");

            String log_date = null;
            log_date = CommonUtil.getDateStr("yyyy-MM-dd");
            long gold_change = 0;
            long gold_get = 0;
            double rmb_get = 0;
            while (__values.hasNext()) {
                Record __v = __values.next();
                gold_change += __v.getBigint("gold_change");
                gold_get += __v.getBigint("gold_get");
                rmb_get += __v.getDouble("rmb_get");

//                __valueOut.set("rmb_get", rmb_get);
            }
            __valueOut.set("gold_change", gold_change);
            __valueOut.set("gold_get", gold_get);
            __valueOut.set("game_id", game_id);
            __valueOut.set("user_id", user_id);
            __valueOut.set("platform", platform);
            __valueOut.set("source", source);
            __valueOut.set("source_type", source_type);
            __valueOut.set("type", type);
            __valueOut.set("log_date", log_date);
            __valueOut.set("rmb_get", rmb_get);
            context.write(__valueOut);
        }

    }

}

