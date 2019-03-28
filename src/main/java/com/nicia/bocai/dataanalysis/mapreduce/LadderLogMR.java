package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class LadderLogMR {


    static HashSet commands = new HashSet();

    static {
        commands.add("开始游戏");
        commands.add("天梯赛报名费");
        commands.add("匹配门票");
        commands.add("天梯赛礼包奖励");
        commands.add("天梯赛奖池奖励");

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
            String custom_field1 = (String) CommonUtil.getValueIfNull(__record.get("custom_field1"), null);
            String custom_field2 = (String) CommonUtil.getValueIfNull(__record.get("custom_field2"), null);
            long type = MEANLESS;
            long game_times = 0;
            long sign_up = 0;

            long gift = 0;
            long prize = 0;

            double rmb_change = __record.getDouble("rmb_change_double");
            double ladder_rmb = 0;

            if (CommonUtil.isInteger(custom_field2)){
                type = Long.parseLong(custom_field2);
            }

            if ("开始游戏".equals(command)){
                if ("5".equals(custom_field1)){
                    game_times = 1;
                }
                else {
                    return;
                }
            }

            if ("天梯赛报名费".equals(command)){
                sign_up = gold_change;
            }

            if( "匹配门票".equals(command) ){
                if ( "5".equals(custom_field1)){
                    sign_up = gold_change;
                }
                else { return ; }
            }

            if ("天梯赛礼包奖励".equals(command)){
                gift = gold_change;
                ladder_rmb = rmb_change;
            }

            if ("天梯赛奖池奖励".equals(command)){
                prize = gold_change;
                ladder_rmb = rmb_change;
            }


            __keyOut.set("game_id", game_id);
            __keyOut.set("user_id", user_id);
            __keyOut.set("platform", platform);
            __keyOut.set("source", source);
            __keyOut.set("source_type", source_type);
            __keyOut.set("type", type);

            __valueOut.set("sign_up", sign_up);
            __valueOut.set("gift", gift);
            __valueOut.set("prize", prize);
            __valueOut.set("ladder_rmb", ladder_rmb);
            __valueOut.set("game_times", game_times);


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

            long sign_up = 0;
            long prize = 0;
            long gift = 0;
            double ladder_rmb = 0;
            long game_times = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                sign_up += __v.getBigint("sign_up");
                prize += __v.getBigint("prize");
                gift += __v.getBigint("gift");
                ladder_rmb += __v.getDouble("ladder_rmb");
                game_times += __v.getBigint("game_times");
            }
            __valueOut.set("sign_up", sign_up);
            __valueOut.set("prize", prize);
            __valueOut.set("gift", gift);
            __valueOut.set("ladder_rmb", ladder_rmb);
            __valueOut.set("game_times", game_times);
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
            long sign_up = 0;
            long prize = 0;
            long gift = 0;
            double ladder_rmb = 0;
            long game_times = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                sign_up += __v.getBigint("sign_up");
                prize += __v.getBigint("prize");
                gift += __v.getBigint("gift");
                ladder_rmb += __v.getDouble("ladder_rmb");
                game_times += __v.getBigint("game_times");
            }
            __valueOut.set("sign_up", sign_up);
            __valueOut.set("prize", prize);
            __valueOut.set("gift", gift);
            __valueOut.set("game_id", game_id);
            __valueOut.set("user_id", user_id);
            __valueOut.set("platform", platform);
            __valueOut.set("source", source);
            __valueOut.set("source_type", source_type);
            __valueOut.set("type", type);
            __valueOut.set("log_date", log_date);
            __valueOut.set("ladder_rmb", ladder_rmb);
            __valueOut.set("game_times", game_times);

            context.write(__valueOut);
        }

    }

}

