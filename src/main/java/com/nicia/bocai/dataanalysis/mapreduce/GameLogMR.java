package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

public class GameLogMR {


    static HashSet commands = new HashSet();

    static {
        commands.add("开始游戏");//customfield1 分享类型还是晋级赛类型还是匹配  customfield2:如果是晋级赛 就记录1元10元100元场
//        commands.add("竞技场开始游戏");
        commands.add("晋级赛奖励");
        commands.add("天梯赛礼包奖励");
        commands.add("天梯赛奖池奖励");
        commands.add("游戏结算");
        commands.add("游戏结算抽奖");
        commands.add("游戏结果记录");
    }

    /*
     * 发奖
     * */
    private static int LADDER_GOLD_GIFT_GOLD = -104;
    private static int LADDER_GOLD_PRIZE_GOLD = -105;
    private static int PRIMARY_FIELD = -110;
    private static int INTERMEDIATE_FIELD = -111;
    private static int FRESHMAN_FIELD = -112;
    private static int STAND_ALONE = -120;

    private static int MATCH = -10;
    private static int MEANLESS = 0;

    public static class TokenizerMapper extends MapperBase {


        private Record __keyOut;
        private Record __valueOut;
        private SimpleDateFormat format;
        private String __log_date;

        @Override
        public void setup(TaskContext context) throws IOException {
            __keyOut = context.createMapOutputKeyRecord();
            __valueOut = context.createMapOutputValueRecord();
            format = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public void map(long recordNum, Record __record, TaskContext context) throws IOException {


            String command = (String) CommonUtil.getValueIfNull(__record.get("command"), null);
            if (!(commands.contains(command))) {
                return;
            }

            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0L);
//            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            String custom_field3 = __record.getString("custom_field3");
            long game_id = __record.getBigint("game_id");
//            if (custom_field3 != null && !"null".equals(custom_field3) && !custom_field3.isEmpty()){
//                game_id = Long.parseLong(custom_field3);
//            }
            String custom_field1 = (String) CommonUtil.getValueIfNull(__record.get("custom_field1"), null);
            String custom_field2 = (String) CommonUtil.getValueIfNull(__record.get("custom_field2"), null);
            long promotion_type = MEANLESS;
            long game_times = 0L;
            double rmb_change_double;
            long action_time = __record.getBigint("action_time");
            long win_times = 0;
            long gold_consume = 0;

            if (__log_date == null){
                __log_date = format.format(new Date(action_time * 1000));
            }
            String log_date = __log_date;
            try {
                rmb_change_double = (double) CommonUtil.getValueIfNull(__record.get("rmb_change_double"), 0D);
            } catch (IllegalArgumentException e){
                rmb_change_double = (double) CommonUtil.getValueIfNull(__record.get("rmb_change"), 0D);
            }
            long gold_change = (long) CommonUtil.getValueIfNull(__record.get("gold_change"), 0L);
            double rmb_get = 0D;
            long gold_get = 0L;

            if ("开始游戏".equals(command) && !"1".equals(custom_field1)){
                 game_times = 1;
            }

            rmb_get = rmb_change_double;
            gold_get = gold_change;
            gold_consume = gold_change < 0 ? gold_change : 0;
            win_times = 0;


            if ("游戏结果记录".equals(command)){
                if (!"1".equals(custom_field1) && custom_field3.trim().equals("1")) {
                    win_times = 1;
                }
            }


            if (Arrays.asList("晋级赛奖励", "开始游戏", "游戏结果记录", "游戏结算", "游戏结算抽奖").contains(command)){
                if ("3".equals(custom_field1)) {
                    promotion_type = custom_field2 == null || custom_field2.trim().isEmpty() || "null".equals(custom_field2.trim()) ? MEANLESS : Long.parseLong(custom_field2);
                }

                if ("5".equals(custom_field1)){
                    promotion_type = LADDER_GOLD_GIFT_GOLD;
                }

                if ("2".equals(custom_field1)){
                    int tmp = 0;
                    if (custom_field2 != null && !custom_field2.trim().isEmpty() && !"null".equals(custom_field2)){
                        tmp = Integer.parseInt(custom_field2);
                    }
                    if (tmp > 0) {
                        switch ((tmp - 1) % 3 + 1) {
                            case 1:
                                promotion_type = FRESHMAN_FIELD;
                                break;
                            case 2:
                                promotion_type = PRIMARY_FIELD;
                                break;
                            case 3:
                                promotion_type = INTERMEDIATE_FIELD;
                                break;
                            default:
                                break;
                        }
                    }
                    else {
                        promotion_type = MATCH;
                    }
                }

                if ("6".equals(custom_field1)){
                    promotion_type = STAND_ALONE;
                }
            }

            if ("天梯赛礼包奖励".equals(command)){
                promotion_type = LADDER_GOLD_GIFT_GOLD;
                win_times = 0;
            }

            if ( "天梯赛奖池奖励".equals(command)) {
                promotion_type = LADDER_GOLD_PRIZE_GOLD;
                win_times = 0;
            }

            __keyOut.set("platform", platform);
            __keyOut.set("source", source);
            __keyOut.set("source_type", source_type);
            __keyOut.set("user_id", user_id);
            __keyOut.set("game_id", game_id);
            __keyOut.set("promotion_type", promotion_type);
            __keyOut.set("log_date", log_date);

            __valueOut.set("game_times", game_times);
            __valueOut.set("rmb_get", rmb_get);
            __valueOut.set("gold_get", gold_get);
            __valueOut.set("win_times", win_times);
            __valueOut.set("gold_consume", gold_consume);

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


            long game_times = 0L;
            double rmb_get = 0D;
            long gold_get = 0L;
            long win_times = 0L;
            long gold_consume = 0L;

            while (__values.hasNext()) {
                Record __v = __values.next();
                game_times += (long) __v.getBigint("game_times");
                rmb_get += (double) __v.getDouble("rmb_get");
                gold_get += (long) __v.getBigint("gold_get");
                win_times += __v.getBigint("win_times");
                gold_consume += __v.getBigint("gold_consume");
            }


            __valueOut.set("game_times", game_times);
            __valueOut.set("rmb_get", rmb_get);
            __valueOut.set("gold_get", gold_get);
            __valueOut.set("win_times", win_times);
            __valueOut.set("gold_consume", gold_consume);

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

            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0L);
            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            long promotion_type = (long) CommonUtil.getValueIfNull(__record.get("promotion_type"), 0L);
            long game_times = 0L;
            double rmb_get = 0D;
            long gold_get = 0L;
            String log_date = null;
//            log_date = "2018-08-14";//
            log_date = __record.getString("log_date");
            long win_times = 0;
            long gold_consume = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                game_times += (long) __v.getBigint("game_times");
                rmb_get += (double) __v.getDouble("rmb_get");
                gold_get += (long) __v.getBigint("gold_get");
                win_times += __v.getBigint("win_times");
                gold_consume += __v.getBigint("gold_consume");
            }
            __valueOut.set("platform", platform);
            __valueOut.set("source", source);
            __valueOut.set("source_type", source_type);
            __valueOut.set("user_id", user_id);
            __valueOut.set("game_id", game_id);
            __valueOut.set("promotion_type", promotion_type);
            __valueOut.set("game_times", game_times);
            __valueOut.set("rmb_get", rmb_get);
            __valueOut.set("gold_get", gold_get);
            __valueOut.set("log_date", log_date);
            __valueOut.set("win_times", win_times);
            __valueOut.set("gold_consume", gold_consume);

            context.write(__valueOut);
        }

    }

}

