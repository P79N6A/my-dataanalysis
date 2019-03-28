package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class LoginLogMR {


    static HashSet commands = new HashSet();

    static {
        commands.add("用户登录");
        commands.add("用户登出");
    }

    private static final String MEANLESS_STR = "meanless";
    private static final long MEANLESS = -10;

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
            long user_id = __record.getBigint("user_id");
            long user_type = (long) CommonUtil.getValueIfNull(__record.get("user_type"), -1);
            long action_time = __record.getBigint("action_time");
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0);
            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0);
            String custom_field1 = (String) CommonUtil.getValueIfNull(__record.get("custom_field1"), MEANLESS_STR);
            String custom_field2 = (String) CommonUtil.getValueIfNull(__record.get("custom_field2"), MEANLESS_STR);
            String custom_field3 = (String) CommonUtil.getValueIfNull(__record.get("custom_field3"), MEANLESS_STR);
            String fromGameType = ((custom_field1 == null || "".equals(custom_field1) || "null".equals(custom_field1))
                    ? MEANLESS_STR : custom_field1);
            long accountType = (custom_field2 == null || "".equals(custom_field2) || "null".equals(custom_field2))
                    ? MEANLESS : Long.parseLong(custom_field2);
            long fromGameId = ((custom_field3 == null || "".equals(custom_field3) || "null".equals(custom_field3))
                    ? MEANLESS : Long.parseLong(custom_field3));
            long login_time = 0;
            long logout_time = 0;
            long login_count = 0;

            if ("用户登录".equals(command)) {
                login_time = action_time;
                login_count++;
            } else{
                logout_time = action_time;
            }

            __keyOut.set("user_id", user_id);
            __valueOut.set("user_type", user_type);
            __keyOut.set("source", source);
            __keyOut.set("source_type", source_type);
            __keyOut.set("game_id", game_id);
            __keyOut.set("platform", platform);
            __keyOut.set("from_game_id", fromGameId);
            __keyOut.set("accout_type", accountType);
            __keyOut.set("from_game_type", fromGameType);

            __valueOut.set("login_time", login_time);
            __valueOut.set("logout_time", logout_time);
            __valueOut.set("login_count", login_count);


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

            long login_time = 0;
            long logout_time = 0;
            long login_count = 0;
            long user_type = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                user_type = __v.getBigint("user_type");
                login_time = login_time == 0 ? __v.getBigint("login_time") : Math.min(login_time, __v.getBigint("login_time"));
                logout_time = logout_time == 0 ? __v.getBigint("logout_time") : Math.max(logout_time, __v.getBigint("logout_time"));
                login_count += (long) __v.get("login_count");
            }
            __valueOut.set("user_type", user_type);
            __valueOut.set("login_time", login_time);
            __valueOut.set("logout_time", logout_time);
            __valueOut.set("login_count", login_count);

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

            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0);;
            String log_date = CommonUtil.getDateStr("yyyy-MM-dd");
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0);
            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0);
            long fromGameId = (long) CommonUtil.getValueIfNull(__record.get("from_game_id"), MEANLESS);
            long accountType = (long) CommonUtil.getValueIfNull(__record.get("accout_type"), 0);
            String fromGameType = (String) CommonUtil.getValueIfNull(__record.get("from_game_type"), MEANLESS_STR);
            long login_time = 0;
            long logout_time = 0;
            long login_count = 0;
            long user_type = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                user_type = __v.getBigint("user_type");
                login_time = login_time == 0 ? __v.getBigint("login_time") : Math.min(login_time, __v.getBigint("login_time"));
                logout_time = logout_time == 0 ? __v.getBigint("logout_time") : Math.max(logout_time, __v.getBigint("logout_time"));
                login_count += (long) __v.get("login_count");
            }
            __valueOut.set("user_id", user_id);
            __valueOut.set("log_date", log_date);
            __valueOut.set("user_type", user_type);
            __valueOut.set("source", source);
            __valueOut.set("source_type", source_type);
            __valueOut.set("game_id", game_id);
            __valueOut.set("platform", platform);
            __valueOut.set("from_game_id", fromGameId);
            __valueOut.set("accout_type", accountType);
            __valueOut.set("from_game_type_str", fromGameType);

            __valueOut.set("login_time", login_time);
            __valueOut.set("logout_time", logout_time);
            __valueOut.set("login_count", login_count);

            context.write(__valueOut);
        }

    }

}

