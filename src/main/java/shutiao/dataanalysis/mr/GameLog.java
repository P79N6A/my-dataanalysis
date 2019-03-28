package shutiao.dataanalysis.mr;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class GameLog{
    private static final Set<String> COMMANDS = new HashSet<>();

    static {
        COMMANDS.add("游戏结束");
    }

    private static String logDate = null;

    public static class TokenizerMapper extends MapperBase {
        Record key;
        Record value;

        @Override
        public void setup(TaskContext context) throws IOException {
            key = context.createMapOutputKeyRecord();
            value = context.createMapOutputValueRecord();
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context)
                throws IOException {
            String command = record.getString("command");
            if (!COMMANDS.contains(command)) {
                return;
            }

            Long userId = record.getBigint("user_id");
            String device = record.getString("device");
            String source = record.getString("source");
            Long platform = record.getBigint("platform;


            key.set("user_id", userId);
            key.set("device", device);
            key.set("source", source);
            key.set("platform", platform);
            value.set("login_times", 1L);
            context.write(key, value);
        }
    }

    public static class SumCombiner extends ReducerBase {
        private Record result;

        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createMapOutputValueRecord();
        }

        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context)
                throws IOException {
            Long loginTimes = 0L;
            while (values.hasNext()) {
                Record val = values.next();
                loginTimes += val.getBigint("login_times");
            }
            result.set("login_times", loginTimes);
            context.write(key, result);
        }
    }

    public static class SumReducer extends ReducerBase {
        private Record result;

        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }

        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context)
                throws IOException {
            Long userId = key.getBigint("user_id");
            String device = key.getString("device");
            String source = key.getString("source");
            Long platform = key.getBigint("platform");
            Long loginTimes = 0L;
            Long tmpL;
            while (values.hasNext()) {
                Record val = values.next();
                tmpL = val.getBigint("login_times");
                loginTimes += tmpL == null ? 0 : tmpL;
            }
            if (logDate == null) {
                logDate = BaseLog.resolveLogDate(OutputUtils.getTables(context.getJobConf())[0].getPartSpec().toString());
            }
            result.set("user_id", userId);
            result.set("log_date", logDate);
            result.set("device", device);
            result.set("source", source);
            result.set("platform", platform);
            result.set("login_times", loginTimes);
            context.write(result);
        }
    }

    public static void main(String[] args) throws OdpsException {
        if (args.length < 2) {
            System.exit(1);
        }
        String[] tmp = new String[BaseLog.ARGS_LENGTH];
        System.arraycopy(args, 0, tmp, 0, args.length);
        int index = args.length;
        tmp[index++] = "log_date:string,user_id:bigint,device:string,source:string,platform:bigint";
        tmp[index++] = "login_times:bigint";
        args = tmp;

        logDate = BaseLog.resolveLogDate(args[1]);
        BaseLog log = new BaseLog(args, new LoginLog());
        log.run();
    }

}

