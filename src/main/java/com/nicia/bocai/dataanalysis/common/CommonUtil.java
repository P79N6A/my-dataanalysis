package com.nicia.bocai.dataanalysis.common;

import com.aliyun.odps.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public abstract class CommonUtil {

	public static String getDateStr() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
	}

	public static String getDateStr(String patten) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return new SimpleDateFormat(patten).format(cal.getTime());
	}

	public static long getMin(long value1, long value2) {
		return (value1 <= 0 || value1 > value2) && value2 > 0 ? value2 : value1;
	}

	public static long getMax(long value1, long value2) {
		return value1 < value2 ? value2 : value1;
	}

	public static double getMax(double value1, double value2) {
		return value1 < value2 ? value2 : value1;
	}

	public static void uploadData(String tableName, String prefix, String dateStr, long blockId) {
		OSSClient ossClient = null;
		InputStream content = null;
		RecordWriter writer = null;
		Record record = null;
		try {
			Instance instance = SQLTask.run(Constants.odps,
					"alter table " + tableName + " add if not exists partition (partion_field='" + dateStr + "');");
			instance.waitForSuccess();

			PartitionSpec partitionSpec = new PartitionSpec("partion_field='" + dateStr + "'");

			ossClient = new OSSClient(Constants.ossEndpoint, Constants.access_id, Constants.access_key);
			ObjectListing ossObjects = ossClient.listObjects(Constants.oss_bucket, prefix);
			if (ossObjects == null || ossObjects.getObjectSummaries() == null
					|| ossObjects.getObjectSummaries().isEmpty()) {
				return;
			} // oss客户端数据
			System.out.println(prefix + " found " + ossObjects.getObjectSummaries().size() + " files.");
			long index = 0;

			for (OSSObjectSummary object : ossObjects.getObjectSummaries()) {
				long bid = blockId * 1000 + index + 1;
				UploadSession uploadSession = Constants.tunnel.createUploadSession(Constants.hadoop_project, tableName,
						partitionSpec);// oss上传至maxcomputer

				writer = uploadSession.openRecordWriter(bid);
				OSSObject ossObject = null;
				try {
					ossObject = ossClient.getObject(Constants.oss_bucket, object.getKey());
				} catch (Exception e) {
					System.out.println("File not found: " + object.getKey());
				}
				if (ossObject == null) {
					continue;
				}
				content = ossObject.getObjectContent();
				if (content != null) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(content));
					while (true) {
						String line = reader.readLine();
						if (line == null) {
							break;
						}
						record = getRecord(line, uploadSession);
						if (record != null) {
							writer.write(record);
						}
					}
					content.close();
					index++;
					System.out.println(index + " files uploaded");
				}
				writer.close();
				uploadSession.commit(new Long[] { bid });
				System.out.println(bid + " uploadSession commited");
			}
		} catch (Throwable e) {
			System.out.println("error occurred in commit");
			if (record != null) {
				System.out.println(Arrays.asList(record.toArray()));
			}
			e.printStackTrace();
		} finally {
			if (ossClient != null) {
				ossClient.shutdown();
			}
			if (content != null) {
				try {
					content.close();
				} catch (IOException e) {
				}
			}
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private static Record getRecord(String line, UploadSession uploadSession) {
		String[] value = line.split("\t");
		TableSchema schema = uploadSession.getSchema();
		Record record = uploadSession.newRecord();
		try {
			for (int i = 0; i < schema.getColumns().size(); i++) {
				Column column = schema.getColumn(i);
				if (i >= value.length && !column.getName().startsWith("custom_field")) {
					return null;
				} else if (i >= value.length) {
					record.setString(i, "");
				} else {
					switch (column.getType()) {
						case BIGINT:
							long longData = StringUtils.isNotBlank(value[i]) ? Long.parseLong(value[i]) : 0;
							record.setBigint(i, longData);
							break;
						case BOOLEAN:
							record.setBoolean(i, Boolean.parseBoolean(value[i]));
							break;
						case DATETIME:
							record.setDatetime(i, new Date());
							break;
						case DOUBLE:
							double doubleData = StringUtils.isNotBlank(value[i]) ? Double.parseDouble(value[i]) : 0;
							record.setDouble(i, doubleData);
							break;
						case STRING:
							record.setString(i, value[i]);
							break;
						case DECIMAL:
							String decimalData = StringUtils.isNotBlank(value[i]) ? value[i] : "0";
							record.setDecimal(i, new BigDecimal(decimalData));
							break;
						default:
							throw new RuntimeException("Unknown column type: " + column.getType());
					}
				}
			}
			return record;
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println(Arrays.asList(record.toArray()));
		}
		return null;
	}

	public static void downloadData(String tableName, String fileName, String dateStr) {
		OSSClient client = null;
		RecordReader reader = null;
		try {
			client = new OSSClient(Constants.ossEndpoint, Constants.access_id, Constants.access_key);
			PartitionSpec partitionSpec = new PartitionSpec("partion_field='" + dateStr + "'");
			DownloadSession downloadSession = Constants.tunnel.createDownloadSession(Constants.hadoop_project,
					tableName, partitionSpec);

			long recordCount = downloadSession.getRecordCount();
			System.out.println("RecordCount is: " + recordCount);

			if (recordCount <= 0) {
				return;
			}

			reader = downloadSession.openRecordReader(0, recordCount);
			Record r = null;
			int count = 0;
			int index = 0;
			StringBuffer sb = new StringBuffer();
			while ((r = reader.read()) != null) {
				for (int i = 0; i < r.getColumnCount(); i++) {
					if (r.getColumns()[i].getType() == OdpsType.STRING) {
						sb.append(r.getString(i)).append("\t");
					} else {
						sb.append(r.get(i)).append("\t");
					}
				}
				sb.append("\r\n");

				count++;
				if (count >= 20000) {
					String key = fileName + index + ".txt";
					client.putObject(Constants.oss_bucket, key, new ByteArrayInputStream(sb.toString().getBytes()));
					index++;
					count = 0;
					sb = new StringBuffer();
				}
			}
			if (sb.length() > 0) {
				String key = fileName + index + ".txt";
				client.putObject(Constants.oss_bucket, key, new ByteArrayInputStream(sb.toString().getBytes()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (client != null) {
				client.shutdown();
			}
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static void deleteDir(String dir) {
		OSSClient ossClient = null;
		try {
			ossClient = new OSSClient(Constants.ossEndpoint, Constants.access_id, Constants.access_key);
			ObjectListing ossObjects = ossClient.listObjects(Constants.oss_bucket, dir);
			for (OSSObjectSummary object : ossObjects.getObjectSummaries()) {
				ossClient.deleteObject(Constants.oss_bucket, object.getKey());
			}
			ossClient.deleteObject(Constants.oss_bucket, dir);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (ossClient != null) {
				ossClient.shutdown();
			}
		}
	}

	public static Object getValueIfNull(Object o1, Object def){
		return o1 == null ? def : o1;
	}

	public static boolean isInteger(String number){ //只能匹配正常的数字 不能比配如1E10这类型的数字, 也不能匹配浮点数
	    if (number == null || number.isEmpty()){ return false; }
	    number = number.trim();
        if (number == null || number.isEmpty()){ return false; }
	    int flag = 0; // 0 , 1 代表正数但是1 代表显示声明是+的正数， -1代表负数

        if (number.charAt(0) == '+'){ flag = 1; }
        else if (number.charAt(0) == '-'){ flag = -1; }

		for (int i = Math.max(flag, 0); i <= number.length() - 1; i ++){
			if (!Character.isDigit(number.charAt(i))){ return false; }
		}

		return true;
	}


	public static String MD5Encode(String origin) {
		String resultString = null;

		try {
			resultString = new String(origin);
			MessageDigest md = MessageDigest.getInstance("MD5");
			resultString = byteArrayToHexString(md.digest(resultString.getBytes("UTF-8")));
		} catch (Exception var3) {
			;
		}

		return resultString;
	}
	private static String byteArrayToHexString(byte[] b) {
		StringBuffer resultSb = new StringBuffer();

		for(int i = 0; i < b.length; ++i) {
			resultSb.append(byteToHexString(b[i]));
		}

		return resultSb.toString();
	}

	private static final String[] hexDigits = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

	private static String byteToHexString(byte b) {
		int n = b;
		if (b < 0) {
			n = b + 256;
		}

		int d1 = n / 16;
		int d2 = n % 16;
		return hexDigits[d1] + hexDigits[d2];
	}






}
