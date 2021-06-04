import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.*;

public class Session_TPCxIoT_Original extends Thread {
    public static String[] sensorKeys = {
            "cent_9_Humidity",
            "side_8_Humidity",
            "side_7_Humidity",
            "ang_30_Humidity",
            "ang_45_Humidity",
            "ang_60_Humidity",
            "ang_90_Humidity",
            "bef_1195_Humidity",
            "aft_1120_Humidity",
            "mid_1125_Humidity",
            "cor_4_Humidity",
            "cor_1_Humidity",
            "cor_5_Humidity",
            "cent_9_Power",
            "side_8_Power",
            "side_7_Power",
            "ang_30_Power",
            "ang_45_Power",
            "ang_60_Power",
            "ang_90_Power",
            "bef_1195_Power",
            "aft_1120_Power",
            "mid_1125_Power",
            "cor_4_Power",
            "cor_1_Power",
            "cor_5_Power",
            "cent_9_Pressure",
            "side_8_Pressure",
            "side_7_Pressure",
            "ang_30_Pressure",
            "ang_45_Pressure",
            "ang_60_Pressure",
            "ang_90_Pressure",
            "bef_1195_Pressure",
            "aft_1120_Pressure",
            "mid_1125_Pressure",
            "cor_4_Pressure",
            "cor_1_Pressure",
            "cor_5_Pressure",
            "cent_9_Flow",
            "side_8_Flow",
            "side_7_Flow",
            "ang_30_Flow",
            "ang_45_Flow",
            "ang_60_Flow",
            "ang_90_Flow",
            "bef_1195_Flow",
            "aft_1120_Flow",
            "mid_1125_Flow",
            "cor_4_Flow",
            "cor_1_Flow",
            "cor_5_Flow",
            "cent_9_Level",
            "side_8_Level",
            "side_7_Level",
            "ang_30_Level",
            "ang_45_Level",
            "ang_60_Level",
            "ang_90_Level",
            "bef_1195_Level",
            "aft_1120_Level",
            "mid_1125_Level",
            "cor_4_Level",
            "cor_1_Level",
            "cor_5_Level",
            "cent_9_Temperature",
            "side_8_Temperature",
            "side_7_Temperature",
            "ang_30_Temperature",
            "ang_45_Temperature",
            "ang_60_Temperature",
            "ang_90_Temperature",
            "bef_1195_Temperature",
            "aft_1120_Temperature",
            "mid_1125_Temperature",
            "cor_4_Temperature",
            "cor_1_Temperature",
            "cor_5_Temperature",
            "cent_9_vibration",
            "side_8_vibration",
            "side_7_Level",
            "ang_30_Level",
            "ang_45_Level",
            "ang_60_Level",
            "ang_90_Level",
            "bef_1195_Level",
            "aft_1120_Level",
            "mid_1125_Level",
            "cor_4_Level",
            "cor_1_Level",
            "cor_5_Level",
            "cent_9_Temperature",
            "side_8_Temperature",
            "side_7_Temperature",
            "ang_30_Temperature",
            "ang_45_Temperature",
            "ang_60_Temperature",
            "ang_90_Temperature",
            "bef_1195_Temperature",
            "aft_1120_Temperature",
            "mid_1125_Temperature",
            "cor_4_Temperature",
            "cor_1_Temperature",
            "cor_5_Temperature",
            "cent_9_vibration",
            "side_8_vibration",
            "side_7_vibration",
            "ang_30_vibration",
            "ang_45_vibration",
            "ang_60_vibration",
            "ang_90_vibration",
            "bef_1195_vibration",
            "aft_1120_vibration",
            "mid_1125_vibration",
            "cor_4_vibration",
            "cor_1_vibration",
            "cor_5_vibration",
            "cent_9_tilt",
            "side_8_tilt",
            "side_7_tilt",
            "ang_30_tilt",
            "ang_45_tilt",
            "ang_60_tilt",
            "ang_90_tilt",
            "bef_1195_tilt",
            "aft_1120_tilt",
            "mid_1125_tilt",
            "cor_4_tilt",
            "cor_1_tilt",
            "cor_5_tilt",
            "cent_9_level2",
            "side_8_level2",
            "side_7_level2",
            "ang_30_level2",
            "ang_45_level2",
            "ang_60_level2",
            "ang_90_level2",
            "bef_1195_level2",
            "aft_1120_level2",
            "mid_1125_level2",
            "cor_4_level2",
            "cor_1_level2",
            "cor_5_level2",
            "cent_9_level_vibrating",
            "side_8_level_vibrating",
            "side_7_level_vibrating",
            "ang_30_level_vibrating",
            "ang_45_level_vibrating",
            "ang_60_level_vibrating",
            "ang_90_level_vibrating",
            "bef_1195_level_vibrating",
            "aft_1120_level_vibrating",
            "mid_1125_level_vibrating",
            "cor_4_level_vibrating",
            "cor_1_level_vibrating",
            "cor_5_level_vibrating",
            "cent_9_level_rotating",
            "side_8_level_rotating",
            "side_7_level_rotating",
            "ang_30_level_rotating",
            "ang_45_level_rotating",
            "ang_60_level_rotating",
            "ang_90_level_rotating",
            "bef_1195_level_rotating",
            "aft_1120_level_rotating",
            "mid_1125_level_rotating",
            "cor_4_level_rotating",
            "cor_1_level_rotating",
            "cor_5_level_rotating",
            "cent_9_level_admittance",
            "side_8_level_admittance",
            "side_7_level_admittance",
            "ang_30_level_admittance",
            "ang_45_level_admittance",
            "ang_60_level_admittance",
            "ang_90_level_admittance",
            "bef_1195_level_admittance",
            "aft_1120_level_admittance",
            "mid_1125_level_admittance",
            "cor_4_level_admittance",
            "cor_1_level_admittance",
            "cor_5_level_admittance",
            "cent_9_Pneumatic_level",
            "side_8_Pneumatic_level",
            "side_7_Pneumatic_level",
            "ang_30_Pneumatic_level"
    };

    private static final String LOCAL_HOST = "127.0.0.1";
    private final int threadId;
    private final String deviceId;
    private final int insertNum;

    public Session_TPCxIoT_Original(int threadId, String deviceId, int insertNum) {
        this.threadId = threadId;
        this.deviceId = deviceId;
        this.insertNum = insertNum;
    }

    public void run() {
        Random rand = new Random();

        Session session;
        session = new Session(LOCAL_HOST, 6667, "root", "root");
        try {
            session.open(false);
            // session.setFetchSize(10000);
            for (String sensor : sensorKeys) {
                if (!session.checkTimeseriesExists(deviceId + "." + sensor)) {
                    session.createTimeseries(
                            deviceId + "." + sensor,
                            TSDataType.TEXT,
                            TSEncoding.PLAIN,
                            CompressionType.UNCOMPRESSED);
                }
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }

        byte[] tempBytes = new byte[1024];
        new Random().nextBytes(tempBytes);
        ArrayList<Boolean> generator = new ArrayList<Boolean>();
        String[] insertValue = new String[] {new String(tempBytes)};

        try {
            PartialPath deviceIdPath = new PartialPath(deviceId);
            long startTime = System.currentTimeMillis();
            long insertTime = startTime;
            for (int i = 0; i < insertNum; i++) {
                String sensor = sensorKeys[rand.nextInt(160)];
                if (rand.nextInt(insertNum) <= (insertNum * 0.00002)) { // SCAN OPERATION
                    long currentTimestamp = insertTime;
                    long scanTimestamp = currentTimestamp - 5000;
                    Map<Long, String> result1;
                    Map<Long, String> result2;
                    System.out.println("StartTime = " + startTime + ", currentTime=" + insertTime);
                    result1 = scanHelper(deviceId, sensor, scanTimestamp, session);

                    long oldTimestamp, randomTimestamp;
                    if (scanTimestamp - startTime > 1800000L) {
                        oldTimestamp = scanTimestamp - 1800000L;
                    } else {
                        oldTimestamp = startTime;
                    }
                    long r = rand.nextInt(1000) / 1000;
                    randomTimestamp = oldTimestamp + (r * (scanTimestamp - 10000L - oldTimestamp));

                    result2 = scanHelper(deviceId, sensor, randomTimestamp, session);
                    System.out.println(
                            "SCAN1 result size = " + result1.size() + ", SCAN2 result size = " + result2.size());

                } else {
                    String[] measurement = new String[] {sensor};
                    InsertRowPlan row_plan =
                            new InsertRowPlan(deviceIdPath, insertTime, measurement, insertValue);
                    try {
                        // ss.insertS(row_plan);
                        List<String> measurements = new ArrayList<>();
                        List<String> values = new ArrayList<>();
                        List<TSDataType> dataTypes = new ArrayList<>();
                        measurements.add(measurement[0]);
                        values.add(insertValue[0]);
                        dataTypes.add(TSDataType.TEXT);
                        // System.out.println("measurements = " + measurements.get(0));
                        session.insertRecord(deviceId, insertTime, measurements, dataTypes, insertValue);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                insertTime += 100;
            }
        } catch (Exception e) {
            System.err.println(e);
        }

        try {
            session.close();
        } catch (IoTDBConnectionException e) {
            e.printStackTrace();
        }
    }

    public Map<Long, String> scanHelper(
            String deviceId, String sensorId, long scanTimestamp, Session session)
            throws QueryProcessException, IOException, IoTDBConnectionException,
            StatementExecutionException {
        HashMap<Long, String> result = new HashMap<Long, String>();

        long beginTime, endTime;
        beginTime = scanTimestamp;
        endTime = beginTime + 5000L;

        String sqlstr =
                "select "
                        + sensorId
                        + " from "
                        + deviceId
                        + " where time > "
                        + beginTime
                        + " and time < "
                        + endTime;

        SessionDataSet dataSet = session.executeQueryStatement(sqlstr);
        System.out.println(dataSet.getColumnNames());
        dataSet.setFetchSize(1024); // default is 10000

        while (dataSet.hasNext()) {
            RowRecord rr = dataSet.next();
            result.put(rr.getTimestamp(), rr.getFields().get(0).toString());
        }

        dataSet.closeOperationHandle();

        return result;
    }

    public static void main(String[] args)
            throws IoTDBConnectionException, StatementExecutionException {

        if (args.length != 2) {
            System.out.println("Please use this command:  Session_TPCxIoT_Original Threads InsertCount");
            System.exit(0);
        }

        int threadNum = Integer.parseInt(args[0]);

        Session_TPCxIoT_Original[] se = new Session_TPCxIoT_Original[threadNum];
        int totalNum = 100000;
        totalNum = Integer.parseInt(args[1]);

        System.out.println("Threads: " + threadNum + " ,InsertCount: " + totalNum);

        for (int i = 1; i <= threadNum; i++) {
            String deviceId = "root.device" + i;

            se[i - 1] = new Session_TPCxIoT_Original(i, deviceId, totalNum / threadNum);
            se[i - 1].start();
        }
        long beginTime = System.currentTimeMillis();
        // insert row plan
        for (int i = 1; i <= threadNum; i++) {
            try {
                se[i - 1].join();
            } catch (InterruptedException e) {
            }
        }

        long endTime = System.currentTimeMillis();
        long timeDiff = endTime - beginTime;

        System.out.println("Elapsed Time(ms) : " + (timeDiff));
    }
}
