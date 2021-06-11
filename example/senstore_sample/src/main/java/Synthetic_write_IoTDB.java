import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.*;

public class Synthetic_write_IoTDB extends Thread {

    private String deviceId;
    private int threadId;
    private int insertNum;

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

    public Synthetic_write_IoTDB() {}

    private static PlanExecutor planExecutor;
    private static Planner processor;

    static {
        IoTDB.metaManager.init();
    }

    public Synthetic_write_IoTDB(int threadId, String deviceId, int insertNum) {
        this.threadId = threadId;
        this.deviceId = deviceId;
        this.insertNum = insertNum;
    }

    public void run() {
        Random rand = new Random(threadId);

        byte[] tempBytes = new byte[1024];
        new Random().nextBytes(tempBytes);
        ArrayList<Boolean> generator = new ArrayList<Boolean>();
        String[] insertValue = new String[] {new String(tempBytes)};

        try {
            PartialPath deviceIdPath = new PartialPath(deviceId);
            long startTime = System.currentTimeMillis();
            long insertTime = startTime;
            for (int i = 0; i < insertNum; i++) {
                String sensor = sensorKeys[rand.nextInt(10)];

                String[] measurment = new String[] {sensor};
                InsertRowPlan row_plan =
                        new InsertRowPlan(deviceIdPath, insertTime, measurment, insertValue);
                try {
                    // ss.insertS(row_plan);
                    planExecutor.insert(row_plan);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                insertTime += 100;
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Please use this command:  TPCxIoT_IoTDB Threads InsertCount");
            System.exit(0);
        }
        IoTDB iotdb = IoTDB.getInstance();
        iotdb.active();

        try {
            planExecutor = new PlanExecutor();
            processor = new Planner();
        } catch (QueryProcessException e) {
            e.printStackTrace();
        }

        int threadNum = 2;
        threadNum = Integer.parseInt(args[0]);

        Synthetic_write_IoTDB[] se = new Synthetic_write_IoTDB[threadNum];
        int totalNum = 100000;
        totalNum = Integer.parseInt(args[1]);

        System.out.println("Threads: " + threadNum + " ,InsertCount: " + totalNum);
        CompressionType compressionType = CompressionType.UNCOMPRESSED;

        for (int i = 1; i <= threadNum; i++) {
            String deviceId = "root.sg1.d" + i;
            for (String sensor : sensorKeys) {
                try {
                    List<PartialPath> pp = new ArrayList<>();
                    pp.add(new PartialPath((deviceId + "." + sensor)));
                    planExecutor.processNonQuery(new DeleteTimeSeriesPlan(pp));

                    planExecutor.processNonQuery(
                            new CreateTimeSeriesPlan(
                                    new PartialPath(deviceId + "." + sensor),
                                    TSDataType.valueOf("TEXT"),
                                    TSEncoding.valueOf("PLAIN"),
                                    compressionType,
                                    null,
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    null));
                } catch (Exception e) {
                    // ignore
                }
            }
            se[i - 1] = new Synthetic_write_IoTDB(i, deviceId, totalNum / threadNum);
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

        iotdb.shutdown();

        System.out.println("Elapsed Time(ms) : " + (timeDiff));
    }
}
