package src.main.javaAPI;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by yunchen on 2017/8/29.
 */
public class javaScanKudu {

        public static final String KUDU_MASTER = "192.168.1.227:7151";

        public static void main(String[] args) throws KuduException {

            String tableName = "impala::default.tsgz_syslog";

            KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

            KuduTable table = client.openTable(tableName);
            Schema schema = table.getSchema();

            List projectColumns = new ArrayList();
/*            projectColumns.add("sys_id");
            projectColumns.add("sys_time");
            projectColumns.add("sys_message");
            projectColumns.add("sys_hostname");*/
            projectColumns.add("stds_id");
            projectColumns.add("stds_srcip");

            //这里可以加很多过滤条件，也可以不加
            KuduPredicate kp1 = KuduPredicate.newComparisonPredicate(schema.getColumn("stds_id"), KuduPredicate.ComparisonOp.EQUAL, "0002aac5-cf50-4804-a954-8a3a2cd133aa");//创建predicate
            KuduPredicate kp2 = KuduPredicate.newComparisonPredicate(schema.getColumn("stds_srcip"), KuduPredicate.ComparisonOp.EQUAL, "zjdw-pre0064");//创建predicate
            KuduPredicate kp3 = KuduPredicate.newComparisonPredicate(schema.getColumn("stds_id"), KuduPredicate.ComparisonOp.EQUAL, "00012bbc-e9b9-413a-80e4-4c5d67c6b3d8");//创建predicate

            KuduScanner scanner = (KuduScanner) client.newScannerBuilder(table)
                    .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)//设置读取快照模式
                    .setProjectedColumnNames(projectColumns)
                    //.addPredicate(kp1)//设置predicate
                    .addPredicate(kp2)
                    .addPredicate(kp3)
                    //.snapshotTimestampMicros(Long.valueOf("1463388259000000"))//设置时间戳
                    .build();

            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                int i=0;
                while (results.hasNext()) {
                    RowResult result = results.next();
                    System.out.println(i+" : "+result.getString(0)+" "+result.getString(1)+" "+result.getString(2));
                }
                break;
            }

        }

    }
