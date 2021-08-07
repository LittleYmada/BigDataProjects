import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.HttpCookie;

import static org.apache.hadoop.hbase.client.ConnectionFactory.createConnection;

public class HBaseInsert {
    public static void main(String [] args) throws IOException {
        // config and connect
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.16.63.15");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        Connection connection;
        connection= createConnection(conf);

        // create table and column family
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("SunPengyu");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor tdesc = new HTableDescriptor(tableName);
        tdesc.addFamily(new HColumnDescriptor("name"));
        tdesc.addFamily(new HColumnDescriptor("info"));
        tdesc.addFamily(new HColumnDescriptor("score"));
        admin.createTable(tdesc);

        Table table =connection.getTable(TableName.valueOf("SunPengyu"));

        // put
        String[][] students= {
                {"0", "Tom", "20210000000001", "1", "75", "82"},
                {"1", "Jerry", "20210000000002", "1", "85", "67"},
                {"2", "Jack", "20210000000003", "2", "80", "80"},
                {"3", "Rose", "20210000000004", "2", "60", "61"},
                {"4", "SunPengyu", "20210735010137", "2", "0","0"}};

        for (int i = 0; i < students.length; i++) {
            Put student = new Put(Bytes.toBytes(students[i][0]));
            student.addColumn(Bytes.toBytes("name"), Bytes.toBytes(""), Bytes.toBytes(students[i][1]));
            student.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(students[i][2]));
            student.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes(students[i][3]));
            student.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes(students[i][4]));
            student.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes(students[i][4]));
            table.put(student);
        }

        // read
        System.out.println("Name,\tIdInfo,\tClassInfo,\tUnderstandingScore,\tProgrammingScore");
        for (int i = 0; i < students.length; i++) {
            Get get = new Get(Bytes.toBytes(students[i][0]));
            Result result = table.get(get);
            byte[] name = result.getValue(Bytes.toBytes("name"), Bytes.toBytes(""));
            byte[] id_info = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"));
            byte[] class_info = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"));
            byte[] understanding_score = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("understanding"));
            byte[] programming_score = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("programming"));

            String Name = Bytes.toString(name);
            String IdInfo = Bytes.toString(id_info);
            String ClassInfo = Bytes.toString(class_info);
            String UnderstandingScore = Bytes.toString(understanding_score);
            String ProgrammingScore = Bytes.toString(programming_score);

            System.out.println(Name + ",\t" + IdInfo + ",\t" + ClassInfo + ",\t" + UnderstandingScore + ",\t" + ProgrammingScore);
        }

        // delete
        String deleRowKey = "2";
        Delete delete_ = new Delete(Bytes.toBytes(deleRowKey));
        table.delete(delete_);

        // show delete results
        System.out.println("Name,\tIdInfo,\tClassInfo,\tUnderstandingScore,\tProgrammingScore");
        for (int i = 0; i < students.length; i++) {
            if (i == 2) continue;
            Get get = new Get(Bytes.toBytes(students[i][0]));
            Result result = table.get(get);
            byte[] name = result.getValue(Bytes.toBytes("name"), Bytes.toBytes(""));
            byte[] id_info = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"));
            byte[] class_info = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"));
            byte[] understanding_score = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("understanding"));
            byte[] programming_score = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("programming"));

            String Name = Bytes.toString(name);
            String IdInfo = Bytes.toString(id_info);
            String ClassInfo = Bytes.toString(class_info);
            String UnderstandingScore = Bytes.toString(understanding_score);
            String ProgrammingScore = Bytes.toString(programming_score);

            System.out.println(Name + ",\t" + IdInfo + ",\t" + ClassInfo + ",\t" + UnderstandingScore + ",\t" + ProgrammingScore);
        }




        // close
        try{
            if (table != null) {
                table.close();
            }

            if (admin != null) {
                admin.close();
            }

            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e){}



    }
}
