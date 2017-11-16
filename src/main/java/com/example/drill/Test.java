package com.example.drill;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by puroc on 2017/11/16.
 */
public class Test {

    public static final String SQL3 = "SELECT CONVERT_FROM(row_key, 'UTF8') AS " +
            "studentid, \n" +
            "        CONVERT_FROM(test3.students.name, 'UTF8') AS name, \n" +
            "        CONVERT_FROM(test3.students.state, 'UTF8') AS state, \n" +
            "        CONVERT_FROM(test3.students.street, 'UTF8') AS street, \n" +
            "        CONVERT_FROM(test3.students.zipcode, 'UTF8') AS zipcode \n" +
            " FROM test3 where (test3.students.name='name2' and test3.students.state='state2') or (test3.students" +
            ".name='name1' and test3.students.state='state1')";

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.drill.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:drill:zk=10.10.30.200:2181/drill/drillpud;" +
                    "schema=hbase");
            Statement st = connection.createStatement();
            final long start = System.currentTimeMillis();
            ResultSet rs = st.executeQuery(SQL3);
            while (rs.next()) {
                System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3));
            }
            final long stop = System.currentTimeMillis();
            System.out.println("time:"+(stop - start));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
