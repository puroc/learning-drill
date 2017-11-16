package com.example.drill;


import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by puroc on 2017/11/16.
 */
@Component
public class DrillService implements CommandLineRunner {

    public static final String SQL = "SELECT CONVERT_FROM(row_key, 'UTF8') AS " +
            "studentid, \n" +
            "        CONVERT_FROM(students.account.name, 'UTF8') AS name, \n" +
            "        CONVERT_FROM(students.address.state, 'UTF8') AS state, \n" +
            "        CONVERT_FROM(students.address.street, 'UTF8') AS street, \n" +
            "        CONVERT_FROM(students.address.zipcode, 'UTF8') AS zipcode \n" +
            " FROM students limit 50";

    public static final String SQL2 = "SELECT CONVERT_FROM(row_key, 'UTF8') AS " +
            "studentid, \n" +
            "        CONVERT_FROM(test3.students.name, 'UTF8') AS name, \n" +
            "        CONVERT_FROM(test3.students.state, 'UTF8') AS state, \n" +
            "        CONVERT_FROM(test3.students.street, 'UTF8') AS street, \n" +
            "        CONVERT_FROM(test3.students.zipcode, 'UTF8') AS zipcode \n" +
            " FROM test3 where test3.students.name='name1' limit 50";

    public static final String SQL3 = "SELECT CONVERT_FROM(row_key, 'UTF8') AS " +
            "studentid, \n" +
            "        CONVERT_FROM(test3.students.name, 'UTF8') AS name, \n" +
            "        CONVERT_FROM(test3.students.state, 'UTF8') AS state, \n" +
            "        CONVERT_FROM(test3.students.street, 'UTF8') AS street, \n" +
            "        CONVERT_FROM(test3.students.zipcode, 'UTF8') AS zipcode \n" +
            " FROM test3 where (test3.students.name='name2' and test3.students.state='state2') or (test3.students" +
            ".name='name1' and test3.students.state='state1') limit 50";


    @Autowired
    private JdbcTemplate jdbcTemplate2;

    @Bean(name = "jdbcTemplate2")
    public JdbcTemplate jdbcTemplate2(@Qualifier("dataSource2") DataSource dataSource2) {
        JdbcTemplate oracle = new JdbcTemplate();
        oracle.setDataSource(dataSource2);
        return oracle;
    }

    @Bean(name = "dataSource2")
    public DataSource dataSource2() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:drill:zk=10.10.30.200:2181/drill/drillpud;" +
                "schema=hbase");
        dataSource.setDriverClassName("org.apache.drill.jdbc.Driver");
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(20);
        dataSource.setMinIdle(0);
        dataSource.setPoolPreparedStatements(true);
        dataSource.setMaxWait(60000);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestWhileIdle(true);
        return dataSource;
    }

    @Override
    public void run(String... strings) throws Exception {

        final long start = System.currentTimeMillis();

        final List<Student> list = jdbcTemplate2.query(SQL3, new RowMapper<Student>() {
            @Override
            public Student mapRow(ResultSet resultSet, int i) throws SQLException {
                Student s = new Student();
                s.setName(resultSet.getString("name"));
                s.setState(resultSet.getString("state"));
                s.setStreet(resultSet.getString("street"));
                s.setZipcode(resultSet.getString("zipcode"));
                return s;
            }
        });
        for (Student student : list) {
            System.out.println("name:" + student.getName() + ",state:" + student.getState() + ",street:" + student
                    .getStreet() + ",zipcode:" + student
                    .getZipcode());
        }
        final long stop = System.currentTimeMillis();
        System.out.println("time:" + (stop - start) + " ms");
    }
}
