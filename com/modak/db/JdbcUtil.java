package com.modak.db;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.modak.utils.AppConstants;
import com.modak.utils.encryption.RSAEncryptionUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;

public class JdbcUtil {+

    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * @param configFilePath
     * @return
     * @throws IOException
     */
    public HikariDataSource source_Connection(String configFilePath) throws Exception {
        String jsonContent = FileUtils.readFileToString(new File(configFilePath), Charset.defaultCharset());
        Map<String, Object> configuration = objectMapper.readValue(jsonContent, new TypeReference<HashMap<String, Object>>() {
        });
        return source_Connection(configuration);
    }

    public HikariDataSource target_Connection(String configFilePath) throws Exception {
        String jsonContent = FileUtils.readFileToString(new File(configFilePath), Charset.defaultCharset());
        Map<String, Object> configuration = objectMapper.readValue(jsonContent, new TypeReference<HashMap<String, Object>>() {
        });
        return target_Connection(configuration);
    }

    /**
     * @param config
     * @return
     */
    public HikariDataSource source_Connection(Map<String, Object> config) throws Exception {
        //System.out.println(config);

        return source_Connection(config.get(AppConstants.Source_Url).toString(),
                RSAEncryptionUtils.decryptPassword(config.get(AppConstants.Source_userName).toString(),
                        "com/modak/resources/privateKey.key"),
                RSAEncryptionUtils.decryptPassword(config.get(AppConstants.Source_password).toString(),
                        "com/modak/resources/privateKey.key"),
                config.get(AppConstants.DRIVER).toString(),
                (Integer) config.get(AppConstants.POOLSIZE),
                (Integer) config.get(AppConstants.MINIMUMIDLE));
    }

    public HikariDataSource target_Connection(Map<String, Object> config) throws Exception {
        System.out.println(config);

        return target_Connection(config.get(AppConstants.Target_Url).toString(),
                RSAEncryptionUtils.decryptPassword(config.get(AppConstants.Target_userName).toString(),
                        "com/modak/resources/privateKey.key"),
                RSAEncryptionUtils.decryptPassword(config.get(AppConstants.Target_password).toString(),
                        "com/modak/resources/privateKey.key"),
                config.get(AppConstants.DRIVER).toString(),
                (Integer) config.get(AppConstants.POOLSIZE),
                (Integer) config.get(AppConstants.MINIMUMIDLE));
    }

    /**
     * @param jdbcUrl
     * @param userName
     * @param password
     * @param driver
     * @param poolSize
     * @param minimumIdle
     * @return
     */

    public HikariDataSource source_Connection(String jdbcUrl, String userName, String password, String driver, int poolSize, int minimumIdle) throws SQLException {
        // System.out.println(jdbcUrl+" == "+userName+" == "+password+" == "+driver+" == "+poolSize+" == "+minimumIdle);
        HikariConfig jdbcConfig = new HikariConfig();
        jdbcConfig.setMaximumPoolSize(poolSize);
        jdbcConfig.setMinimumIdle(minimumIdle);
        jdbcConfig.setJdbcUrl(jdbcUrl);
        jdbcConfig.setDriverClassName(driver);
        jdbcConfig.setUsername(userName);
        jdbcConfig.setPassword(password);
        System.out.println("creating connection pool .....");
        HikariDataSource hikariDataSource = new HikariDataSource(jdbcConfig);
        System.out.println("connection pool created!");
        Connection connection = hikariDataSource.getConnection();
        if (!connection.isClosed())
            System.out.println("postgres connected :)  ...!");
        close(connection);
        return hikariDataSource;
    }

    public HikariDataSource target_Connection(String jdbcUrl, String userName, String password, String driver, int poolSize, int minimumIdle) throws SQLException {
        // System.out.println(jdbcUrl+" == "+userName+" == "+password+" == "+driver+" == "+poolSize+" == "+minimumIdle);
        HikariConfig jdbcConfig = new HikariConfig();
        jdbcConfig.setMaximumPoolSize(poolSize);
        jdbcConfig.setMinimumIdle(minimumIdle);
        jdbcConfig.setJdbcUrl(jdbcUrl);
        jdbcConfig.setDriverClassName(driver);
        jdbcConfig.setUsername(userName);
        jdbcConfig.setPassword(password);
        System.out.println("creating connection pool .....");
        HikariDataSource hikariDataSource = new HikariDataSource(jdbcConfig);
        System.out.println("connection pool created!");
        Connection connection = hikariDataSource.getConnection();
        if (!connection.isClosed())
            System.out.println("timeScale connected :)  ...!");
        close(connection);
        return hikariDataSource;
    }


    /**
     * @param query
     * @param connection
     * @throws Exception
     */
    public void executeUpdateQuery(String query, Connection connection) throws Exception {
        Statement statement = connection.createStatement();
        statement.executeUpdate(query);
        close(statement);
        close(connection);
    }

    /**
     * @param query
     * @param connection
     * @return
     * @throws Exception
     */
    public List<LinkedHashMap<String, Object>> executeSelectQuery(String query, Connection connection) throws Exception {
        List<LinkedHashMap<String, Object>> data = new ArrayList<LinkedHashMap<String, Object>>();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        LinkedList<String> columnList = new LinkedList<String>();
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        for (int columnIndex = 1; columnIndex <= resultSetMetaData.getColumnCount(); columnIndex++) {
            columnList.add(resultSetMetaData.getColumnName(columnIndex));
        }
        while (rs.next()) {
            LinkedHashMap<String, Object> linkedHashMap = new LinkedHashMap<String, Object>();
            for (String s : columnList) {
                linkedHashMap.put(s, rs.getObject(s));
            }
            data.add(linkedHashMap);
        }
        close(statement);
        close(rs);
        close(connection);
        return data;

    }


    /**
     * @param resultSet
     */
    public void close(ResultSet resultSet) {
        try {
            resultSet.close();
        } catch (Exception e) {
            System.out.println("unable to close resultset");
        }
    }


    public void close(Statement statement) {
        try {
            statement.close();
        } catch (Exception e) {
            System.out.println("unable to close statement object");
        }
    }

    public void close(Connection connection) {
        try {
            connection.close();
        } catch (Exception e) {
            System.out.println("unable to close connection object");
        }

    }

    public void shutDownPool(HikariDataSource hikariDataSource) {
        hikariDataSource.shutdown();
    }

}
