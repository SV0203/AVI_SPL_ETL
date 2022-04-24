package com.modak.app;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DDLCreate {

    public void getColumnDataType(String file_path ,Connection sourceConnection, Connection targetConnection) throws SQLException {

        List<String> core_tableName = getListOfCoreTables(file_path);
        List<String> public_tableName = getListOfPublicTables(file_path);

//        List<String> table_names = get_table_list(schema_names.getString(1), sourceConnection);
        core_tableName.forEach(table_name -> {
            try {
                List<String[]> list_column_datatype = get_column_datatype("core", table_name, sourceConnection);
                createNewTable("core", table_name, list_column_datatype, targetConnection);
//                create_hypertable(schema_names.getString(1), table_name, targetConnection); // To Create Hypertable
                injection("core", table_name, sourceConnection, targetConnection, list_column_datatype.size());
//                table_count(table_name, sourceConnection, targetConnection);

            } catch (SQLException throwable) {
                throwable.printStackTrace();
            }
        });

        public_tableName.forEach(table_name -> {
                    System.out.println("Schema name: " + "public" + " Table_name: " + table_name);
                    try {
                        List<String[]> list_column_datatype = get_column_datatype("public", table_name, sourceConnection);

                        createNewTable("public", table_name, list_column_datatype, targetConnection);
//                             create_hypertable(schema_names.getString(1),table_name,targetConnection); // To Create Hypertable
                        injection("public", table_name, sourceConnection, targetConnection, list_column_datatype.size());
//                       table_count(table_name, sourceConnection, targetConnection);
                    } catch (SQLException throwable) {
                        throwable.printStackTrace();
                    }
                }
        );

        sourceConnection.close();
        targetConnection.close();
    }

    private void createNewTable(String schema_name, String table_name, List<String[]> list_column_datatype, Connection connect) throws SQLException {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS staging" + "." + table_name + " (";
        String createSchemaQuery = "CREATE SCHEMA IF NOT EXISTS staging";
        StringBuilder sql = new StringBuilder(createTableQuery);
        String prefix = "";

        for (String[] s : list_column_datatype) {
            sql.append(prefix);
            prefix = ", ";
            sql.append(s[0]);
            sql.append(" ");
            sql.append(s[1]);
        }

        sql.append(");");
        System.out.println(sql);
        Statement stmt = connect.createStatement();
        try {
            stmt.execute(createSchemaQuery);
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


    }

    private List<String[]> get_column_datatype(String schema_name, String table_name, Connection connection) throws SQLException {
        String query_column_datatype = " select column_name,data_type " +
                "from information_schema.columns " +
                "where table_name = '" + table_name + "'  " +
                "and table_schema = '" + schema_name + "' ";

        System.out.println("Query: " + query_column_datatype);

        List<String[]> list_column_datatype = new ArrayList<>();

        try (Statement stmt = connection.createStatement()) {
            ResultSet column_name_datatype = stmt.executeQuery(query_column_datatype);

            while (column_name_datatype.next()) {
                String[] array_column_with_datatype = new String[2];
                array_column_with_datatype[0] = column_name_datatype.getString(1);
                array_column_with_datatype[1] = column_name_datatype.getString(2);
                System.out.println(column_name_datatype.getString(1));
                System.out.println(column_name_datatype.getString(2));
                list_column_datatype.add(array_column_with_datatype);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list_column_datatype;
    }

    public List<String> get_table_list(String individual_schema_name, Connection connect) throws SQLException {

        List<String> table_name_list = new ArrayList<String>();
        PreparedStatement preparedStatement = connect.prepareStatement("SELECT table_name\n" +
                "FROM information_schema.tables\n" +
                "WHERE table_schema = ? ");
        preparedStatement.setString(1, individual_schema_name);

        ResultSet result = preparedStatement.executeQuery();
        while (result.next()) {
            table_name_list.add(result.getString(1));
        }
        result.close();
        return table_name_list;
    }

    public void create_hypertable(String schema_name, String table_name, Connection connection) throws SQLException {
        String create_hypertable = "SELECT create_hypertable('" + schema_name + "." + table_name + "'," + "'dt_monitored');";

        System.out.println(create_hypertable);
        Statement stmt = connection.createStatement();

        try {
            stmt.execute(create_hypertable);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void injection(String schema_name, String table_name, Connection source_connection, Connection target_connection, int column_count) throws SQLException {
        String query = "select * from " + schema_name + "." + table_name;
        System.out.println(query);
        Statement stmt = source_connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query);
        resultSet.next();
        System.out.println(resultSet.getString(1));


        do {
            String insert_query = " Insert into staging." + table_name + " values(";
            StringBuilder sql = new StringBuilder(insert_query);
            String prefix = "";

            for (int i = 1; i <= column_count; i++) {
                sql.append(prefix);
                prefix = ", ";
                if (resultSet.getString(i) == null) {
                    sql.append(resultSet.getString(i));
                } else if (resultSet.getString(i).contains("'")) {
                    String colValue = resultSet.getString(i).replaceAll("'", "''");
                    sql.append("'");

                    sql.append(colValue);

                    sql.append("'");
                } else {
                    sql.append("'");

                    sql.append(resultSet.getString(i));

                    sql.append("'");
                    System.out.println(resultSet.getString(i));
                }
            }
            sql.append(");");
            System.out.println(sql);

            try {
                Statement statement = target_connection.createStatement();
                statement.executeQuery(sql.toString());
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

        } while (resultSet.next());
    }

    public void table_count(String table_name, Connection source_connection, Connection target_connection) throws Exception {

        String query = "select count(*) from " + "." + table_name;
        System.out.println(query);
        Statement stmt = source_connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query);
        resultSet.next();

    }

    public ArrayList<String> getListOfCoreTables(String file_path) {
        ArrayList<String> list = new ArrayList<String>();
        try {
            InputStream inputStream = new FileInputStream(new File(file_path + File.separator + "tables_to_load.yaml"));
            Yaml yaml = new Yaml();

            Map<String, Object> data = yaml.load(inputStream);
            list = (ArrayList) data.get("tables");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public ArrayList<String> getListOfPublicTables(String file_path) {
        ArrayList<String> list = new ArrayList<String>();
        try {
            InputStream inputStream = new FileInputStream(new File(file_path + File.separator + "tables_to_load.yaml"));
            Yaml yaml = new Yaml();

            Map<String, Object> data = yaml.load(inputStream);
            list = (ArrayList) data.get("large_tables");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}