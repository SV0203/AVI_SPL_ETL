package com.modak.app;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class ColumnMetadataCrawler {

    private static final Logger logger = LoggerFactory.getLogger(ColumnMetadataCrawler.class);


    public List<Map<String, Object>> getListOfColumns(HikariDataSource target_connection, String config_path, String table_metadata, String tablename) throws SQLException {
//        HikariDataSource hikariDataSource = null;
        Connection kosh_conn = target_connection.getConnection();
//        JdbcUtil jdbcUtil = new JdbcUtil();

        STGroup sqlQueriesgroup = new STGroupFile(table_metadata, '$', '$');
        ST getTableColumns = sqlQueriesgroup.getInstanceOf("getTableColumns");
        getTableColumns.add("tablename", tablename);

        System.out.println(getTableColumns.render());
        try {
//            hikariDataSource = jdbcUtil.source_Connection(config_path+ File.separator+"config.json");
//            kosh_conn = hikariDataSource.getConnection();
            QueryRunner queryRunner = new QueryRunner();
            List listOfColumns = queryRunner.query(kosh_conn, getTableColumns.render(), new MapListHandler());

//
//            ST getTableCDC1 = sqlQueriesgroup.getInstanceOf("staging_cdc_table_data_from_create_ddl").add("sourceColumns", listOfColumns);
//            getTableCDC1.add("tablename", tablename);
//            //ST temp_obj = obj.getInstanceOf("sourceColumns").add("list",listOfColumns);
//            String str = getTableCDC1.render();
//            System.out.println(str);
            return listOfColumns;

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kosh_conn.close();
//                hikariDataSource.shutdown();
        }


        return null;

    }


    public List<Map<String, Object>> getJsonListOfColumns(Connection kosh_conn, String table_metadata) {


        STGroup sqlQueriesgroup = new STGroupFile(table_metadata, '$', '$');
        ST getJsontableColumns = sqlQueriesgroup.getInstanceOf("getJsontableColumns");
        System.out.println(getJsontableColumns.render());
        try {

            QueryRunner queryRunner = new QueryRunner();
            List listOfColumns = queryRunner.query(kosh_conn, getJsontableColumns.render(), new MapListHandler());

            return listOfColumns;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                kosh_conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;

    }
}

