package com.modak.db;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Ashish Veni  on 02022022
 */
public class DbAccessor {

    public List<Map<String, Object>> getListOfjson(String config_path) {
        HikariDataSource hikariDataSource = null;
        Connection kosh_conn = null;
        JdbcUtil jdbcUtil = new JdbcUtil();

        STGroup sqlQueriesgroup = new STGroupFile(config_path + File.separator + "getJsons.stg", '$', '$');
        ST pharamaProjects = sqlQueriesgroup.getInstanceOf("getListOfJsons");
        try {
            hikariDataSource = jdbcUtil.source_Connection(config_path + File.separator + "config.json");
            kosh_conn = hikariDataSource.getConnection();
            QueryRunner queryRunner = new QueryRunner();
            return queryRunner.query(kosh_conn, pharamaProjects.render(), new MapListHandler());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                kosh_conn.close();
                hikariDataSource.shutdown();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
