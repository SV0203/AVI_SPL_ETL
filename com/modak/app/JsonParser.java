package com.modak.app;

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

public class JsonParser {


    private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);
    private QueryRunner queryrunner = new QueryRunner();
    private STGroupFile cdcRollBackGroup = null;
    private STGroupFile cdcFlowGroup = null;

    public void getJsonColumnNames(Connection kosh_conn, String config_path, String table_metadata) throws SQLException {

        STGroup sqlQueriesgroup = new STGroupFile(table_metadata, '$', '$');
        ST getJsonColumns = sqlQueriesgroup.getInstanceOf("getJsonColumns");
        System.out.println(getJsonColumns.render());

        ST insertJsonColumns = sqlQueriesgroup.getInstanceOf("insertStagingJsonColumns");
        System.out.println(insertJsonColumns.render());

        try {
            QueryRunner queryRunner = new QueryRunner();
            List<Map<String, Object>> listJsonColumns = queryRunner.query(kosh_conn, getJsonColumns.render(), new MapListHandler());
            queryrunner.batch(kosh_conn, insertJsonColumns.render(), this.createObjectArrayFromMapList(listJsonColumns));
            logger.info(insertJsonColumns.render());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kosh_conn.close();
        }


    }

    private Object[][] createObjectArrayFromMapList(List<Map<String, Object>> mapList) {
        Object[][] listArray = new Object[mapList.size()][];
        for (int mapIndex = 0; mapIndex < mapList.size(); mapIndex++) {
            Object[] mapArray = {mapList.get(mapIndex).get("device_model"), mapList.get(mapIndex).get("properties")};
            listArray[mapIndex] = mapArray;
        }
        return listArray;
    }

}