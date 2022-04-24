package com.modak;

import com.modak.app.*;
import com.modak.db.JdbcUtil;
import com.modak.utils.JsonToMap;
import com.zaxxer.hikari.HikariDataSource;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class App {

    public static void main(String[] args) throws Exception {
        //Change the Config_path.stg and arg[0] before running the code

        String config_path = args[0];
        Map<String, Object> outputMap = null;
        JdbcUtil jdbcUtil = new JdbcUtil();
        STGroup sqlQueriesgroup = new STGroupFile(config_path + "config_path.stg", '$', '$');


        ST cdcqueries = sqlQueriesgroup.getInstanceOf("cdcqueries");

        outputMap = JsonToMap.jsonString2Map(cdcqueries.render());
        String config = (String) outputMap.get("config_path");
        String table_metadata = (String) outputMap.get("table_metadata");
        String staging_schema = (String) outputMap.get("staging_schema");
        String datamart_schema = (String) outputMap.get("datamart_schema");
        String core_schema = (String) outputMap.get("core_schema");

        HikariDataSource sourceConnection = jdbcUtil.source_Connection(config + File.separator + "config.json");
        HikariDataSource targetConnection = jdbcUtil.target_Connection(config + File.separator + "config.json");


//Step1-Injection of whole Schema

        DDLCreate ddlCreate = new DDLCreate();
        ddlCreate.getColumnDataType(config,sourceConnection.getConnection(), targetConnection.getConnection());


//Step2- Datamart Tables Creation
        DataMartTableCRUD dataMartTableCRUD = new DataMartTableCRUD();
        dataMartTableCRUD.DataMartTables(config, targetConnection.getConnection());


//Step3- Datamart Tables Creation
        dataMartTableCRUD.datamart_agg(config, targetConnection.getConnection());


// Step4- CDC
        TableMetadataCrawler TableMetadataCrawler = new TableMetadataCrawler(config + File.separator + "crawling.json");
        ArrayList<String> listOfTables = TableMetadataCrawler.getListOfTables(config);
        Map<String, Object> crawlInfo = TableMetadataCrawler.startCrawl(targetConnection);


        ColumnMetadataCrawler ColumnMetadataCrawler = new ColumnMetadataCrawler();

        CDCCrawler cdcCrawler = new CDCCrawler(config + File.separator + "crawling.json");

        for (int counter = 0; counter < listOfTables.size(); counter++) {
            List sourcecolumns = ColumnMetadataCrawler.getListOfColumns(targetConnection, config, table_metadata, listOfTables.get(counter));
            cdcCrawler.crawl(targetConnection, config + File.separator + "crawling.json", crawlInfo, staging_schema, datamart_schema, listOfTables.get(counter), sourcecolumns);
        }

        JsonParser jsonparser = new JsonParser();
        jsonparser.getJsonColumnNames(targetConnection.getConnection(), config, table_metadata);

        List jsonsourcecolumns = ColumnMetadataCrawler.getJsonListOfColumns(targetConnection.getConnection(), table_metadata);
        JsonCDCPerformer jsonCDCPerformer = new JsonCDCPerformer(config + File.separator + "crawlingJson.json");

        jsonCDCPerformer.crawl(targetConnection, config + File.separator + "crawlingJson.json", crawlInfo, staging_schema, datamart_schema, jsonsourcecolumns);

        TableMetadataCrawler.FinishCrawl(targetConnection, crawlInfo);

    }
}





