package com.modak.app;

import com.modak.db.JdbcUtil;
import com.modak.utils.JsonToMap;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class CDCCrawler {

    private static final String CDC_FLOW = "cdc_flow";
    private static final String CDC_STAGING = "cdc_staging";
    private static final String CDC_ROLL_BACK = "cdc_roll_back";
    private static final String JSON_SCHEMA_PATH = "JSON_SCHEMA_PATH";
    private static final Logger logger = LoggerFactory.getLogger(CDCCrawler.class);
    private QueryRunner queryrunner = new QueryRunner();
    private STGroupFile cdcRollBackGroup = null;
    private STGroupFile cdcStagingGroup = null;
    private STGroupFile cdcFlowGroup = null;
    private String jsonSchemaFilePath = null;
    private JdbcUtil jdbcUtil = new JdbcUtil();
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public CDCCrawler(String crawlConfigPath) throws IOException {
        this(JsonToMap.jsonString2Map(FileUtils.readFileToString(new File(crawlConfigPath), Charset.defaultCharset())));
//        System.out.println(crawlConfigPath);
    }

    public CDCCrawler(Map<String, Object> crawlingConfig) {
        this(crawlingConfig.get(CDC_ROLL_BACK).toString(), crawlingConfig.get(CDC_STAGING).toString(), crawlingConfig.get(CDC_FLOW).toString());
//        System.out.println(crawlingConfig.get(CDC_STAGING).toString());
    }

    public CDCCrawler(String cdcRollBackGroup, String cdcStagingGroup, String cdcFlowGroup) {
        this.cdcRollBackGroup = new STGroupFile(cdcRollBackGroup, '$', '$');
        this.cdcStagingGroup = new STGroupFile(cdcStagingGroup, '$', '$');
        this.cdcFlowGroup = new STGroupFile(cdcFlowGroup, '$', '$');

    }

    public void crawl(HikariDataSource hikariDataSource, String ConfigPath, Map<String, Object> crawlInfo, String staging_schema, String datamart_schema, String table_name, List sourceColumns) throws Exception {

        try {
            this.doCDCStaging(hikariDataSource.getConnection(), Integer.valueOf(crawlInfo.get("crawl_id").toString()), staging_schema, datamart_schema, table_name,   sourceColumns);

        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    public void doCDCStaging(Connection hikariDataSource, int crawl_id, String staging_schema, String datamart_schema, String table_name, List sourceColumns) throws Exception {
        System.out.println("do CDC");
        try {
            //cdc_query1

//            logger.info("=========================================cdc_query1===============================================");
//            ST staging_cdc_staging_deletequery1 = cdcStagingGroup.getInstanceOf("staging_cdc_staging_deletequery1");
//            staging_cdc_staging_deletequery1.add("staging_schema", staging_schema);
//            staging_cdc_staging_deletequery1.add("table_name", table_name);
////            queryrunner.update(koshConnection, staging_cdc_staging_deletequery1.render());
//            //logger.info(staging_cdc_staging_deletequery1.render());
//            System.out.println(staging_cdc_staging_deletequery1.render());


            logger.info("=========================================cdc_query2===============================================");
            ST staging_cdc_staging_analysequery1 = cdcStagingGroup.getInstanceOf("staging_cdc_staging_analysequery1");
            staging_cdc_staging_analysequery1.add("staging_schema", staging_schema);
            staging_cdc_staging_analysequery1.add("table_name", table_name);
            queryrunner.update(hikariDataSource, staging_cdc_staging_analysequery1.render());
//            logger.info(staging_cdc_staging_analysequery1.render());
            System.out.println(staging_cdc_staging_analysequery1.render());

            logger.info("=========================================cdc_query3===============================================");
            ST staging_cdc_deletequery2_tmp_staging_update = cdcStagingGroup.getInstanceOf("staging_cdc_deletequery2_tmp_staging_update");
            staging_cdc_deletequery2_tmp_staging_update.add("staging_schema", staging_schema);
            staging_cdc_deletequery2_tmp_staging_update.add("table_name", table_name);
            queryrunner.update(hikariDataSource, staging_cdc_deletequery2_tmp_staging_update.render());
//            logger.info(staging_cdc_deletequery2_tmp_staging_update.render());
            System.out.println(staging_cdc_deletequery2_tmp_staging_update.render());

            logger.info("=========================================cdc_query4===============================================");
            ST staging_cdc_insertquery2_tmp_staging_update = cdcStagingGroup.getInstanceOf("find_new_records");
            staging_cdc_insertquery2_tmp_staging_update.add("staging_schema", staging_schema);
            staging_cdc_insertquery2_tmp_staging_update.add("table_name", table_name);
            staging_cdc_insertquery2_tmp_staging_update.add("sourceColumns", sourceColumns);
            staging_cdc_insertquery2_tmp_staging_update.add("datamart_schema", datamart_schema);
            staging_cdc_insertquery2_tmp_staging_update.add("crawl_id", crawl_id);
            queryrunner.update(hikariDataSource, staging_cdc_insertquery2_tmp_staging_update.render());
//            logger.info(staging_cdc_insertquery2_tmp_staging_update.render());
            System.out.println(staging_cdc_insertquery2_tmp_staging_update.render());

            logger.info("=========================================cdc_query5===============================================");
            ST staging_cdc_analyse_query2_tmp_staging_update = cdcStagingGroup.getInstanceOf("staging_cdc_analyse_query2_tmp_staging_update");
            staging_cdc_analyse_query2_tmp_staging_update.add("staging_schema", staging_schema);
            staging_cdc_analyse_query2_tmp_staging_update.add("table_name", table_name);
            queryrunner.update(hikariDataSource, staging_cdc_analyse_query2_tmp_staging_update.render());
            logger.info(staging_cdc_analyse_query2_tmp_staging_update.render());
            System.out.println(staging_cdc_analyse_query2_tmp_staging_update.render());

            logger.info("=========================================cdc_query6===============================================");
            ST staging_cdc_insertquery3_staging = cdcStagingGroup.getInstanceOf("staging_cdc_insertquery3_staging");
            staging_cdc_insertquery3_staging.add("staging_schema", staging_schema);
            staging_cdc_insertquery3_staging.add("table_name", table_name);
            staging_cdc_insertquery3_staging.add("sourceColumns", sourceColumns);
            staging_cdc_insertquery3_staging.add("datamart_schema", datamart_schema);
            queryrunner.update(hikariDataSource, staging_cdc_insertquery3_staging.render());
            logger.info(staging_cdc_insertquery3_staging.render());
            System.out.println(staging_cdc_insertquery3_staging.render());


            logger.info("=========================================cdc_query3===============================================");
            ST staging_cdc_deletequery2_tmp_staging_update1 = cdcStagingGroup.getInstanceOf("staging_cdc_deletequery2_tmp_staging_update");
            staging_cdc_deletequery2_tmp_staging_update1.add("staging_schema", staging_schema);
            staging_cdc_deletequery2_tmp_staging_update1.add("table_name", table_name);
            queryrunner.update(hikariDataSource, staging_cdc_deletequery2_tmp_staging_update1.render());
//            logger.info(staging_cdc_deletequery2_tmp_staging_update.render());
            System.out.println(staging_cdc_deletequery2_tmp_staging_update1.render());


            logger.info("=========================================cdc_query4===============================================");
            ST staging_cdc_insertquery2_tmp_staging_update1 = cdcStagingGroup.getInstanceOf("find_deleted_records");
            staging_cdc_insertquery2_tmp_staging_update1.add("staging_schema", staging_schema);
            staging_cdc_insertquery2_tmp_staging_update1.add("table_name", table_name);
            staging_cdc_insertquery2_tmp_staging_update1.add("sourceColumns", sourceColumns);
            staging_cdc_insertquery2_tmp_staging_update1.add("datamart_schema", datamart_schema);
            staging_cdc_insertquery2_tmp_staging_update1.add("crawl_id", crawl_id);
            queryrunner.update(hikariDataSource, staging_cdc_insertquery2_tmp_staging_update1.render());
//            logger.info(staging_cdc_insertquery2_tmp_staging_update.render());
            System.out.println(staging_cdc_insertquery2_tmp_staging_update1.render());


            logger.info("=========================================cdc_query7===============================================");
            ST staging_cdc_updatequery3_staging = cdcStagingGroup.getInstanceOf("update_deleted_records");
            staging_cdc_updatequery3_staging.add("staging_schema", staging_schema);
            staging_cdc_updatequery3_staging.add("table_name", table_name);
            staging_cdc_updatequery3_staging.add("datamart_schema", datamart_schema);
            queryrunner.update(hikariDataSource, staging_cdc_updatequery3_staging.render());
            logger.info(staging_cdc_updatequery3_staging.render());
            System.out.println(staging_cdc_updatequery3_staging.render());


            logger.info("=========================================cdc_query8===============================================");
            ST staging_cdc_analyse_query3_staging = cdcStagingGroup.getInstanceOf("staging_cdc_analyse_query3_staging");
            staging_cdc_analyse_query3_staging.add("table_name", table_name);
            staging_cdc_analyse_query3_staging.add("datamart_schema", datamart_schema);
            queryrunner.update(hikariDataSource, staging_cdc_analyse_query3_staging.render());
            logger.info(staging_cdc_analyse_query3_staging.render());
            System.out.println(staging_cdc_analyse_query3_staging.render());

            logger.info("cdc completed .... :)!");
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("crawling failed at cdc :( !");
        } finally {
            hikariDataSource.close();
        }
    }


}
