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

public class JsonCDCPerformer {

    private static final String CDC_FLOW = "cdc_flow";
    private static final String JSON_CDC_STAGING = "json_cdc_perform";
    private static final String CDC_ROLL_BACK = "cdc_roll_back";
    private static final String JSON_SCHEMA_PATH = "JSON_SCHEMA_PATH";
    private static final Logger logger = LoggerFactory.getLogger(JsonCDCPerformer.class);
    private QueryRunner queryrunner = new QueryRunner();
    private STGroupFile cdcRollBackGroup = null;
    private STGroupFile jsoncdcStagingGroup = null;
    private STGroupFile cdcFlowGroup = null;
    private String jsonSchemaFilePath = null;
    private JdbcUtil jdbcUtil = new JdbcUtil();
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public JsonCDCPerformer(String crawlConfigPath) throws IOException {
        this(JsonToMap.jsonString2Map(FileUtils.readFileToString(new File(crawlConfigPath), Charset.defaultCharset())));
    }

    public JsonCDCPerformer(Map<String, Object> crawlingConfig) {
        this(crawlingConfig.get(CDC_ROLL_BACK).toString(), crawlingConfig.get(JSON_CDC_STAGING).toString(), crawlingConfig.get(CDC_FLOW).toString());
    }

    public JsonCDCPerformer(String cdcRollBackGroup, String jsoncdcStagingGroup, String cdcFlowGroup) {
        this.cdcRollBackGroup = new STGroupFile(cdcRollBackGroup, '$', '$');
        this.jsoncdcStagingGroup = new STGroupFile(jsoncdcStagingGroup, '$', '$');
        this.cdcFlowGroup = new STGroupFile(cdcFlowGroup, '$', '$');
    }

    public void crawl(HikariDataSource hikariDataSource, String ConfigPath, Map<String, Object> crawlInfo, String staging_schema, String datamart_schema, List sourceColumns) throws Exception {

        Connection targetConnection = hikariDataSource.getConnection();
        try {
            //performing cdc
            this.doCDCStaging(targetConnection, Integer.valueOf(crawlInfo.get("crawl_id").toString()), staging_schema, datamart_schema, sourceColumns);

        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            targetConnection.close();
            ;
        }
    }

    private void doCDCStaging(Connection koshConnection, int crawl_id, String staging_schema, String datamart_schema, List sourceColumns) throws Exception {
        try {
            //cdc_query1

            logger.info("=========================================cdc_query1===============================================");
//            ST staging_json_staging_deletequery1 = cdcStagingGroup.getInstanceOf("staging_json_staging_deletequery1");
//            staging_json_staging_deletequery1.add("staging_schema", staging_schema);
//            staging_json_staging_deletequery1.add("table_name", table_name);
//            //queryrunner.update(koshConnection, staging_json_staging_deletequery1.render());
//            //logger.info(staging_json_staging_deletequery1.render());
//            System.out.println(staging_json_staging_deletequery1.render());


//            logger.info("=========================================cdc_query2===============================================");
//            ST staging_json_analysequery1 = jsoncdcStagingGroup.getInstanceOf("staging_json_analysequery1");
//            staging_json_analysequery1.add("staging_schema", staging_schema);
//            queryrunner.update(koshConnection, staging_json_analysequery1.render());
//            logger.info(staging_json_analysequery1.render());
//           System.out.println(staging_json_analysequery1.render());

            logger.info("=========================================cdc_query3===============================================");
            ST staging_json_deletequery2_tmp_staging_update = jsoncdcStagingGroup.getInstanceOf("staging_json_deletequery2_tmp_staging_update");
            staging_json_deletequery2_tmp_staging_update.add("staging_schema", staging_schema);
            System.out.println(staging_json_deletequery2_tmp_staging_update.render());
            queryrunner.update(koshConnection, staging_json_deletequery2_tmp_staging_update.render());
            logger.info(staging_json_deletequery2_tmp_staging_update.render());

//
//            logger.info("=========================================cdc_query4===============================================");
            ST staging_json_insertquery2_tmp_staging_update = jsoncdcStagingGroup.getInstanceOf("staging_json_insertquery2_tmp_staging_update");
            staging_json_insertquery2_tmp_staging_update.add("staging_schema", staging_schema);
            staging_json_insertquery2_tmp_staging_update.add("sourceColumns", sourceColumns);
            staging_json_insertquery2_tmp_staging_update.add("datamart_schema", datamart_schema);
            staging_json_insertquery2_tmp_staging_update.add("crawl_id", crawl_id);
            queryrunner.update(koshConnection, staging_json_insertquery2_tmp_staging_update.render());
//            logger.info(staging_json_insertquery2_tmp_staging_update.render());
            System.out.println(staging_json_insertquery2_tmp_staging_update.render());
//
            logger.info("=========================================cdc_query5===============================================");
            ST staging_json_analyse_query2_tmp_staging_update = jsoncdcStagingGroup.getInstanceOf("staging_json_analyse_query2_tmp_staging_update");
            staging_json_analyse_query2_tmp_staging_update.add("staging_schema", staging_schema);
            queryrunner.update(koshConnection, staging_json_analyse_query2_tmp_staging_update.render());
//            logger.info(staging_json_analyse_query2_tmp_staging_update.render());
            System.out.println(staging_json_analyse_query2_tmp_staging_update.render());
//
//            logger.info("=========================================cdc_query6===============================================");
            ST staging_json_insertquery3_staging = jsoncdcStagingGroup.getInstanceOf("staging_json_insertquery3_staging");
            staging_json_insertquery3_staging.add("staging_schema", staging_schema);
            staging_json_insertquery3_staging.add("sourceColumns", sourceColumns);
            staging_json_insertquery3_staging.add("datamart_schema", datamart_schema);
            queryrunner.update(koshConnection, staging_json_insertquery3_staging.render());
//            logger.info(staging_json_insertquery3_staging.render());
            System.out.println(staging_json_insertquery3_staging.render());
//
//            logger.info("=========================================cdc_query7===============================================");
            ST staging_json_updatequery3_staging = jsoncdcStagingGroup.getInstanceOf("staging_json_updatequery3_staging");
            staging_json_updatequery3_staging.add("staging_schema", staging_schema);
            staging_json_updatequery3_staging.add("datamart_schema", datamart_schema);
            queryrunner.update(koshConnection, staging_json_updatequery3_staging.render());
//            logger.info(staging_json_updatequery3_staging.render());
            System.out.println(staging_json_updatequery3_staging.render());
//
//
//            logger.info("=========================================cdc_query8===============================================");
            ST staging_json_analyse_query3_staging = jsoncdcStagingGroup.getInstanceOf("staging_json_analyse_query3_staging");
            staging_json_analyse_query3_staging.add("datamart_schema", datamart_schema);
            queryrunner.update(koshConnection, staging_json_analyse_query3_staging.render());
            logger.info(staging_json_analyse_query3_staging.render());
            System.out.println(staging_json_analyse_query3_staging.render());

            logger.info("cdc completed .... :)!");
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("crawling failed at cdc :( !");
        } finally {
            koshConnection.close();
        }
    }


}
