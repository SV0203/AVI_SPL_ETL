package com.modak.app;

import com.modak.db.JdbcUtil;
import com.modak.utils.JsonToMap;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Map;

public class TableMetadataCrawler {

    private static final String CDC_FLOW = "cdc_flow";
    private static final String CDC_ROLL_BACK = "cdc_roll_back";
    private static final Logger logger = LoggerFactory.getLogger(TableMetadataCrawler.class);
    private QueryRunner queryrunner = new QueryRunner();
    private STGroupFile cdcRollBackGroup = null;
    private STGroupFile cdcFlowGroup = null;


    public TableMetadataCrawler(String crawlingPath) throws IOException {
        this(JsonToMap.jsonString2Map(FileUtils.readFileToString(new File(crawlingPath), Charset.defaultCharset())));
    }

    public TableMetadataCrawler(Map<String, Object> crawlingConfig) {
        this(crawlingConfig.get(CDC_ROLL_BACK).toString(), crawlingConfig.get(CDC_FLOW).toString());
    }

    public TableMetadataCrawler(String cdcRollBackGroup, String cdcFlowGroup) {
        this.cdcRollBackGroup = new STGroupFile(cdcRollBackGroup, '$', '$');
        this.cdcFlowGroup = new STGroupFile(cdcFlowGroup, '$', '$');

    }

    public Map<String, Object> startCrawl(HikariDataSource hikariDataSource) throws Exception {
        Map<String, Object> crawlInfo = this.initiateCrawling(hikariDataSource.getConnection());
        return crawlInfo;
    }

    public void FinishCrawl(HikariDataSource hikariDataSource, Map<String, Object> crawlInfo) throws Exception {
        this.finishCrawling(hikariDataSource.getConnection(), Integer.valueOf(crawlInfo.get("crawl_id").toString()));
        logger.info("crawling done sucessfully :) .............!!!!");
        //hikariDataSource.close();
    }

    public ArrayList<String> getListOfTables(String file_path) {
        ArrayList<String> list = new ArrayList<String>();
        {
            try {
                InputStream inputStream = new FileInputStream(new File(file_path+ File.separator +"datamart_tablesToLoad.yaml"));
                Yaml yaml = new Yaml();

                Map<String, Object> data = yaml.load(inputStream);
                list = (ArrayList) data.get("tables");
                return list;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private void rollBack(Connection connection_kosh) throws Exception {
        logger.info("Checking if roll back required ");
        try {

            // requireRollBackCrawlJobInfo
            Map<String, Object> mapgetcrawljobno = null;
            ST requireRollBackCrawlJobInfo = cdcRollBackGroup.getInstanceOf("requireRollBackCrawlJobInfo");
            mapgetcrawljobno = queryrunner.query(connection_kosh, requireRollBackCrawlJobInfo.render(),
                    new MapHandler());
            logger.info("===========================================================");
            logger.info(requireRollBackCrawlJobInfo.render());
            // RollBackCrawlJobInfo
            if (mapgetcrawljobno.get("crawl_id") != null) {
                logger.info("crawl_id to be roll backed : " + mapgetcrawljobno.get("crawl_id"));
                Map<String, Object> maprollBackCrawlJobInfo = null;
                ST rollBackCrawlJobInfo = cdcRollBackGroup.getInstanceOf("RollBackCrawlJobInfo");
                rollBackCrawlJobInfo.add("rollbackjobno", mapgetcrawljobno);
                maprollBackCrawlJobInfo = queryrunner
                        .query(connection_kosh, rollBackCrawlJobInfo.render(), new MapHandler());
                logger.info("===========================================================");
                logger.info(rollBackCrawlJobInfo.render());
                logger.info(rollBackCrawlJobInfo.render());
                mapgetcrawljobno.put("prev_crawl_id", maprollBackCrawlJobInfo.get("prev_crawl_id"));

                ST rollbackTableUpdate = cdcRollBackGroup.getInstanceOf("rollbackTableUpdate");
                rollbackTableUpdate.add("mapgetcrawljobno", mapgetcrawljobno);
                logger.info(rollbackTableUpdate.render());
                queryrunner.update(connection_kosh, rollbackTableUpdate.render());
                logger.info("===========================================================");
                logger.info(rollbackTableUpdate.render());

                ST rollbackTableDelete = cdcRollBackGroup.getInstanceOf("rollbackTableDelete");
                rollbackTableDelete.add("mapgetcrawljobno", mapgetcrawljobno);
                logger.info(rollbackTableDelete.render());
                logger.info("===========================================================");
                logger.info(rollbackTableDelete.render());
                queryrunner.update(connection_kosh, rollbackTableDelete.render());

                // delete query
                ST delete_query = cdcRollBackGroup.getInstanceOf("delete_query");
                delete_query.add("rollbackjobno", mapgetcrawljobno);
                logger.info("===========================================================");
                logger.info(delete_query.render());
                queryrunner.update(connection_kosh, delete_query.render());
            } else {
                logger.info("Roll back not required continue with crawling");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new Exception(ex);
        } finally {
            if (connection_kosh != null) {
                connection_kosh.close();
            }
        }
    }

    private Map<String, Object> initiateCrawling(Connection koshConnection) throws Exception {
        try {
            logger.info("======================starting flow===============================");

            //getNewCrawlJobInfo
            ST getNewCrawlJobInfo = cdcFlowGroup.getInstanceOf("getNewCrawlJobInfo");
            Map<String, Object> newCrawlJobInfoMap = queryrunner.query(koshConnection, getNewCrawlJobInfo.render(), new MapHandler());
            logger.info(getNewCrawlJobInfo.render());

            //insertCrawlJobInfo
            ST insertCrawlJobInfo = cdcFlowGroup.getInstanceOf("insertCrawlJobInfo")
                    .add("crawl_id", newCrawlJobInfoMap.get("crawl_id"));
            queryrunner.update(koshConnection, insertCrawlJobInfo.render());

            logger.info(insertCrawlJobInfo.render());
            return newCrawlJobInfoMap;
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new Exception("crawiling failed at intializing :( !");
        } finally {
            if (koshConnection != null) {
                koshConnection.close();
            }
        }
    }

    //updateCrawlJobInfo_Finished
    private void finishCrawling(Connection koshConnection, int crawl_id) throws Exception {

        try {
            ST updateCrawlJobInfo_Finished = cdcFlowGroup.getInstanceOf("updateCrawlJobInfo_Finished")
                    .add("crawl_id", crawl_id);
            queryrunner.update(koshConnection, updateCrawlJobInfo_Finished.render());
            System.out.println(updateCrawlJobInfo_Finished.render());
            logger.info(updateCrawlJobInfo_Finished.render());
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(" crawling failed at finishing:( !");
        } finally {
            koshConnection.close();
        }


    }

}