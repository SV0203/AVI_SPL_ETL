package com.modak.app;


import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DataMartTableCRUD {
    private static final Logger logger = LoggerFactory.getLogger(DataMartTableCRUD.class);

    public List<Map<String, Object>> DataMartTables(String config_path, Connection target_Connection) throws Exception {

        String currentLine = "";

        try {

            QueryRunner queryRunner = new QueryRunner();
            STGroup sqlQueriesgroup = new STGroupFile(config_path + File.separator + "datamart_staging_queries.stg", '$', '$');
            BufferedReader reader = new BufferedReader(new FileReader(config_path + File.separator + "datamart_staging_template_name.txt"));
            while ((currentLine = reader.readLine()) != null) {
                ST template_run = sqlQueriesgroup.getInstanceOf(currentLine);
                System.out.println(template_run.render());
                queryRunner.update(target_Connection, template_run.render());
            }

            QueryRunner queryRunner2 = new QueryRunner();
            STGroup sqlQueriesgroup2 = new STGroupFile(config_path + File.separator + "temp_datamart_queries.stg", '$', '$');
            BufferedReader reader2 = new BufferedReader(new FileReader(config_path + File.separator + "temp_datamart_template_name.txt"));
            while ((currentLine = reader2.readLine()) != null) {
                ST template_run2 = sqlQueriesgroup2.getInstanceOf(currentLine);
                System.out.println(template_run2.render());
                queryRunner2.update(target_Connection, template_run2.render());
            }

            QueryRunner queryRunner1 = new QueryRunner();
            STGroup sqlQueriesgroup1 = new STGroupFile(config_path + File.separator + "datamart_queries.stg", '$', '$');
            BufferedReader reader1 = new BufferedReader(new FileReader(config_path + File.separator + "datamart_template_name.txt"));
            while ((currentLine = reader1.readLine()) != null) {
                ST template_run1 = sqlQueriesgroup1.getInstanceOf(currentLine);
                System.out.println(template_run1.render());
                queryRunner1.update(target_Connection, template_run1.render());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            target_Connection.close();
        }

        return null;

    }

    public void datamart_agg(String config_path, Connection target_Connection) throws SQLException {

        try {
            QueryRunner queryRunner = new QueryRunner();
            STGroup sqlQueriesgroup = new STGroupFile(config_path + File.separator + "datamart_agg_queries.stg", '$', '$');
            BufferedReader reader = new BufferedReader(new FileReader(config_path + File.separator + "datamart_agg_template_name.txt"));
            String currentLine = null;

            while ((currentLine = reader.readLine()) != null) {
                ST template_run = sqlQueriesgroup.getInstanceOf(currentLine);
                System.out.println(template_run.render());
                queryRunner.update(target_Connection, template_run.render());
            }
        } catch (Exception e) {
            System.out.println(e);
        }finally {
            target_Connection.close();
        }
    }
}
