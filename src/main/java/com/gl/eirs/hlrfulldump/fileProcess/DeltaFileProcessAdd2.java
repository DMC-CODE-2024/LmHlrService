package com.gl.eirs.hlrfulldump.fileProcess;


import com.gl.eirs.hlrfulldump.alert.AlertDto;
import com.gl.eirs.hlrfulldump.alert.AlertManagement;
import com.gl.eirs.hlrfulldump.audit.AuditManagement;
import com.gl.eirs.hlrfulldump.configuration.AppConfig;
//import com.gl.eirs.hlrfulldump.configuration.ProcessConfiguration;
//import com.gl.eirs.hlrfulldump.pgmDao.PGMDao;
import com.gl.eirs.hlrfulldump.dto.FileDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

@Component
public class DeltaFileProcessAdd2 {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    @Autowired
    AlertManagement alertManagement;
    @Autowired
    AuditManagement auditManagement;

    private static String moduleName = "HLR_Full_Dump";
    private static String featureName = "HLR_Full_Dump_Processor";
    private static long executionFinishTime;
    private static long executionFinalTime;


    @Autowired
    AppConfig appConfig;
    public boolean deltaFileProcess(final Connection conn, final long executionStartTime, String addFileName,
                                 String operator, FileDto addFileDto) throws Exception {

        logger.info("Inside delta file addition process function.");
        Integer batchCount = appConfig.getBatchCount();
        String fileSeparator = appConfig.getFileSeparator();

        String deltaFilePath = appConfig.getDeltaFilePath();


        moduleName = moduleName+"_"+operator;
        LocalDateTime dateObj = LocalDateTime.now();
        File hlrDeltaFile = new File(deltaFilePath + addFileName);
        long addFileCount = 0;
        long failureCount=0;
        final String addInHlr = "INSERT INTO app.active_msisdn_list (imsi, msisdn, activation_date, operator, remark)" +
                "VALUES(?,?,?,?,?)";


        ArrayList<String> sqlQueries = new ArrayList<>();
        logger.info("Starting to read the delta file addition for processing.");
        int i = 0;

        conn.setAutoCommit(false);

        int line = 0;
        try(BufferedReader reader = new BufferedReader(new FileReader(hlrDeltaFile));
            PreparedStatement addInHlrSt = conn.prepareStatement(addInHlr);
        ) {
            String nextLine;
            while ((nextLine = reader.readLine()) != null) {
                if (nextLine.isEmpty()) {
                    continue;
                }

                String[] hlrRecord = nextLine.split(fileSeparator, -1);
                logger.info("HLR record is {}", (Object) Arrays.stream(hlrRecord).toArray());
                String imsi = hlrRecord[0].trim();
                String msisdn = hlrRecord[1].trim();
                String activationDate = hlrRecord[2].trim();
                String remarks = "Hlr Full Dump";
                if(activationDate.isBlank()) {
                    activationDate = null;
                }

                if (imsi.equalsIgnoreCase("IMSI") || msisdn.equalsIgnoreCase("MSISDN")) continue;
                addInHlrSt.setString(1, imsi);
                addInHlrSt.setString(2, msisdn);
                addInHlrSt.setString(3, activationDate);
                addInHlrSt.setString(4, operator);
                addInHlrSt.setString(5, remarks);

                addInHlrSt.addBatch();
                sqlQueries.add("INSERT INTO app.active_msisdn_list (imsi, msisdn, activation_date, operator, remark) VALUES (" + imsi + "," + msisdn + "," + activationDate + "," + operator + "," + remarks + ")");
                logger.info("Query added to batch for insert: INSERT INTO app.active_msisdn_list (imsi, msisdn, activation_date, operator, remark) VALUES({}, {}, {}, {}, {})", imsi, msisdn, activationDate, operator, remarks);
                line++;
                if (line % batchCount == 0) {
                    logger.info("Executing batch statements for addition {} entries.", batchCount);
                    try {
                        int[] addInDev = addInHlrSt.executeBatch();
                        logger.info("Total entries processed for insert {}", line);
                        for (int m=0;m<addInDev.length;m++) {
                            if (addInDev[i] == 0) {
                                logger.error("Insert statement to create a record in active_msisdn_list table failed.");
                                logger.error("The record is " + sqlQueries.get(m));
                                failureCount++;
                            } else addFileCount++;
                        }
                        conn.commit();
                    } catch (BatchUpdateException e) {
//                        alertManagement.raiseAnAlert("alert5215", addFileName, operator, 0);
                        logger.error("Insert statement to create a record in active_msisdn_list table failed for this batch." + e.getLocalizedMessage());
                        int cnt[] = e.getUpdateCounts();
                        for(int j=0;j<cnt.length;j++) {
                            if(cnt[j] <= 0) {
                                logger.error("The query failed is " + sqlQueries.get(j));
                                failureCount++;
                            }
                        }
                    }
                    sqlQueries.removeAll(sqlQueries);
                }

            }

            if (line % batchCount != 0) {
                logger.info("Executing batch statements for insert {} entries.", line);
                try {
                    int[] addInDev = addInHlrSt.executeBatch();
                    conn.commit();
                    logger.info("Total entries processed for insert {}", line);
                    for (int m=0;m<addInDev.length;m++) {
                        if (addInDev[m] == 0) {
                            logger.error("Insert statement to create a record in active_msisdn_list table failed.");
                            logger.error("The records are" + sqlQueries.get(m));
                            failureCount++;
                        } else addFileCount++;
                    }
                } catch (BatchUpdateException e) {
//                    alertManagement.raiseAnAlert("alert5215", addFileName, operator, 0);
                    logger.error("Insert statement to create a record in active_msisdn_list table failed for insert status for this batch." + e.getLocalizedMessage());
                    logger.error(String.valueOf(sqlQueries));
                    int cnt[] = e.getUpdateCounts();
                    for(int j=0;j<cnt.length;j++) {
                        if(cnt[j] <= 0) {
                            logger.error("The query failed is " + sqlQueries.get(j));
                            failureCount++;
                        }
                    }
                }

            }

            conn.setAutoCommit(true);
            addFileDto.setSuccessRecords(addFileCount);
            addFileDto.setFailedRecords(failureCount);
            return true;
        } catch (Exception exception) {

            logger.error("Exception {} in the add file processing" + exception.getMessage());
            return false;

        }
    }

}
