package com.gl.eirs.hlrfulldump.fileProcess;

import com.gl.eirs.hlrfulldump.alert.AlertManagement;
import com.gl.eirs.hlrfulldump.audit.AuditManagement;
import com.gl.eirs.hlrfulldump.configuration.AppConfig;
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
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class DeltaFileProcessDel2 {

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

    public boolean deltaFileProcess(final Connection conn, final long executionStartTime, String delFileName,
                                 String operator, FileDto delFileDto) throws Exception {
        Integer batchCount = appConfig.getBatchCount();
        String fileSeparator = appConfig.getFileSeparator();
        String deltaFilePath = appConfig.getDeltaFilePath();

        logger.info("Inside delta file deletion process function.");
        moduleName = moduleName + "_" + operator;
        LocalDateTime dateObj = LocalDateTime.now();
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        File hlrDeltaFile = new File(deltaFilePath + delFileName);
        final String selectInHlr = "SELECT * from app.active_msisdn_list where msisdn = ? ";
        final String delInHlr = "DELETE FROM app.active_msisdn_list WHERE msisdn = ? ";
        final String insertInHis = "INSERT INTO app.active_msisdn_list_his (imsi, msisdn, operator, remark, operation,activation_date) VALUES (?, ?, ?, ?, 0,?)";
        ArrayList<String> sqlQueries = new ArrayList<>();
        ArrayList<String> deleteQueries = new ArrayList<>();
        logger.info("Starting to read the delta file deletion for processing.");
        int line = 0;
        long delFileCount = 0;
        long failureCount = 0;
        conn.setAutoCommit(false);

        List<String[]> hlrRecords = new ArrayList<>();
        String remarks = "Hlr Full Dump";

        try (BufferedReader reader = new BufferedReader(new FileReader(hlrDeltaFile));
             PreparedStatement delInHlrSt = conn.prepareStatement(delInHlr);
             PreparedStatement insertInHisSt = conn.prepareStatement(insertInHis);
             PreparedStatement selectInHlrSt =conn.prepareStatement(selectInHlr)) {

            String nextLine;
            String imsi="", msisdn="", activationDate="";
            String oldImsi = "", oldMsisdn = "", oldOperator = "", oldActivationDate ="";
            while ((nextLine = reader.readLine()) != null) {

                if (nextLine.isEmpty()) {
                    continue;
                }
                String[] hlrRecord = nextLine.split(fileSeparator, -1);
                logger.info("The record is {}", (Object) Arrays.stream(hlrRecord).toArray());
                imsi = hlrRecord[0].trim();
                msisdn = hlrRecord[1].trim();
                activationDate = hlrRecord[2].trim();


                if (imsi.equalsIgnoreCase("IMSI") || msisdn.equalsIgnoreCase("MSISDN")) continue;
                boolean flag=false;
                selectInHlrSt.setString(1, msisdn);
                try (ResultSet rs = selectInHlrSt.executeQuery()) {
                    while (rs.next()) {
                        logger.info("Entry with msisdn exists in active_msisdn_list table {}", msisdn);
                        oldImsi = rs.getString("imsi");
                        oldMsisdn = rs.getString("msisdn");
                        oldOperator = rs.getString("operator");
                        oldActivationDate = rs.getString("activation_date");
                        flag = true;
                    }
                }
                // delete only when flag=1;
                hlrRecords.add(hlrRecord);


                if (flag == true) {


                    delInHlrSt.setString(1, msisdn);
//                    delInHlrSt.setString(2, imsi);
                    delInHlrSt.addBatch();
//                sqlQueries.add("")
                    deleteQueries.add("DELETE FROM app.active_msisdn_list where msisdn = " + msisdn);
                    logger.info("Query added to batch for delete: DELETE FROM app.active_msisdn_list where msisdn = {}", msisdn);
                    line++;
                    if (line % batchCount == 0) {
                        logger.info("Executing batch statements for deletion {} entries.", batchCount);

                        try {
                            int[] delInDev = delInHlrSt.executeBatch();
//                        logger.info("The del statement is {}", Arrays.stream(delInDev).toArray());
                            logger.info("Total entries processed for delete {}", line);
                            for (int k = 0; k < delInDev.length; k++) {
                                if (delInDev[k] == 0) {
                                    logger.error("Delete statement to delete a record in active_msisdn_table table failed.");
                                    logger.error("The record is " + deleteQueries.get(k));
                                    failureCount++;
                                } else {
                                    delFileCount++;
                                    logger.info("Adding entry in active_msisdn_list_his");
                                    String[] record = hlrRecords.get(k);
                                    insertInHisSt.setString(1, oldImsi);
                                    insertInHisSt.setString(2, oldMsisdn);
                                    insertInHisSt.setString(3, oldOperator);
                                    insertInHisSt.setString(4, remarks);
                                    insertInHisSt.setString(5, oldActivationDate);
                                    insertInHisSt.addBatch();
                                    sqlQueries.add("INSERT INTO app.active_msisdn_list_his (imsi, msisdn, operator, remark, operation, activation_date) VALUES (" + record[0].trim() + ", " + record[1].trim() + ", " + operator + ", " + remarks + ", 0 , " + record[2].trim() + ")");
                                    logger.info("Query added to batch for insert: INSERT INTO app.active_msisdn_list_his (imsi, msisdn, operator, remark, operation, activation_date) VALUES ({},{},{},{},{},{})", record[0].trim(), record[1].trim(), operator, remarks, 0, record[2].trim());
                                    insertInHisSt.executeBatch();
                                    logger.info("Inserting success deleted entries in history table");
                                }
                            }
                            conn.commit();
                        } catch (BatchUpdateException e) {
//                        alertManagement.raiseAnAlert("alert5215", delFileName, operator, 0);
                            logger.error("Delete statement to delete in active_msisdn_list failed for this batch." + e.getLocalizedMessage());
                            int[] cnt = e.getUpdateCounts();
                            for (int j = 0; j < cnt.length; j++) {
                                if (cnt[j] <= 0) {
                                    logger.error("The query failed is " + deleteQueries.get(j));
                                    failureCount++;
                                }
                            }
                        }
                        sqlQueries.clear();
                        hlrRecords.clear();
                        deleteQueries.clear();
                    }
                } else {
                    logger.info("Entry with msisdn {} does not exists in active_msisdn_list table", msisdn);
                }
            }

            if (line % batchCount != 0) {
                logger.info("Executing batch statements for deletion {} entries.", line);
                try {
                    int[] delInDev = delInHlrSt.executeBatch();
                    logger.info("Total entries processed for delete {}", line);
                    for (int k=0;k<delInDev.length;k++) {
                        if (delInDev[k] == 0) {
                            logger.error("Delete statement to delete a record in active_msisdn_table table failed.");
                            logger.error("The record is " + hlrRecords.get(k));
                            failureCount++;
                        }
                        else {
                            delFileCount++;
                            String[] record = hlrRecords.get(k);
                            insertInHisSt.setString(1, record[0].trim());
                            insertInHisSt.setString(2, record[1].trim());
                            insertInHisSt.setString(3, operator);
                            insertInHisSt.setString(4, "Hlr Full Dump");
                            insertInHisSt.setString(5, record[2].trim());
                            insertInHisSt.addBatch();
                            sqlQueries.add("INSERT INTO app.active_msisdn_list_his (imsi, msisdn, operator, remark, operation, activation_date) VALUES (" + record[0].trim() + ", " + record[1].trim() + ", " + operator + ", " + remarks + ", 0 , " + record[2].trim() + ")");
                            logger.info("Query added to batch for insert: INSERT INTO app.active_msisdn_list_his (imsi, msisdn, operator, remark, operation, activation_date) VALUES ({},{},{},{},{},{})" , record[0].trim(), record[1].trim(), operator, remarks, 0, record[2].trim());
                            insertInHisSt.executeBatch();
                            logger.info("Inserting success deleted entries in history table");
                        }
                    }
                    conn.commit();
                } catch (BatchUpdateException e) {
//                    alertManagement.raiseAnAlert("alert5215", delFileName, operator, 0);
                    logger.error("Delete statement to delete a record in active_msisdn_list table failed for this batch." + e.getLocalizedMessage());
                    int[] cnt = e.getUpdateCounts();
                    for (int j = 0; j < cnt.length; j++) {
                        if (cnt[j] <= 0) {
                            logger.error("The query failed is " + sqlQueries.get(j));
                            failureCount++;
                        }
                    }
                }
            }
            conn.setAutoCommit(true);
            delFileDto.setFailedRecords(failureCount);
            delFileDto.setSuccessRecords(delFileCount);
            return  true;
        } catch (Exception exception) {
            logger.error("Exception {} in the delete file processing " + exception.getMessage());
            return false;
//            final Date finishDate = new Date();
//            executionFinishTime = finishDate.getTime();
//            executionFinalTime = executionFinishTime - executionStartTime;
//            logger.info("Execution finish time " + executionFinalTime);
//            long insertCount = 0;
//            long deletedCount = delFileCount;
//            alertManagement.raiseAnAlert("alert5202", exception.getMessage(), operator, 0);
//            auditManagement.updateAudit(501, "FAIL", featureName, moduleName, insertCount, "",
//                    executionFinalTime, deletedCount, failureCount, "The diff file processing failed for file " + delFileName, conn);
//            System.exit(1);
        }
    }
}
