package com.gl.eirs.hlrfulldump;

import com.gl.eirs.hlrfulldump.alert.AlertManagement;
import com.gl.eirs.hlrfulldump.audit.AuditManagement;
import com.gl.eirs.hlrfulldump.configuration.AppConfig;
import com.gl.eirs.hlrfulldump.connection.MySQLConnection;
import com.gl.eirs.hlrfulldump.dto.FileDto;
import com.gl.eirs.hlrfulldump.fileProcess.DeltaFileProcessAdd2;
import com.gl.eirs.hlrfulldump.fileProcess.DeltaFileProcessDel2;
import com.gl.eirs.hlrfulldump.validation.ConfigValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

@Component
@Service
public class HlrDumpProcessorMain {

    @Autowired
    MySQLConnection connection;

    @Autowired
    ConfigValidation configValidation;

    @Autowired
    AlertManagement alertManagement;

    @Autowired
    AuditManagement auditManagement;

    @Autowired
    DeltaFileProcessAdd2 deltaFileProcessAdd2;

    @Autowired
    DeltaFileProcessDel2 deltaFileProcessDel2;

    @Autowired
    AppConfig appConfig;

    @Autowired
    FileDto fileDto;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static long executionFinishTime;
    private static long executionFinalTime;

    private static String moduleName = "HLR_Full_Dump";
    private static String featureName = "HLR_Full_Dump_Processor";

    public void startFunction(int intParam, String addFileName, String delFileName) throws Exception {
       String operator = appConfig.getOperator();

        if (intParam == 1) {
            // Process files based on the provided file names
            logger.info("Processing files provided by the user:");
            logger.info("Add file name: {}", addFileName);
            logger.info("Delete file name: {}", delFileName);
        } else if (intParam == 0) {
            // Use default file names or logic
            logger.info("Processing files using default logic or file names.");
        } else {
            logger.info("Invalid value for intParam. Please provide 0 or 1.");
            return;
        }

        moduleName = moduleName + "_" + operator;
        try ( Connection conn = connection.getConnection();)  {
            final Date date = new Date();
            final long executionStartTime = date.getTime();
            logger.info("Java process execution Start Time = " + executionStartTime);

            // creating an entry in audit table.
            logger.info("Making an entry in audit table indicate start of HLR Dump Process.");
            auditManagement.createAudit(201, 0, featureName, conn, moduleName);

            // do config validation
            boolean configValidationStatus = configValidation.configValidation(alertManagement, auditManagement, executionStartTime, conn);
            if (!configValidationStatus) {
                System.exit(-1);
            }
            LocalDateTime dateObj = LocalDateTime.now();
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            FileDto addFileDto = new FileDto(addFileName, appConfig.getDeltaFilePath());
            FileDto delFileDto = new FileDto(delFileName, appConfig.getDeltaFilePath());
            Date dateFinish = new Date();
            logger.info("Starting the process of reading the deletion diff file.");
            boolean deleteFileStatus = deltaFileProcessDel2.deltaFileProcess(conn, executionStartTime, delFileName, operator, delFileDto);
            Date finishDate = new Date();
            executionFinishTime = finishDate.getTime();
            executionFinalTime = executionFinishTime - executionStartTime;
            if(!deleteFileStatus) {
                alertManagement.raiseAnAlert("alert5202", operator, "", 0);
                auditManagement.updateAudit(501, "FAIL", featureName, moduleName, delFileDto.getTotalRecords() + addFileDto.getTotalRecords(),"",
                        executionFinalTime, delFileDto.getSuccessRecords() + addFileDto.getSuccessRecords(), delFileDto.getFailedRecords() + addFileDto.getFailedRecords(), "The diff file processing failed for file " + addFileName, conn);
                System.exit(1);
            }
            finishDate = new Date();
            executionFinishTime = finishDate.getTime();
            executionFinalTime = executionFinishTime - executionStartTime;
            logger.info("Execution finish time for deletion file" + executionFinalTime);

            logger.info("Starting the process of diff of insertion file.");

            boolean addFileStatus = deltaFileProcessAdd2.deltaFileProcess(conn, executionStartTime, addFileName, operator, addFileDto);

            logger.info("Execution finish for time addition file " + executionFinalTime);
            if(!addFileStatus) {
                alertManagement.raiseAnAlert("alert5202", operator, "", 0);
                auditManagement.updateAudit(501, "FAIL", featureName, moduleName, delFileDto.getTotalRecords() + addFileDto.getTotalRecords(),"",
                    executionFinalTime, delFileDto.getSuccessRecords() + addFileDto.getSuccessRecords(), delFileDto.getFailedRecords() + addFileDto.getFailedRecords(), "The diff file processing failed for file " + addFileName,connection.getConnection());
                connection.getConnection().close();
                System.exit(1);
            }



//            File addHlrDeltaFile = new File(deltaFilePath + addFileName);
//            File delHlrDeltaFile = new File(deltaFilePath + delFileName);
//
//            Path addFile = Paths.get(addHlrDeltaFile.toURI());
//            Path delFile = Paths.get(delHlrDeltaFile.toURI());
//            long insertCount = Files.lines(addFile).count();
//            long deletedCount = Files.lines(delFile).count();


//            long failureCount = 0;
            auditManagement.updateAudit(200, addFileDto.getTotalRecords() + delFileDto.getTotalRecords(), "", executionFinalTime, addFileDto.getSuccessRecords()+delFileDto.getSuccessRecords(), addFileDto.getFailedRecords() + delFileDto.getFailedRecords(), "NA", moduleName, featureName, "SUCCESS", conn);
            logger.info("File Processing is successfully completed.");

            // Call the method to fill missing IMSI and MSISDN
            logger.info("Calling fillMissingImsiMsisdn method.");
            fillMissingImsiMsisdn(conn);
            logger.info("fillMissingImsiMsisdn method completed successfully.");
        } catch (Exception exception) {
            logger.error(String.valueOf(exception));
            alertManagement.raiseAnAlert("alert5202", exception.getMessage(), operator, 0);
            auditManagement.updateAudit(501, "FAIL", featureName, moduleName, 0, "",
                    executionFinalTime, 0, 0, "The process exited with exception" + exception.getMessage() + " for operator" + operator + ".", connection.getConnection());
           connection.getConnection().close();
            System.exit(1);
        }
    }

    public void fillMissingImsiMsisdn(Connection conn) throws SQLException {
        logger.info("Starting to fill missing IMSI and MSISDN values for imei_pair_detail table and duplicate_device_detail table");
        String queryForImeiPair = "SELECT id, imsi, msisdn FROM app.imei_pair_detail WHERE imsi IS NULL OR imsi = '' OR msisdn IS NULL OR msisdn = ''";
        String queryForDeviceDuplicate = "SELECT id, imsi, msisdn FROM app.duplicate_device_detail WHERE imsi IS NULL OR imsi = '' OR msisdn IS NULL OR msisdn = ''";
        String updateQuery = "UPDATE {table} SET imsi = ?, msisdn = ? WHERE id = ?";

        fillImsiMsisdnForTable(conn, queryForImeiPair, "app.imei_pair_detail", updateQuery);
        fillImsiMsisdnForTable(conn, queryForDeviceDuplicate, "app.duplicate_device_detail", updateQuery);
        logger.info("Finished filling missing IMSI and MSISDN values for imei_pair_detail table and duplicate_device_detail table");
    }

    public void fillImsiMsisdnForTable(Connection conn, String selectQuery, String tableName, String updateQuery) throws SQLException {
        logger.info("Starting to process table: {}", tableName);
        try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery);
             PreparedStatement updateStmt = conn.prepareStatement(updateQuery.replace("{table}", tableName))) {

            ResultSet rs = selectStmt.executeQuery();
            if(rs.next() == false) {
                logger.info("No entries in {} with imsi or msisdn empty", tableName);
            }
            while (rs.next()) {
                int id = rs.getInt("id");
                String imsi = rs.getString("imsi");
                String msisdn = rs.getString("msisdn");

                logger.info("Processing record id: {}, imsi: {}, msisdn: {}", id, imsi, msisdn);

                if ((imsi == null || imsi.isEmpty()) && (msisdn != null && !msisdn.isEmpty())) {
                    logger.info("IMSI is null or empty, retrieving IMSI for MSISDN: {}", msisdn);
                    imsi = getImsiFromMsisdn(msisdn);
                    logger.info("Retrieved IMSI: {}", imsi);
                } else if ((msisdn == null || msisdn.isEmpty()) && (imsi != null && !imsi.isEmpty())) {
                    logger.info("MSISDN is null or empty, retrieving MSISDN for IMSI: {}", imsi);
                    msisdn = getMsisdnFromImsi(imsi);
                    logger.info("Retrieved MSISDN: {}", msisdn);
                }

                if (imsi != null && !imsi.isEmpty() && msisdn != null && !msisdn.isEmpty()) {
                    updateStmt.setString(1, imsi);
                    updateStmt.setString(2, msisdn);
                    updateStmt.setInt(3, id);
                    int rowsUpdated = updateStmt.executeUpdate();
                    logger.info("Updated record with id: {}. Rows affected: {}", id, rowsUpdated);
                } else {
                    logger.warn("Could not update record with id: {} because either IMSI or MSISDN is still null or empty. IMSI: {}, MSISDN: {}", id, imsi, msisdn);
                }
            }
        } catch (SQLException e) {
            logger.error("Error processing table: " + tableName, e);
            throw e;
        }
        logger.info("Finished processing table: {}", tableName);
    }

    public String getMsisdnFromImsi(String imsi) {
        logger.info("Retrieving MSISDN for IMSI: {}", imsi);
        String msisdn = null;
        String query = "SELECT msisdn FROM app.active_msisdn_list WHERE imsi = ?";
        try (Connection conn = connection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, imsi);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    msisdn = rs.getString("msisdn");
                    logger.info("Retrieved MSISDN: {} for IMSI: {}", msisdn, imsi);
                } else {
                    logger.warn("No MSISDN found for IMSI: {}", imsi);
                }
            }
        } catch (Exception e) {
            logger.error("Error retrieving MSISDN for IMSI: " + imsi, e);
        }
        return msisdn;
    }

    public String getImsiFromMsisdn(String msisdn) {
        logger.info("Retrieving IMSI for MSISDN: {}", msisdn);
        String imsi = null;
        String query = "SELECT imsi FROM app.active_msisdn_list WHERE msisdn = ?";
        try (Connection conn = connection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, msisdn);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    imsi = rs.getString("imsi");
                    logger.info("Retrieved IMSI: {} for MSISDN: {}", imsi, msisdn);
                } else {
                    logger.warn("No IMSI found for MSISDN: {}", msisdn);
                }
            }
        } catch (Exception e) {
            logger.error("Error retrieving IMSI for MSISDN: " + msisdn, e);
        }
        return imsi;
    }

}

