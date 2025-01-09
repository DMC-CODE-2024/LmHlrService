package com.gl.eirs.hlrfulldump.validation;

import com.gl.eirs.hlrfulldump.alert.AlertManagement;
import com.gl.eirs.hlrfulldump.audit.AuditManagement;
import com.gl.eirs.hlrfulldump.configuration.AppConfig;
//import com.gl.eirs.hlrfulldump.configuration.ProcessConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;


@Component
public class ConfigValidation {

//    private static String hlrErrorFilePath = ProcessConfiguration.getProperty("hlrErrorFilePath");
//    final private static String retryCount = ProcessConfiguration.getProperty("retryCount");
//    final private static String batchCount = ProcessConfiguration.getProperty("batchCount");
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static String moduleName = "HLR_Full_Dump";
    private static String featureName = "HLR_Full_Dump_Processor";
    private static long executionFinishTime;
    private static long executionFinalTime;
    private Date dateFinish = new Date();

    @Autowired
    AppConfig appConfig;


    private boolean isDirectoryValidation(final String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return false;
        }

        final Path path = Paths.get(filePath);
        return Files.exists(path) && Files.isDirectory(path);
    }

    private boolean isNumber(final String configKey, final String configValue) {
        try {
            int x = Integer.parseInt(configValue);
            return true;
        } catch (NumberFormatException exception) {
            logger.error("Value for {} is not a valid integer. The value is {}, it should be a valid integer.", configKey, configValue);
            return false;
        }
    }

    public boolean configValidation(final AlertManagement alertManagement, final AuditManagement auditManagement,
                                    final long executionStartTime, final Connection connection) throws Exception {

        LocalDate dateObj = LocalDate.now();
        int batchCount = appConfig.getBatchCount();
        String operator = appConfig.getOperator();
        moduleName = moduleName+"_"+operator;
        final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
//        hlrErrorFilePath =hlrErrorFilePath.replace("${DATA_HOME}", System.getenv("DATA_HOME"));
//        if(!isDirectoryValidation(hlrErrorFilePath)) {
//            logger.error("The config value for hlrErrorFilePath is not correct.");
//            this.dateFinish = new Date();
//            this.executionFinishTime = this.dateFinish.getTime();
//            this.executionFinalTime = this.executionFinishTime - executionStartTime;
//
//            auditManagement.updateAudit(501, "FAIL", "HLR Dump File Process", 0, "HLR Dump File Processor",
//                    "Validation Check", this.executionFinalTime, 0, 0, "hlrErrorFilePathNotFound", connection);
////            String msg = alertManagement.alertMessage("alert1101", connection);
//            alertManagement.raiseAnAlert("alert1101", "", "HLR Dump File Process", 0);
//            return false;
//        }
//
//        if(!isNumber("retryCount", retryCount)) {
//            this.dateFinish = new Date();
//            this.executionFinishTime = this.dateFinish.getTime();
//            this.executionFinalTime = this.executionFinishTime - executionStartTime;
//
//            auditManagement.updateAudit(501, "FAIL", featureName, moduleName, 0, "",
//                    executionFinalTime, 0, 0, "The value of retry count is not an integer " + retryCount, connection);
//
////            String msg = alertManagement.alertMessage("alert1102", connection);
//            alertManagement.raiseAnAlert("alert5213", retryCount, operator, 0);
//            return false;
//        }
        if(!isNumber("batchCount", String.valueOf(batchCount))) {
            this.dateFinish = new Date();
            this.executionFinishTime = this.dateFinish.getTime();
            this.executionFinalTime = this.executionFinishTime - executionStartTime;

            auditManagement.updateAudit(501, "FAIL", featureName, moduleName, 0, "",
                    executionFinalTime, 0, 0, "The value of batch count is not an integer " + batchCount, connection);

//            String msg = alertManagement.alertMessage("alert1103", connection);
            alertManagement.raiseAnAlert("alert5214", String.valueOf(batchCount), operator, 0);
            return false;
        }
        return true;
    }


}
