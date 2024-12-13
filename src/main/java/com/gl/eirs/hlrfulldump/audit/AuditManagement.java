package com.gl.eirs.hlrfulldump.audit;

import com.gl.eirs.hlrfulldump.hlrDao.AuditDao;
import org.springframework.stereotype.Service;

import java.sql.Connection;

@Service
public class AuditManagement {

    final private AuditDao auditDao;
//    final private ConfigurationDao configurationDao;

    public AuditManagement() {
        auditDao = new AuditDao();
//        configurationDao = new ConfigurationDao();
    }
//    public Date getExpectedDate(final Connection conn) throws SQLException {
////        Date date = auditDao.getExpectedDate(conn);
//        return date;
//    }

    public void createAudit(final int statusCode, final int executionTime,
            final String featureName, final Connection connection, String modulename) throws Exception {

//        String initialMessage = configurationDao.fetchConfigMessage(tag, connection);
        auditDao.saveInAuditTable(statusCode, "INITIAL", "NA", featureName, connection, executionTime, modulename);
    }

    public void updateAudit(final int statusCode, final String status, final String featureName, final String moduleName,
                            final long numberOfRecords, final String info, final long executionTime,
                            final long updatedCount, final long failureCount,
                            final String errorMsg, final Connection connection) throws ClassNotFoundException {
//        String errorMsg = configurationDao.fetchConfigMessage(tag, connection);
        auditDao.updateInAuditTable(statusCode, status, errorMsg, numberOfRecords, info, moduleName, featureName,
                connection, executionTime, updatedCount,failureCount);
    }

    public void updateAudit(final int statusCode, long numberOfRecords, final String info, final long executionTime,
                            final long updatedCount, final long failureCount, String errMsg, String moduleName, String featureName, String status,final Connection connection) throws ClassNotFoundException {
//        String successMsg = configurationDao.fetchConfigMessage(tag, connection);
        if (moduleName.equalsIgnoreCase("HLR_Full_Dump_ST")) numberOfRecords = numberOfRecords - 2;
        auditDao.updateInAuditTable(statusCode, status, errMsg, numberOfRecords, info, moduleName,featureName,connection, executionTime, updatedCount, failureCount);
    }
}
