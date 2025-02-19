package com.gl.eirs.hlrfulldump.hlrDao;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.gl.eirs.hlrfulldump.HlrDumpProcessorMain;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;

@Component
public class AuditDao {


    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public void saveInAuditTable(final int statusCode, final String status, final String errorMessage, final String feature,
                                 final Connection connection , final long executionFinishTime, String moduleName) throws ClassNotFoundException {

        String serverName=null;
        try {
            serverName
                    = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            logger.error("Exception in finding hostname " + e);
        }
        int count = 0;
        String query = " insert into aud.modules_audit_trail (status_code,status,error_message,feature_name,server_name,execution_time,module_name) "
                + "values('" + statusCode + "','" + status + "','" + errorMessage + "','" + feature  + "','"+serverName+"','"+executionFinishTime+"','"+moduleName+"')";
        logger.info("Insert statement to create a record in audit table = " + query);

//            try (Statement stmt = connection.createStatement();) {
//                int result=stmt.executeUpdate(query);
//                logger.info("Query execution result in audit table = "+ result);
//            } catch (SQLException exception) {
//                count++;
//                logger.error("Retry count is --" + count);
//                logger.error("Insert in audit table failed with = " + exception);
//            }

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
            var rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                HlrDumpProcessorMain.generatedKey = rs.getInt(1);
            }
            logger.info("Id for modules audit  = " + HlrDumpProcessorMain.generatedKey );
        } catch (Exception e) {
            logger.error("Unable to insert module audit: " + e.getLocalizedMessage());
        }
    }

    public void updateInAuditTable(int statusCode, String status, String errorMessage,
                                   long numberOfRecord, String info, String moduleName, String featureName, Connection conn , long executionFinishTime, long updatedCount, long failureCount) throws ClassNotFoundException {
        int count = 0;
        String query = "update  aud.modules_audit_trail set status_code='"+statusCode+"',status='"+status+"',error_message='"+errorMessage+"',info='"+info+"',count='"+numberOfRecord+"',execution_time='"+executionFinishTime+"',count2='"+updatedCount+"' , failure_count='"+failureCount+"' ,modified_on=CURRENT_TIMESTAMP where module_name='"+moduleName+"' and feature_name='"+featureName+"' and id =  "+HlrDumpProcessorMain.generatedKey;
        logger.info("Update statement to update a record in audit table = " + query);

        try (Statement stmt = conn.createStatement();) {
            int result=stmt.executeUpdate(query);
            logger.info("Query execution result to update in audit table = "+result);
        } catch (SQLException exception) {
            count++;
            logger.error("Retry count is --" + count);
            logger.error("Update statement to update in audit table failed with " + exception);
        }
    }

//    public Date getExpectedDate(final Connection conn) throws SQLException {
//        int count = 0;
//        ResultSet resultSet = null;
//        Date date = null;
//        String query = "select expected_date from aud.modules_audit_trail where feature_name LIKE '%HLR Dump Script Process' order by created_on desc limit 1";
//        logger.info("Select statement to get expected_date in audit table = " + query);
//        while (count <= retryCount) {
//            if (count == retryCount) {
//                logger.error("Retry count is exceeded for query = "+ query + " , exiting from system.");
//                System.exit(100);
//            }
//            try (Statement stmt = conn.createStatement();) {
//                resultSet = stmt.executeQuery(query);
//                logger.info("Query execution result to get expected_date from audit table = "+ resultSet);
//                if(resultSet.next()) {
//                    logger.info("Expected date "+ resultSet.getDate(1));
//                    date = resultSet.getDate(1);
//                    count = retryCount + 1;
//                }
//                else count++;
//                /* conn.commit(); */
//            } catch (SQLException exception) {
//                count++;
//                logger.error("Retry count is --" + count);
//                logger.error("Select statement to get expected_date in audit table failed with " + exception);
//            }
//        }
//        return date;
//    }

}
