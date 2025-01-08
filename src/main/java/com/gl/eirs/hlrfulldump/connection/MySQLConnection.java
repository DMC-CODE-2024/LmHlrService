package com.gl.eirs.hlrfulldump.connection;

import com.gl.eirs.hlrfulldump.configuration.AppConfig;
//import com.gl.eirs.hlrfulldump.configuration.CommonConfiguration;
//import com.gl.eirs.hlrfulldump.configuration.ProcessConfiguration;
import org.jasypt.util.text.BasicTextEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


@Component
public class MySQLConnection {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    AppConfig appConfig;
    public Connection getConnection() throws Exception {



        Connection conn = null;
        try {

//            final String JDBC_DRIVER = CommonConfiguration.getProperty("jdbc_driver").trim();
            String JDBC_DRIVER = appConfig.getJdbcDriver();

            String DB_URL = appConfig.getDbUrl();

            String USER = appConfig.getDbUsername();

            String PASS =    appConfig.getSpringDatasourcePassword();
            logger.info("Connection  Init " + java.time.LocalDateTime.now());
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, decryptor(PASS));
            logger.info("Connection created successfully " + conn + " .. " + java.time.LocalDateTime.now());
            return conn;
        } catch (Exception e) {
            logger.error(" Error : : " + e + " :  " + java.time.LocalDateTime.now() );
            System.exit(100);
            try {
                conn.close();
            } catch (SQLException ex) {
                logger.error(" SQLException : " + ex + " :  " + java.time.LocalDateTime.now());
            }
            System.exit(100);
            return null;
        }
    }

    public static String decryptor(String encryptedText) {
        BasicTextEncryptor encryptor = new BasicTextEncryptor();
        encryptor.setPassword(System.getenv("JASYPT_ENCRYPTOR_PASSWORD"));
        return encryptor.decrypt(encryptedText);
    }
    
//    String getPassword(final String passwordDecryptor) {
//        String passwordDecryptorNew =passwordDecryptor.replace("${APP_HOME}", System.getenv("APP_HOME"));
//        logger.info("Decrypting Password");
//        String pass = appConfig.getSpringDatasourcePassword();
//        String line = null;
//        String response = null;
//        try {
//            String cmd = "java -jar " + passwordDecryptorNew + " spring.datasource.psa" + pass;
//            logger.debug("cmd to  run::" + cmd);
//            Process pro = Runtime.getRuntime().exec(cmd);
//            BufferedReader in = new BufferedReader(
//                    new InputStreamReader(pro.getInputStream()));
//            while ((line = in.readLine()) != null) {
//                logger.info("Response::" + line);
//                response = line;
//            }
//            return response;
//        } catch (Exception e) {
//            logger.info("Error  getPassword " + e);
//            e.printStackTrace();
//            return null;
//        }
//    }
}
