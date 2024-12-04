package com.gl.eirs.hlrfulldump;


import com.gl.eirs.hlrfulldump.configuration.AppConfig;
import com.gl.eirs.hlrfulldump.connection.MySQLConnection;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@EnableEncryptableProperties
@SpringBootApplication
//@ComponentScan("com.gl.eirs.hlrfulldump")
public class HlrFullDumpApplication implements CommandLineRunner {
    @Autowired
    HlrDumpProcessorMain hlrDumpProcessorMain;

    @Autowired
    MySQLConnection connection;

    @Autowired
    AppConfig appConfig;


    public static void main(String[] args) {
        SpringApplication.run(HlrFullDumpApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        if (args.length == 4) {
            try {
                int intParam = Integer.parseInt(args[1]);
                String addFileName;
                String delFileName;
                String operator = appConfig.getOperator();
                if (intParam == 1) {
                    // Take filenames from command line arguments
                    addFileName = args[2];
                    delFileName = args[3];
                } else {
                    // Generate filenames based on code logic
                    LocalDateTime dateObj = LocalDateTime.now();
                    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                    addFileName = "hlr_full_dump_diff_add_" + operator + "_" + dateFormatter.format(dateObj) + ".csv";
                    delFileName = "hlr_full_dump_diff_del_" + operator + "_" + dateFormatter.format(dateObj) + ".csv";
                }
                hlrDumpProcessorMain.startFunction(intParam, addFileName, delFileName);
                // Call the method to fill missing IMSI and MSISDN


            } catch (NumberFormatException e) {
                System.err.println("Invalid value for intParam. Please provide an integer.");
            }
        } else {
            System.err.println("Usage: java HlrDumpProcessorMain <intParam> <addFileName> <delFileName>");
        }
    }
}
