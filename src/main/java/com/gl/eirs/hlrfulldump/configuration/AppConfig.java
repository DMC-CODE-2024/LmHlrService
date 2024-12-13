package com.gl.eirs.hlrfulldump.configuration;


import com.gl.eirs.hlrfulldump.HlrDumpProcessorMain;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Data
@Service
public class AppConfig {
//    @Bean
//    public HlrDumpProcessorMain hlrDumpProcessorMain() {
//        return new HlrDumpProcessorMain();
//    }

    @Value("${batch.count}")
    int batchCount;

    @Value("${file.separator.parameter}")
    String fileSeparator;

    @Value("${operator}")
    String operator;

    @Value("${input.file.path}")
    String deltaFilePath;

    @Value("${eirs.alert.url}")
    String alertUrl;


//    @Value("${password.decryptor}")
//    String passwordDecryptor;

    @Value("${spring.datasource.url}")
    String dbUrl;

    @Value("${spring.datasource.driver-class-name}")
    String jdbcDriver;

    @Value("${dbEncyptPassword}")
    String springDatasourcePassword;

    @Value("${spring.datasource.username}")
    String dbUsername;

}
