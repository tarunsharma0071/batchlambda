package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Configuration
@EnableAutoConfiguration
@EnableBatchProcessing
@Log4j2
public class BatchDemoApplication implements RequestStreamHandler {


    public static void main(String[] args) {
        SpringApplication.run(BatchDemoApplication.class, args);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Setter
    public static class Patient {
        private int age;
        private String name;
        private String email;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Setter
    public static class User {

        private String email;
        private String password;
    }

    
      @Bean FlatFileItemReader<Patient> itemReader(@Value("classpath:./patient.csv") Resource in) {
      
      return new FlatFileItemReaderBuilder<Patient>().resource(in).name("file-reader").targetType(Patient.class).delimited() .delimiter(",").names("name",
      "age", "email").build(); }
     

    
     @Bean JdbcBatchItemWriter<Patient> itemWriter(DataSource dataSource) {
     
     return new JdbcBatchItemWriterBuilder<Patient>() .dataSource(dataSource) .sql("insert into PATIENT( name, age, email) values (:name, :age, :email)"
     ).beanMapped() .build(); }
    

    @Bean
    Job job(JobBuilderFactory jbf, StepBuilderFactory sbf, ItemReader<? extends Patient> ir, ItemWriter<? super Patient> iw) {

        Step step1 = sbf.get("File-To-Patient")
                .<Patient, Patient> chunk(4)
                .reader(ir)
                .writer(iw).build();

        return jbf.get("etl").incrementer(new RunIdIncrementer())
                .start(step1).build();
    }
    
    /*@Bean
    ItemReader<Patient> jdbcReader(DataSource dataSource) {

        return new JdbcCursorItemReaderBuilder<Patient>().dataSource(dataSource).name("jdbc-reader")
                .sql("select * from PATIENT").rowMapper(new RowMapper<BatchDemoApplication.Patient>() {

                    @Override
                    public Patient mapRow(ResultSet rs, int i) throws SQLException {
                        Patient patient = new Patient();
                        patient.setAge(rs.getInt("age"));
                        patient.setName(rs.getString("name"));
                        patient.setEmail(rs.getString("email"));
                        return patient;
                    }
                }).build();
    }

    @Bean
    JdbcBatchItemWriter<User> itemWriter(DataSource dataSource) {

        return new JdbcBatchItemWriterBuilder<User>().dataSource(dataSource)
                .sql("insert into USER( email, password) values (:email, :password)").beanMapped().build();
    }

    @Bean
    Job job(JobBuilderFactory jbf, StepBuilderFactory sbf, ItemReader<? extends Patient> ir, ItemWriter<? super User> iw) {

        Step step1 = sbf.get("Patient-To-User DB")
                .<Patient, User> chunk(4)
                .reader(ir).processor(new ItemProcessor<Patient, User>() {

                    @Override
                    public User process(Patient patient) throws Exception {
                        User user = new User();
                        user.setEmail(patient.getEmail());
                        user.setPassword(UUID.randomUUID().toString());
                        return user;
                    }
                }).writer(iw).build();

        return jbf.get("etl").incrementer(new RunIdIncrementer())
                .start(step1).build();
    }*/

    @Override
    public void handleRequest(InputStream arg0, OutputStream arg1, Context context) throws IOException {
        log.info("Batch process started");
        this.main(new String[] {});
        log.info("Batch process completed");
    }
}
