package com.awss3.sample;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class S3Sample {
    public static void main(String[] args) throws IOException{
        // new AmazonS3Client() is depracated.
        AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTHEAST_1)
                .build();

        try{
            // list all buckets
            for (Bucket item: s3.listBuckets()){
                System.out.println(" = " + item.getName());
            }

            final String bucketName = "deanzhao";
            // create the bucket
            if(!s3.doesBucketExist(bucketName)  ) {
                s3.createBucket(bucketName);
            }

            // create a file on S3
            final String key = "MyObjectkey" + UUID.randomUUID();
            s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

            // download the file
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            displayTextInputStream(object.getObjectContent());

        } finally {

        }
    }

    private static File createSampleFile() throws IOException{
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("hahahaha\n");
        writer.write("fasdfasdfad\n");
        writer.close();

        return file;
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }
}
