package com.awss3.sample;

import com.amazonaws.AmazonServiceException;
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
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class S3Sample {
    public static void main(String[] args) throws IOException, InterruptedException {
        // new AmazonS3Client() is depracated.
        AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTHEAST_1)
                .build();

        TransferManager transferManager = TransferManagerBuilder
                .standard()
                .withS3Client(s3)
                .withShutDownThreadPools(true)
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
            final String key = "s3putObject" + UUID.randomUUID();
            s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

            // download the file
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            displayTextInputStream(object.getObjectContent());

            // upload a single file by TransferManager
            uploadSingleFileViaTransferManager(transferManager, bucketName,"transferManagerSingleFile"+ UUID.randomUUID());

            uploadMultipleFilesViaTransferManager(transferManager, bucketName, "transferManagerMultiFile"+ UUID.randomUUID());

        } finally {
            transferManager.shutdownNow(true);
        }
    }

    private static File createSampleFile() throws IOException{
        // The most reliable way to avoid a ResetException is to provide data by using a File or FileInputStream,
        // which the AWS SDK for Java can handle without being constrained by mark and reset limits.
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

    private static void uploadSingleFileViaTransferManager(TransferManager transferManager, String bucketName, String key)
            throws IOException, InterruptedException {
        File file = createSampleFile();

        try {
            Upload xfer = transferManager.upload(bucketName, key, file);
            xfer.waitForCompletion();
            // loop with Transfer.isDone()
            //  or block with Transfer.waitForCompletion()
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        System.out.println("Single file uploaded by transfer manager");
    }

    private static void uploadMultipleFilesViaTransferManager(TransferManager transferManager, String bucketName, String key)
            throws IOException, InterruptedException {
        ArrayList<File> files = new ArrayList<File>();
        files.add(createSampleFile());
        files.add(createSampleFile());
        files.add(createSampleFile());
        for (File file: files){
            file.deleteOnExit();
        }
        try {
            MultipleFileUpload xfer = transferManager.uploadFileList(bucketName,
                    key,
                    new File("."),
                    files
            );
            xfer.waitForCompletion();
            // loop with Transfer.isDone()
            //  or block with Transfer.waitForCompletion()
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        System.out.println("Multi file uploaded by transfer manager");
    }
}
