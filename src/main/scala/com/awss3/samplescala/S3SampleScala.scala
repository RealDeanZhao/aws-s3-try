
import java.io._
import java.util.UUID

import com.amazonaws.AmazonServiceException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.TransferManagerBuilder

import scala.collection.JavaConverters
import scala.util.control.Breaks._


object S3SampleScalaApp extends App {
  val s3 = AmazonS3ClientBuilder
  .standard
  .withRegion(Regions.AP_SOUTHEAST_1)
  .build

  val transferManager = TransferManagerBuilder
    .standard
    .withS3Client(s3)
    .withShutDownThreadPools(true)
    .build

  try {
    // list all buckets
    for (item <-JavaConverters.asScalaBuffer(s3.listBuckets) ) {
      System.out.println(" = " + item.getName)
    }
    val bucketName = "deanzhao"
    // create the bucket
    if (!s3.doesBucketExist(bucketName)) s3.createBucket(bucketName)
    // create a file on S3
    val key = "s3putObject" + UUID.randomUUID
    s3.putObject(new PutObjectRequest(bucketName, key, S3SampleScala.createSampleFile))
    // download the file
    val s3Object = s3.getObject(new GetObjectRequest(bucketName, key))
    S3SampleScala.displayTextInputStream(s3Object.getObjectContent)
    // upload a single file by TransferManager
    S3SampleScala.uploadSingleFileViaTransferManager(transferManager, bucketName, "transferManagerSingleFile" + UUID.randomUUID)
    S3SampleScala.uploadMultipleFilesViaTransferManager(transferManager, bucketName, "transferManagerMultiFile" + UUID.randomUUID)
  } finally transferManager.shutdownNow(true)
}


object S3SampleScala {
  @throws[IOException]
  def createSampleFile = {
    // The most reliable way to avoid a ResetException is to provide data by using a File or FileInputStream,
    // which the AWS SDK for Java can handle without being constrained by mark and reset limits.
    val file = File.createTempFile("aws-java-sdk-", ".txt")
    file.deleteOnExit()
    val writer = new OutputStreamWriter(new FileOutputStream(file))
    writer.write("hahahaha\n")
    writer.write("fasdfasdfad\n")
    writer.close()
    file
  }

  object AllDone extends Exception {  }

  @throws[IOException]
  def displayTextInputStream(input: InputStream):Unit = {
    val reader = new BufferedReader(new InputStreamReader(input))
    try {
      while ( {
        true
      }) {
        val line = reader.readLine
        if (line == null) {
          println("    " + line)
          throw AllDone
        }
      }
    } catch {
      case e: Exception =>
    }
  }

  @throws[IOException]
  @throws[InterruptedException]
  def uploadSingleFileViaTransferManager
  (transferManager: TransferManager, bucketName: String, key: String) = {
    val file = createSampleFile
    try {
      val xfer = transferManager.upload(bucketName, key, file)
      xfer.waitForCompletion()
      // loop with Transfer.isDone()
      //  or block with Transfer.waitForCompletion()
    } catch {
      case e: AmazonServiceException =>
        System.err.println(e.getErrorMessage)
        System.exit(1)
    }
    System.out.println("Single file uploaded by transfer manager")
  }

  @throws[IOException]
  @throws[InterruptedException]
  def uploadMultipleFilesViaTransferManager(transferManager: TransferManager, bucketName: String, key: String) = {
    val files = List[File](createSampleFile, createSampleFile,createSampleFile)
    for (file <- files) {
      file.deleteOnExit()
    }
    try {
      val xfer = transferManager.uploadFileList(bucketName, key, new File("."),JavaConverters.seqAsJavaList(files))
      xfer.waitForCompletion()
    } catch {
      case e: AmazonServiceException =>
        System.err.println(e.getErrorMessage)
        System.exit(1)
    }
    System.out.println("Multi file uploaded by transfer manager")
  }
}
