import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.commons.io.IOUtils;

public class AWS{
  Dotenv dotenv = Dotenv.configure().load();
  String bucket_name;
  String file_path;
  String key_name;
  String profileName;
  AmazonS3 s3;

  AWS() {
    this.bucket_name="baz-noticias";
    this.file_path="input.json";
    this.key_name="podcast/Azteca/relatos-de-horror-historias-de-terror/index.json";
    this.profileName="dev-diego";
    this.s3 = getInstanceAWS("ProfileCLI");
  }

  public boolean putObject() throws FileNotFoundException {
    System.out.format("\n\nUploading %s to S3 bucket %s...\n\n", this.file_path, this.bucket_name);

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("application/json");

    InputStream streamData = new FileInputStream(this.file_path);
    try {
      this.s3.putObject(this.bucket_name, this.key_name, streamData, metadata);
      return true;
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
      return false;
    }
  }

  public String getObject() throws Exception {
    System.out.format("\n\nUploading %s to S3 bucket %s...\n\n", this.file_path, this.bucket_name);

    try {
      S3Object s3Object = this.s3.getObject(this.bucket_name, this.key_name);
      S3ObjectInputStream s3Is = s3Object.getObjectContent();
      return IOUtils.toString(s3Is, StandardCharsets.UTF_8);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      return null;
    }
  }

  public AmazonS3 getInstanceAWS(String typeInstance) {
    if(typeInstance.equals("ProfileCLI"))
      return AmazonS3ClientBuilder.standard()
        .withCredentials(new ProfileCredentialsProvider(this.profileName))
        .withRegion(Regions.US_EAST_1).build();
    else {
      BasicAWSCredentials awsCredentials =
        new BasicAWSCredentials(dotenv.get("AWS_ACCESS_KEY_ID"), dotenv.get("AWS_SECRET_KEY"));
      return AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
        .withRegion(Regions.US_EAST_1)
        .build();
    }
  }
}
