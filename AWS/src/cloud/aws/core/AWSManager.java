package cloud.aws.core;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Async;
import com.amazonaws.services.ec2.AmazonEC2AsyncClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import core.util.PropertiesUtil;

public class AWSManager {
	
	private final Logger log = LogManager.getLogger(AWSManager.class);
	
	private static AWSManager instance = null;
	private BasicAWSCredentials creds = null;
	
	private AWSManager() {
		String accessKey = PropertiesUtil.getInstance().getAccessKey();
		String secretKey = PropertiesUtil.getInstance().getSecretKey();
		if (Objects.isNull(accessKey) || Objects.isNull(secretKey)) {
			log.error("Problem with access-key and secret-key.");
			log.error("Exiting...");
			System.exit(-1);
		}
		creds = new BasicAWSCredentials(accessKey, secretKey);
	}
	
	public static AWSManager getInstance() {
		if (instance == null) {
			instance = new AWSManager();
		}
		return instance;
	}
	
	public AmazonS3 getAmazonS3(Regions region) {
		return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(region).build();
	}

	public AmazonEC2 getAmazonEC2(Regions region) {
		return getAmazonEC2(region.getName());
	}
	
	public AmazonEC2 getAmazonEC2(String region) {
		return AmazonEC2ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(region).build();
	}
	
	public AmazonEC2Async getAmazonEC2Assync(Regions region) {
		return getAmazonEC2Assync(region.getName());
	}

	public AmazonEC2Async getAmazonEC2Assync(String region) {
		return AmazonEC2AsyncClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(region).build();
	}
	
	
}
