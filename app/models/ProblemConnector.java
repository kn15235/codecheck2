package models;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.horstmann.codecheck.Util;
import com.typesafe.config.Config;

import play.Logger;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;
import play.db.Database;

// TODO Use DI configuration to avoid this delegation

@Singleton
public class ProblemConnector {
    private ProblemConnection delegate;

    @Inject public ProblemConnector(Config config) {
        if (config.hasPath("com.horstmann.codecheck.s3.region"))
            delegate = new ProblemS3Connection(config);
        else if(config.hasPath("com.horstmann.codecheck.sql.region")) 
            delegate = new ProblemSQLConnection(config);
        else 
            delegate = new ProblemLocalConnection(config);
    }

    public void write(byte[] contents, String repo, String key) throws IOException {
        delegate.write(contents, repo, key);
    }

    public void delete(String repo, String key) throws IOException {
        delegate.delete(repo, key);
    }

    public byte[] read(String repo, String key) throws IOException {
        return delegate.read(repo, key);
    }
}

interface ProblemConnection {
    public void write(byte[] contents, String repo, String key) throws IOException;
    public void delete(String repo, String key) throws IOException;
    public byte[] read(String repo, String key) throws IOException;
}

class ProblemS3Connection implements ProblemConnection {
    private String bucketSuffix = null;
    private AmazonS3 amazonS3;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");

    public ProblemS3Connection(Config config) {
        String awsAccessKey = config.getString("com.horstmann.codecheck.aws.accessKey");
        String awsSecretKey = config.getString("com.horstmann.codecheck.aws.secretKey");
        String region = config.getString("com.horstmann.codecheck.s3.region"); 
        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
                .withRegion(region)
                .withForceGlobalBucketAccessEnabled(true)
                .build();

        bucketSuffix = config.getString("com.horstmann.codecheck.s3.bucketsuffix");
    }

    public void write(Path file, String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            amazonS3.putObject(bucket, key, file.toFile());
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.putToS3: Cannot put " + file + " to " + bucket);
            throw ex;
        }
    }

    public void write(String contents, String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            amazonS3.putObject(bucket, key, contents);
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.putToS3: Cannot put " + contents.replaceAll("\n", "|").substring(0, Math.min(50, contents.length())) + "... to " + bucket);
            throw ex;
        }
    }

    public void write(byte[] contents, String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contents.length);
        metadata.setContentType("application/zip");
        try {
            try (ByteArrayInputStream in = new ByteArrayInputStream(contents)) {
                amazonS3.putObject(bucket, key, in, metadata); 
            }                 
        } catch (AmazonS3Exception ex) {
            String bytes = Arrays.toString(contents);
            logger.error("S3Connection.putToS3: Cannot put " + bytes.substring(0, Math.min(50, bytes.length())) + "... to " + bucket);
            throw ex;                
        }
    }

    public void delete(String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            amazonS3.deleteObject(bucket, key);
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.deleteFromS3: Cannot delete " + bucket);
            throw ex;
        }            
    }

    public byte[] read(String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;

        byte[] bytes = null;
        try {
            // TODO -- trying to avoid warning 
            // WARN - com.amazonaws.services.s3.internal.S3AbortableInputStream - Not all bytes were read from the S3ObjectInputStream, aborting HTTP connection. This is likely an error and may result in sub-optimal behavior. Request only the bytes you need via a ranged GET or drain the input stream after use
            try (InputStream in = amazonS3.getObject(bucket, key).getObjectContent()) {
                bytes = in.readAllBytes();
            }
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.readFromS3: Cannot read " + key + " from " + bucket);
            throw ex;
        }
        return bytes;            
    }
}

class ProblemLocalConnection implements ProblemConnection {
    private Path root;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");

    public ProblemLocalConnection(Config config) {
        this.root = Path.of(config.getString("com.horstmann.codecheck.s3.local"));
        try {
           Files.createDirectories(root);            
        } catch (IOException ex) {
            logger.error("Cannot create " + root);
        } 
    }

    public void write(byte[] contents, String repo, String key) throws IOException {
        try {
            Path repoPath = root.resolve(repo);
            Files.createDirectories(repoPath);                
            Path newFilePath = repoPath.resolve(key + ".zip");
            Files.write(newFilePath, contents);
        } catch (IOException ex) {
            String bytes = Arrays.toString(contents);
            logger.error("ProblemLocalConnection.write : Cannot put " + bytes.substring(0, Math.min(50, bytes.length())) + "... to " + repo);
            throw ex;                   
        }

    }

    public void delete(String repo, String key) throws IOException {
        Path repoPath = root.resolve(repo);
        Path directoryPath = repoPath.resolve(key);
        try {
            Util.deleteDirectory(directoryPath);
        } catch (IOException ex) {
            logger.error("ProblemLocalConnection.delete : Cannot delete " + repo);
            throw ex;
        }
    }
    
    public byte[] read(String repo, String key) throws IOException {
        byte[] result = null;
        try {
            Path repoPath = root.resolve(repo);
            Path filePath = repoPath.resolve(key + ".zip");
            result = Files.readAllBytes(filePath); 
        } catch (IOException ex) {
            logger.error("ProblemLocalConnection.read : Cannot read " + key + " from " + repo);
            throw ex;                
        }
        
        return result;  
    }
}

class ProblemSQLConnection implements ProblemConnection {
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");

    public ProblemSQLConnection (Config config){
        Connection conn = null; 
        var props = new Properties();
        try (Reader in = Files.newBufferedReader(
                Path.of("database.properties"), StandardCharsets.UTF_8))
        {
            props.load(in);
            String drivers = props.getProperty("jdbc.drivers");
            if (drivers != null)
            {
                System.setProperty("jdbc.drivers", drivers);     
            } 
            String url = props.getProperty("jdbc.url");
            String username = props.getProperty("jdbc.username");
            String password = props.getProperty("jdbc.password");
            conn = DriverManager.getConnection(url, username, password);

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            conn.close(); 
        }
    }

    public void write(String repo, String key, byte[] contents) throws IOException {
        try (Connection conn = getConnection();
                Statement stat = conn.createStatement())
        {
            stat.execute("CREATE TABLE IF NOT EXISTS PROBLEM (REPO VARCHAR(10), KEY VARCHAR(10), CONTENTS BYTEA)"); 
            stat.executeUpdate("INSERT INTO PROBLEM VALUES (repo, key, contents)");

        } catch (SQLException ex) {
            String bytes = Arrays.toString(contents);
            logger.error("ProblemSQLConnection.write : Cannot put " + bytes.substring(0, Math.min(50, bytes.length())) + "... to " + repo);
            throw ex;                   
        }
    }

    public void delete(String repo, String key) throws IOException {
        try {
            //unsure...



        } catch (SQLException ex) {
            logger.error("ProblemSQLConnection.delete : Cannot delete " + repo);
            throw ex;
        }
    }

    public byte[] read(String repo, String key) throws IOException {
        byte[] result = null;

        try (Connection conn = getConnection();
                Statement stat = conn.createStatement()
                ResultSet result = stat.executeQuery("SELECT * FROM PROBLEM"))
        {
            if (result.next())
            {
                System.out.println(result.getString(1)); 
                System.out.println(result.getString(2));
            }
            stat.executeUpdate("DROP TABLE PROBLEM");

        } catch (SQLException ex) {
            logger.error("ProblemSQLConnection.read : Cannot read " + key + " from " + repo);
            throw ex;                
        }
        return result; 
    }
}




