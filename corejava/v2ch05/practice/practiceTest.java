package practice; 

//Should learn why these needed to be imported java.___.*
import java.nio.file.*; 
import java.sql.*; 
import java.io.*; 
import java.nio.charset.*; 
import java.util.*; 

//I believe this would see if database & JDBC are correctly configured like how it is in TestDB
public class practiceTest
{
    public static void main(String args[]) throws IOException
    {
        try
        {
            runTest();
        }
        catch (SQLException e)
        {
            for (Throwable t : e)
                t.printStackTrace();
        }
    }

    // making a test make the PROBLEMS table with columns REPO, KEY, and CONTENTS. 
    // The first two are VARCHAR, the third BYTEA (the Postgres version of BLOB). Insert a couple of rows and select

    public static void runTest() throws SQLException, IOException
    {
        try (Connection conn = getConnection();
                Statement stat = conn.createStatement())
        {
            //stat.executeUpdate("DROP TABLE PROBLEM");
            stat.executeUpdate("CREATE TABLE PROBLEM (REPO VARCHAR(10), KEY VARCHAR(10), CONTENTS BYTEA)");
            stat.executeUpdate("INSERT INTO PROBLEM VALUES ('1', 'test.exe', '\b012345')");
            stat.executeUpdate("INSERT INTO PROBLEM VALUES ('2', 'test.exe', '\b012345')");
            stat.executeUpdate("INSERT INTO PROBLEM VALUES ('3', 'test.exe', '\b012345')");
            stat.executeUpdate("INSERT INTO PROBLEM VALUES ('4', 'test.exe', '\b012345')");
            stat.executeUpdate("INSERT INTO PROBLEM VALUES ('5', 'test.exe', '\b012345')");

            try (ResultSet result = stat.executeQuery("SELECT * FROM PROBLEM"))
            {
                while(result.next())
                {
                    System.out.println(result.getString(1));
                    System.out.println(result.getString(2));
                    //get blob & get byte and to print out?
                    System.out.println(result.getBytes(3));

                }
            }
            stat.executeUpdate("DROP TABLE PROBLEM");
        }
    }

    //section for the, should be different
    public static Connection getConnection() throws SQLException, IOException
    {
        var props = new Properties();
        try (Reader in = Files.newBufferedReader(
                Path.of("database.properties"), StandardCharsets.UTF_8))
        {
            props.load(in);
        }
        String drivers = props.getProperty("jdbc.drivers");
        if (drivers != null) System.setProperty("jdbc.drivers", drivers);
        String url = props.getProperty("jdbc.url");
        String username = props.getProperty("jdbc.username");
        String password = props.getProperty("jdbc.password");

        return DriverManager.getConnection(url, username, password);
    }
}

