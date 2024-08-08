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
        catch (SQLExeption e)
        {
            for (Throwable t : e)
                t.printStackTrace();
        }
    }
}

// making a test make the PROBLEMS table with columns REPO, KEY, and CONTENTS. 
// The first two are VARCHAR, the third BYTEA (the Postgres version of BLOB). Insert a couple of rows and select

public static void runTest() throws SQLException, IOException
{
    try (Connection conn = getConnection();
            Statement stat = conn.createStatement())
}
