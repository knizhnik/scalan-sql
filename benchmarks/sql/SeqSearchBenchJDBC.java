import java.sql.*;

public class SeqSearchBenchJDBC {
	public static void main(String[] args) throws Exception {
        int nRecords = 50000000;

        String db = (args.length != 0) ? args[0].toLowerCase() : "monetdb";
        String driver;
        String url;
        String username;
        String password;
        if (db.startsWith("monet")) {
            driver = "nl.cwi.monetdb.jdbc.MonetDriver";
            url = "jdbc:monetdb://localhost/demo";
            username = "monetdb";
            password = "monetdb";
        } else if (db.startsWith("postgre")) {
            driver = "org.postgresql.Driver";
            url = "jdbc:postgresql:postgres";
            username = "postgres";
            password = "postgres";                
        } else {
            throw new IllegalArgumentException("Unsupported database");
        }             
		Class.forName(driver);
		Connection con = DriverManager.getConnection(url, username, password);

        Statement stmt = con.createStatement(); 
        long start = System.currentTimeMillis();
        stmt.execute("start transaction");
        stmt.execute("create table KeyValue (key integer, value varchar(20))");
        PreparedStatement pstmt = con.prepareStatement("insert into KeyValue (key,value) values (?,?)");
        
        for (int i = 0; i  < nRecords; i++) {
            pstmt.setInt(1, i+1);
            pstmt.setString(2, Integer.toString(i));
            pstmt.execute();
        }
        stmt.execute("commit");
        System.out.println("Elapsed time for loading data: " + (System.currentTimeMillis() - start) + " msec");
        start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery("select count(*) from KeyValue where value like '%123%' and key > 0");
        rs.next();
        System.out.println("Found " + rs.getLong(1) + " records");
        System.out.println("Elapsed time for query execution: " + (System.currentTimeMillis() - start) + " msec");
        con.close();
    }
}