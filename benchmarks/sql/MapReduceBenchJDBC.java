import java.sql.*;

public class MapReduceBenchJDBC {
	public static void main(String[] args) throws Exception {
        int nRecords = 10000000;

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
        stmt.execute("create table key_value (key integer, value varchar(100))");
        PreparedStatement pstmt = con.prepareStatement("insert into key_value (key,value) values (?,?)");
        
        for (int i = 0; i  < nRecords; i++) {
            pstmt.setInt(1, i);
            pstmt.setString(2, "http://www.scala-lang.org/some/long/url/" + (i % 100));
            pstmt.execute();
        }
        stmt.execute("commit");
        System.out.println("Elapsed time for loading data: " + (System.currentTimeMillis() - start) + " msec");
        start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery("select value,sum(key) from key_value group by value order by value");
        while (rs.next()) {
            System.out.println(rs.getString(1) + "->" + rs.getLong(2));
        }
        System.out.println("Elapsed time for query execution: " + (System.currentTimeMillis() - start) + " msec");
        con.close();
    }
}