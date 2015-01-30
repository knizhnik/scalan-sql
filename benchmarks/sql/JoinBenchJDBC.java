import java.sql.*;

public class JoinBenchJDBC {
	public static void main(String[] args) throws Exception {
        int nRecordsLog = 24;
        int nRecords = 1 << nRecordsLog;

        String db = (args.length != 0) ? args[0].toLowerCase() : "monetdb";
        String driver;
        String url;
        String username;
        String password;
        boolean autoCommit = false;
        boolean createIndexesAfterLoad = false;
        if (db.startsWith("monet")) {
            driver = "nl.cwi.monetdb.jdbc.MonetDriver";
            url = "jdbc:monetdb://localhost/demo";
            username = "monetdb";
            password = "monetdb";
            createIndexesAfterLoad = true;
        } else if (db.startsWith("sqlite")) {
            driver = "org.sqlite.JDBC";
            url = "jdbc:sqlite::memory:";
            username = "sqlite";
            password = "sqlite";     
            autoCommit = true;
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

        if (!autoCommit) { 
            stmt.execute("start transaction");
        }
        if (createIndexesAfterLoad) { 
            stmt.execute("create table t1 (head integer, tail integer)");
            stmt.execute("create table t2 (head integer, tail integer)");
            stmt.execute("create table t3 (head integer, tail integer)");
            stmt.execute("create table t4 (head integer, tail integer)");
            stmt.execute("create table t5 (head integer, tail integer)");
        } else {
            stmt.execute("create table t1 (head integer, tail integer)");
            stmt.execute("create table t2 (head integer primary key, tail integer)");
            stmt.execute("create table t3 (head integer primary key, tail integer)");
            stmt.execute("create table t4 (head integer primary key, tail integer)");
            stmt.execute("create table t5 (head integer primary key, tail integer)");
        }
        for (int i = 0; i < 5; i++) { 
            PreparedStatement pstmt = con.prepareStatement("insert into t" + (i+1) + " (head,tail) values (?,?)");
            for (int j = 0; j < nRecords; j++) {
                pstmt.setInt(1 + (i & 1), j);
                pstmt.setInt(2 - (i & 1), Integer.reverse(j) >>> (32 - nRecordsLog));
                pstmt.execute();
            }
            pstmt.close();
        }
        if (!autoCommit) { 
            stmt.execute("commit");
        }
        System.out.println("Elapsed time for loading data: " + (System.currentTimeMillis() - start) + " msec");
        if (createIndexesAfterLoad) { 
            start = System.currentTimeMillis();
            stmt.execute("start transaction");
            stmt.execute("create index t2_pk on t2(head)");
            stmt.execute("create index t3_pk on t3(head)");
            stmt.execute("create index t4_pk on t4(head)");
            stmt.execute("create index t5_pk on t5(head)");
            stmt.execute("commit");
            System.out.println("Elapsed time for creating indexes: " + (System.currentTimeMillis() - start) + " msec");
        }
        start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery("select sum(t5.tail) from t1 join t2 on t1.tail=t2.head join t3 on t2.tail=t3.head join t4 on t3.tail=t4.head join t5 on t4.tail=t5.head where t1.head % 3 = 0 and t5.tail % 3 = 0");
        rs.next();
        System.out.println(rs.getLong(1));
        System.out.println("Elapsed time for query execution: " + (System.currentTimeMillis() - start) + " msec");
        con.close();
    }
}