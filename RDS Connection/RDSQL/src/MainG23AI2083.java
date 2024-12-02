import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
public class MainG23AI2083 {
    private static final Logger LOGGER = Logger.getLogger(MainG23AI2083.class.getName());
    private Connection connection;
    // private String dbURL = "database-1.crcy6uage9cb.ap-south-1.rds.amazonaws.com:5432/postgres";
    private String dbURL = "url";
    private String userId= "postgres";
    private String password = "password";
    public static void main(String[] args) {
        MainG23AI2083 main = new MainG23AI2083();
        try{
            main.connect();
//            main.drop();
//            main.create();
//            main.insert();
//            main.queryOne();
//            main.queryTwo();
//            main.queryThree();
            main.delete();
            main.closeConnection();
        }catch(SQLException e){
            LOGGER.log(Level.SEVERE, "Database connection failed!", e);
        }
    }
    public void connect() throws SQLException {
        String jdbcUrl = "jdbc:postgresql://" + dbURL;
        connection = DriverManager.getConnection(jdbcUrl, userId, password);
        LOGGER.info("Connected to the PostgreSQL server successfully.");
    }
    public void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
            LOGGER.info("Database connection closed.");
        }
    }
    public void drop() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS stockprice");
            statement.executeUpdate("DROP TABLE IF EXISTS company");

            LOGGER.info("Table dropped successfully.");
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to drop the table.", e);
        }
    }
    public void create() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE company (" +
                            "id INT PRIMARY KEY, " +
                            "name VARCHAR(50), " +
                            "ticker CHAR(10), " +
                            "annualRevenue DECIMAL(15,2), " +
                            "numEmployees INT)"
            );
            statement.executeUpdate(
                    "CREATE TABLE stockprice (" +
                            "companyId INT, " +
                            "priceDate DATE, " +
                            "openPrice DECIMAL(10,2), " +
                            "highPrice DECIMAL(10,2), " +
                            "lowPrice DECIMAL(10,2), " +
                            "closePrice DECIMAL(10,2), " +
                            "volume INT, " +
                            "PRIMARY KEY (companyId, priceDate), " +
                            "FOREIGN KEY (companyId) REFERENCES company(id))"
            );
            LOGGER.info("Tables created successfully.");
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to create the table.", e);
        }
    }
    public void insert() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "INSERT INTO company VALUES " +
                            "(1, 'Apple', 'AAPL', 387540000000.00, 154000), " +
                            "(2, 'GameStop', 'GME', 611000000.00, 12000), " +
                            "(3, 'Handy Repair', NULL, 2000000, 50), " +
                            "(4, 'Microsoft', 'MSFT', 198270000000.00, 221000), " +
                            "(5, 'StartUp', NULL, 50000, 3)"
            );
            statement.executeUpdate(
                    "INSERT INTO stockprice VALUES " +
                            "(1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700), " +
                            "(1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100)," +
                            "(1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000)," +
                            "(1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100)," +
                            "(1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500)," +
                            "(1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800)," +
                            "(1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100)," +
                            "(1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500)," +
                            "(1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200)," +
                            "(1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500)," +
                            "(1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000), " +
                            "(1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200), " +
                            "(2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100), " +
                            "(2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800), " +
                            "(2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400), " +
                            "(2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400), " +
                            "(2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600), " +
                            "(2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600), " +
                            "(2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300), " +
                            "(2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300)," +
                            "(2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300), " +
                            "(2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500), " +
                            "(2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700), " +
                            "(2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200), " +
                            "(4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700), " +
                            "(4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900), " +
                            "(4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400), " +
                            "(4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200), " +
                            "(4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200), " +
                            "(4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100), " +
                            "(4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400), " +
                            "(4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000), " +
                            "(4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400), " +
                            "(4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500), " +
                            "(4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500), " +
                            "(4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)"
            );
            LOGGER.info("Records inserted successfully.");
            ResultSet resultSet = statement.executeQuery("SELECT * FROM company");
            LOGGER.info(resultSetToString(resultSet, 10));
            resultSet = statement.executeQuery("SELECT * FROM stockprice");
            LOGGER.info(resultSetToString(resultSet, 30));
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to insert the records.", e);
        }
    }

    public void delete() throws SQLException {
        String fetchCompanyIdSql = "SELECT id FROM company WHERE name = 'GameStop'";
        int companyId = -1;
        try (Statement fetchStatement = connection.createStatement();
             ResultSet resultSet = fetchStatement.executeQuery(fetchCompanyIdSql)) {
            if (resultSet.next()) {
                companyId = resultSet.getInt("id");
                LOGGER.info(String.format("GameStop companyId found: %d", companyId));
            } else {
                LOGGER.warning("GameStop not found in the company table.");
                return;
            }
        }
        String deleteSql = "DELETE FROM stockprice WHERE priceDate < '2022-08-20' OR companyId = ?";
        try (PreparedStatement deleteStatement = connection.prepareStatement(deleteSql)) {
            deleteStatement.setInt(1, companyId);
            int rowsDeleted = deleteStatement.executeUpdate();
            LOGGER.info(String.format("%d rows deleted from stockprice table.", rowsDeleted));
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error occurred while deleting stock price records.", e);
        }
    }

    public void queryOne() throws SQLException {
        String query = "SELECT name, annualRevenue, numEmployees " +
                "FROM company " +
                "WHERE numEmployees > 10000 OR annualRevenue < 1000000 " +
                "ORDER BY name ASC";
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
             LOGGER.info(resultSetToString(rs, 10));
        }
    }

    public void queryTwo() throws SQLException {
        String query = "SELECT c.name, c.ticker, MIN(s.lowPrice) AS lowestPrice, MAX(s.highPrice) AS highestPrice, " +
                "AVG(s.closePrice) AS avgClosePrice, AVG(s.volume) AS avgVolume " +
                "FROM stockprice s " +
                "JOIN company c ON c.id = s.companyId " +
                "WHERE s.priceDate BETWEEN '2022-08-22' AND '2022-08-26' " +
                "GROUP BY c.name, c.ticker " +
                "ORDER BY avgVolume DESC";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            LOGGER.info(resultSetToString(rs, 10));
        }
    }

    public void queryThree() throws SQLException {
        String query = "SELECT c.name, c.ticker, s.closePrice " +
                "FROM company c " +
                "LEFT JOIN stockprice s ON c.id = s.companyId AND s.priceDate = '2022-08-30' " +
                "WHERE (s.closePrice <= 1.10 * (SELECT AVG(closePrice) FROM stockprice WHERE priceDate BETWEEN '2022-08-15' AND '2022-08-19' AND companyId = c.id)) " +
                "OR s.closePrice IS NULL " +
                "ORDER BY c.name ASC";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            LOGGER.info(resultSetToString(rs, 10));
        }
    }

    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException
    {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        if (rst == null)
            return "ERROR: No ResultSet";
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        while (rst.next())
        {
            if (rowCount < maxrows)
            {
                for (int j = 0; j < meta.getColumnCount(); j++)
                {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            } rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + ", "
                + meta.getColumnType(1) + "-" + meta.getColumnTypeName(1) + ", " + meta.getColumnDisplaySize(1) + ", "
                + meta.getPrecision(1) + ", " + meta.getScale(1) + ")");
        for (int j = 2; j <= meta.getColumnCount(); j++) {
            buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + ", "
                    + meta.getColumnType(j) + "-" + meta.getColumnTypeName(j) + ", " + meta.getColumnDisplaySize(j) + ", "
                    + meta.getPrecision(j) + ", " + meta.getScale(j) + ")");
        }
        return buf.toString();
    }
}
