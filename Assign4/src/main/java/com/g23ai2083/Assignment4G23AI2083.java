package com.g23ai2083;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Assignment4G23AI2083 {
    private static final String PROJECT_ID = "virtualization-and-cloud";
    private static final String INSTANCE_ID = "chella1505";
    private static final String COLUMN_FAMILY = "sensor";
    private static final String TABLE_ID = "weather";
    private BigtableDataClient bigtableDataClient;
    private BigtableTableAdminClient adminClient;
    private static final String SPEED = "speed";
    private static final String TEMPERATURE = "temperature";
    private static final Logger logger = Logger.getLogger(Assignment4G23AI2083.class.getName());
    public static void main(String[] args) {
        Assignment4G23AI2083 g23AI2083 = new Assignment4G23AI2083();
        try {
            g23AI2083.run();
        } catch (Exception e) {
            logger.severe("Failed to connect to Bigtable: " + e.getMessage());
        }
    }
    public void run() {
        try {
            connect();
            deleteTable();
            createTable();
            loadData("C:\\Users\\Chella Vignesh K P\\Downloads\\Assig-4\\Assig-4\\data");
            int temp = query1();
            logger.log(Level.INFO, "Temperature at Vancouver on 2022-10-01 at 10 a.m.: {0}", new Object[]{temp});
            int windspeed = query2();
            logger.log(Level.INFO, "Highest Windspeed in Month of Sep 2022: {0}", new Object[]{windspeed});
            List<Object[]> data = query3();
            StringBuilder buf = new StringBuilder();
            buf.append("Date\t\t\tHour\tTemperature\tDewPoint\tHumidity\tWindSpeed\tPressure\n");
            for (Object[] vals : data) {
                buf.append(String.format("%-12s\t%-5s\t%-12s\t%-8s\t%-8s\t%-9s\t%-8s%n",
                        vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6]));
            }
            logger.log(Level.INFO, "Query #3 results:\n {0}", buf);
            int temperature = query4();
            logger.log(Level.INFO, "Highest Temperature at any station in the summer months of 2022: {0}", new Object[]{temperature});
            double avgTemp = query5();
            logger.log(Level.INFO, "Average Temperature in the Month of October at SeaTac: {0}", new Object[]{avgTemp});
            close();
        } catch (Exception e) {
            logger.severe("Failed to create the table: " + e.getMessage());
        }
    }
    public void connect() throws IOException {
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build();
        bigtableDataClient = BigtableDataClient.create(dataSettings);
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build();
        adminClient = BigtableTableAdminClient.create(adminSettings);
        logger.info("Successfully connected to Bigtable.");
    }

    public void close() {
        if (bigtableDataClient != null) {
            bigtableDataClient.close();
            logger.info("Successfully closed the Bigtable data client.");
        }
        if (adminClient != null) {
            adminClient.close();
            logger.info("Successfully closed the Bigtable admin client.");
        }
    }

    public void createTable() {
        try {
            if (!adminClient.exists(TABLE_ID)) {
                logger.log(Level.INFO, "Creating table: {0}", new Object[]{TABLE_ID});
                adminClient.createTable(CreateTableRequest.of(TABLE_ID).addFamily(COLUMN_FAMILY));
                logger.log(Level.INFO, "Table {0} created successfully.", new Object[]{TABLE_ID});
            } else {
                logger.log(Level.INFO, "Table {0} already exists.", new Object[]{TABLE_ID});
            }
        } catch (Exception e) {
            logger.severe("Error creating table: " + e.getMessage());
        }
    }

    public void loadData(String dirPath) throws IOException {
        File dir = new File(dirPath);
        File[] files = dir.listFiles((dir1, name) -> name.endsWith(".csv"));

        if (files == null || files.length == 0) {
            logger.log(Level.WARNING, "No CSV files found in directory: {0}", new Object[]{dirPath});
            return;
        }

        for (File file : files) {
            logger.info("Processing file: " + file.getName());
            loadSensorDataHourly(file);
        }
    }

    private void loadSensorDataHourly(File file) throws IOException {
        int recordCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            String previousHour = null;
            String[] previousFields = null;
            String stationId = getStationIdFromFile(file.getName());
            String title = br.readLine();
            String header = br.readLine();
            logger.log(Level.INFO, "Title: {0}, Header: {1}", new Object[]{title, header});
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length < 9) {
                    continue;
                }
                String date = fields[1];
                String time = fields[2];
                String[] timeParts = time.split(":");
                int hour = Integer.parseInt(timeParts[0]);
                String currentHour = date + "T" + String.format("%02d", hour);
                if (!currentHour.equals(previousHour)) {
                    if (previousFields != null) {
                        String rowKey = stationId + "#" + previousHour;
                        writeRowToBigtable(rowKey, previousFields);
                        recordCount++;
                    }
                    previousHour = currentHour;
                    previousFields = fields;
                }
            }
            if (previousFields != null) {
                String rowKey = stationId + "#" + previousHour;
                writeRowToBigtable(rowKey, previousFields);
                recordCount++;
            }
        }
        logger.log(Level.INFO, "File processed: {0}, Records inserted: {1}", new Object[]{file.getName(), recordCount});
    }

    private void writeRowToBigtable(String rowKey, String[] fields) {
        try {
            RowMutation rowMutation = RowMutation.create(TableId.of(TABLE_ID), rowKey)
                    .setCell(COLUMN_FAMILY, "pseudo_julian_date", fields[0])
                    .setCell(COLUMN_FAMILY, "date", fields[1])
                    .setCell(COLUMN_FAMILY, "time", fields[2])
                    .setCell(COLUMN_FAMILY, TEMPERATURE, fields[3])
                    .setCell(COLUMN_FAMILY, "dewpoint", fields[4])
                    .setCell(COLUMN_FAMILY, "relhum", fields[5])
                    .setCell(COLUMN_FAMILY, SPEED, fields[6])
                    .setCell(COLUMN_FAMILY, "gust", fields[7])
                    .setCell(COLUMN_FAMILY, "pressure", fields[8]);
            bigtableDataClient.mutateRow(rowMutation);
        } catch (Exception e) {
            logger.severe("Failed to insert row: " + rowKey + ", Exception: " + e.getMessage());
        }
    }

    private String getStationIdFromFile(String fileName) {
        if (fileName.contains("seatac")) return "SEA";
        if (fileName.contains("vancouver")) return "YVR";
        if (fileName.contains("portland")) return "PDX";
        return "UNKNOWN";
    }

    public void deleteTable() {
        logger.log(Level.INFO, "Deleting table: {0}", new Object[]{TABLE_ID});
        try {
            adminClient.deleteTable(TABLE_ID);
            logger.log(Level.INFO, "Table {0} deleted successfully.", new Object[]{TABLE_ID});
        } catch (NotFoundException e) {
            logger.info(String.format("Table %s does not exist.", TABLE_ID));
        } catch (Exception e) {
            logger.severe("Error deleting table: " + e.getMessage());
        }
    }

    public int query1() {
        logger.log(Level.INFO, "Executing query #1.");
        Query query = Query.create(TableId.of(TABLE_ID))
                .prefix("YVR#2022-10-01T10");
        for (Row row : bigtableDataClient.readRows(query)) {
            for (RowCell cell : row.getCells()) {
                String qualifier = cell.getQualifier().toStringUtf8();
                String value = cell.getValue().toStringUtf8();
                if (qualifier.equals(TEMPERATURE)) {
                    return Integer.parseInt(value);
                }
            }
        }
        logger.log(Level.WARNING, "Temperature column not found for the given row key.");
        return 0;
    }

    public int query2(){
        logger.log(Level.INFO, "Executing query #2.");
        Query query = Query.create(TableId.of(TABLE_ID))
                .prefix("PDX#2022-09");
        int maxWindSpeed = 0;
        for (Row row : bigtableDataClient.readRows(query)) {
            for (RowCell cell : row.getCells()) {
                String qualifier = cell.getQualifier().toStringUtf8();
                if (qualifier.equals(SPEED)) {
                    try {
                        int windSpeed = Integer.parseInt(cell.getValue().toStringUtf8());
                        maxWindSpeed = Math.max(maxWindSpeed, windSpeed);
                    } catch (NumberFormatException e) {
                        logger.log(Level.SEVERE, "Invalid wind speed value: {0}", new Object[]{cell.getValue().toStringUtf8()});
                    }
                }
            }
        }
        return maxWindSpeed;
    }
    public List<Object[]> query3() {
        logger.log(Level.INFO, "Executing query #3.");
        Query query = Query.create(TableId.of(TABLE_ID))
                .prefix("SEA#2022-10-02");
        List<Object[]> data = new ArrayList<>();
        for (Row row : bigtableDataClient.readRows(query)) {
            String date = row.getCells(COLUMN_FAMILY, "date").getFirst().getValue().toStringUtf8();
            String hour = row.getCells(COLUMN_FAMILY, "time").getFirst().getValue().toStringUtf8();
            int temperature = Integer.parseInt(row.getCells(COLUMN_FAMILY, TEMPERATURE).getFirst().getValue().toStringUtf8());
            int dewPoint = Integer.parseInt(row.getCells(COLUMN_FAMILY, "dewpoint").getFirst().getValue().toStringUtf8());
            String humidity = row.getCells(COLUMN_FAMILY, "relhum").getFirst().getValue().toStringUtf8();
            String windSpeed = row.getCells(COLUMN_FAMILY, SPEED).getFirst().getValue().toStringUtf8();
            String pressure = row.getCells(COLUMN_FAMILY, "pressure").getFirst().getValue().toStringUtf8();
            data.add(new Object[] {date, hour, temperature,dewPoint, humidity, windSpeed, pressure});
        }
        return data;
    }

    public int query4(){
        logger.log(Level.INFO, "Executing query #4.");
        String startkeypdx = "PDX#2022-07-01T00";
        String endkeypdx = "PDX#2022-08-31T23";
        String startkeyyvr = "YVR#2022-07-01T00";
        String endkeyyvr = "YVR#2022-08-31T23";
        String startkeysea = "SEA#2022-07-01T00";
        String endkeysea = "SEA#2022-08-31T23";
        int maxTemp = Integer.MIN_VALUE;
        Query query = Query.create(TableId.of(TABLE_ID))
                .range(startkeypdx, endkeypdx)
                .range(startkeyyvr, endkeyyvr)
                .range(startkeysea, endkeysea);
        ServerStream<Row> rows = bigtableDataClient.readRows(query);
        for (Row row : rows) {
            for (RowCell cell : row.getCells()) {
                String qualifier = cell.getQualifier().toStringUtf8();
                if (TEMPERATURE.equals(qualifier)) {
                    int temperature = Integer.parseInt(cell.getValue().toStringUtf8());
                    maxTemp = Math.max(maxTemp, temperature);
                }
            }
        }
        return maxTemp;
    }

    public double query5(){
        logger.log(Level.INFO, "Executing query #5.");
        logger.log(Level.INFO, "Calculating the average temperature at SeaTac for October 2022.");
        Query query = Query.create(TableId.of(TABLE_ID))
                .prefix("SEA#2022-10");
        int sumTemperature = 0;
        int count = 0;
        for (Row row : bigtableDataClient.readRows(query)) {
            for (RowCell cell : row.getCells()) {
                if (cell.getQualifier().toStringUtf8().equals(TEMPERATURE)) {
                    sumTemperature += Integer.parseInt(cell.getValue().toStringUtf8());
                    count++;
                }
            }
        }
        if (count == 0) {
            logger.log(Level.WARNING, "No temperature data found for the given query.");
            return 0;
        }
        double averageTemperature = (double) sumTemperature / count;
        logger.log(Level.INFO, "Average Temperature: {0}", new Object[]{averageTemperature});
        return averageTemperature;
    }

}