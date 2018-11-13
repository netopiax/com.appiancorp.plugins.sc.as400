package com.appiancorp.plugins.sc.as400v2;

import com.appiancorp.exceptions.InsufficientPrivilegesException;
import com.appiancorp.exceptions.ObjectNotFoundException;
import com.appiancorp.ps.plugins.typetransformer.AppianList;
import com.appiancorp.ps.plugins.typetransformer.AppianObject;
import com.appiancorp.ps.plugins.typetransformer.AppianTypeFactory;
import com.appiancorp.suiteapi.expression.annotations.AppianScriptingFunctionsCategory;
import com.appiancorp.suiteapi.expression.annotations.Function;
import com.appiancorp.suiteapi.expression.annotations.Parameter;
import com.appiancorp.suiteapi.security.external.SecureCredentialsStore;
import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.TypeService;
import com.appiancorp.suiteapi.type.TypedValue;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

@AppianScriptingFunctionsCategory
public class QueryAS400v2 {

    public QueryAS400v2() {

    }
    public void main(String[] args){

    }

    private static final Logger LOG = Logger.getLogger(QueryAS400v2.class.getName());

    @Function
    public TypedValue RunQueryAS400v2(TypeService ts, SecureCredentialsStore scs, @Parameter String scsKey, @Parameter String connectionString, @Parameter String sqlStatement, @Parameter int startIndex, @Parameter int batchSize) {

//        List<TypedValue> asResultList = new ArrayList<>(batchSize);
        AppianTypeFactory tf = AppianTypeFactory.newInstance(ts);
        AppianList asResultList = tf.createList(AppianType.DICTIONARY);

        // convert from 1-based (Appian) to 0-based (SQL LIMIT clause) index
        startIndex = Integer.max(0, startIndex-1);

        // following two blocks check that the driver class (included in plugin) can be loaded and registered
        try {
            Class.forName("com.ibm.as400.access.AS400JDBCDriver");
        } catch (Exception e) {
            LOG.error("Couldn't load driver class: " + e.getMessage(), e);
        }

        try {
            DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());
        } catch (Exception e) {
            LOG.error("Couldn't load AS400 JDBC driver: " + e.getMessage(), e);
        }

        // create SQL connection properties object
        Properties prop = new Properties();
        try {
            // access secure credential store for username and password
            Map<String, String> creds = scs.getSystemSecuredValues(scsKey);

            prop.setProperty("user", creds.get("username"));
            prop.setProperty("password", creds.get("password"));
            // translate binary transparently converts VARCHARBINARY and CHARBINARY columns to VARCHAR and CHAR
            prop.setProperty("translate binary", "true");

        } catch (ObjectNotFoundException | InsufficientPrivilegesException e) {
            LOG.error("Couldn't access Secure Credentials Store: " + e.getMessage(), e);
        }

        try (
                // try-with-resources automatically closes connection after try block succeeds or fails
                Connection connection = DriverManager.getConnection(connectionString, prop)
        ) {
            // result set scrollable so we can get row count. read only - this expression must not have side effects
            PreparedStatement ps = connection.prepareStatement(
                            sqlStatement + " LIMIT ?, ?",
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY
            );
            ps.setInt(1, startIndex);
            ps.setInt(2, batchSize);

                LOG.debug("connection successful, statement created");

            ResultSet rs = ps.executeQuery();
                LOG.debug("Statement executed... " + ps.toString());

            ResultSetMetaData resultSetMetaData = rs.getMetaData();

            int columnCount = resultSetMetaData.getColumnCount();
            LOG.debug("Result Set Column Count: " + columnCount);

            // save column names for use in populating dictionary. Print types and names to debug
            ArrayList<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++){
                columnNames.add(resultSetMetaData.getColumnLabel(i));
                LOG.debug("Result Set column [" + i + "]: " + resultSetMetaData.getColumnTypeName(i) + " " + columnNames.get(i - 1));
            }

            // scroll to last row to get length of result set for debug
            rs.last();
            LOG.debug("ResultSet Length: " + rs.getRow());
            rs.beforeFirst();

            try {
                while (rs.next()) { // iterate through resultset rows

                    // create dictionary object that represents one row of results
                    AppianObject tempDict = (AppianObject) tf.createElement(AppianType.DICTIONARY);
                    LOG.debug("Copying row " + asResultList.size());

                    //iterate through columns
                    for(int i = 1; i <= columnCount; i++){
                        // add each column by name and value to this row's dictionary
                        tempDict.put(columnNames.get(i - 1), tf.createString(rs.getString(i)));
                    }

                    //add single row dictionary to the List of Dictionary we'll return
                    asResultList.add(tempDict);
                }
            } catch (Exception e) {

                LOG.error("Error copying ResultSet: " + e.getMessage(), e);
            }
        } catch (SQLException se) {
            LOG.error("Error executing query: " + se.getMessage(), se);
        } catch (Exception e) {
            LOG.error("Other plugin error: " + e.getMessage(), e);
        }

        return tf.toTypedValue(asResultList);
    }

}
