/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package amb.dev.geo.lookup.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.amb.geo.data.output.GeoResult;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.PathParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.postgresql.PGConnection;

/**
 * REST Web Service
 *
 * @author AntB
 */
@Path("geolookup")
public class GeoLookupService {

    @Context
    private UriInfo context;

    private Properties dbProps = null;
    private Connection connection = null;
    
    public GeoLookupService() throws SQLException {
        
        try {
            
            URL propsUrl = this.getClass().getResource("/database.properties");
            dbProps = new Properties();
            dbProps.load(propsUrl.openStream());
            
            String server = dbProps.getProperty("databaseServer");
            String port = dbProps.getProperty("databasePort");
            String name = dbProps.getProperty("databaseName");
            String user = dbProps.getProperty("databaseUser");
            String password = dbProps.getProperty("databaseUserPassword");
            
            this.connection = DriverManager.getConnection("jdbc:postgresql://" + server + ":" + port + "/" + name, user, password);
            
        } catch (Exception e) {
            System.out.println("Failed to initialise the GeoLookupService class due to an issue reading the database.properties file.\n"
                    + "Blow are the first 5 elements of the stack trace from the exception:\n");
            
            if(e.getStackTrace().length > 5){
                for (int a = 0; a < 5; a++) {
                    System.out.println(e.getStackTrace()[a]);
                }
            }
        }
    }
    
    
    
    @GET
    @Path("/places/{placename}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public String getPlacenames(@PathParam("placename") ArrayList<String> places) throws SQLException, JsonProcessingException {
        
        String sql = "";
        if (places != null) {
        
            Statement statement = this.connection.createStatement();
        
            for (String placename : places) {
                
                placename = this.escapeQueryCharacters(placename);
                
                if(sql.equals("") == true) {
                    sql = placename;
                } else {
                    sql = sql + " OR " + placename;
                }
            }
            // TODO Review the SQL format
            sql = "SELECT name, country, latitude, longitude FROM geoname, cpuntryWHERE name = (" + sql + ") ORDER BY name ASC";
            ResultSet results = statement.executeQuery(sql);
            
            if (results != null && results.next() == true) {
                ArrayList<GeoResult> resultGeos = new ArrayList<GeoResult>();
                
                while(results.next()) {
                    GeoResult geo = new GeoResult();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    resultGeos.add(geo);
                }
                
                ObjectMapper mapper = new ObjectMapper();
                
                return mapper.writeValueAsString(resultGeos);
                
            } else {
                return "{}";
            }
            
        } else {
            NullPointerException npe = new NullPointerException();
            throw npe;
        }
    }
    // Search for all places that match places
    
    // Validate input
    // Create connection
    // Create SQL
    // Execute, and parse result to object
    // marshall object to JSON and return
    
    
    
    // search for places in a list of countries
    
    // Validate input
    // Create connection
    // Create SQL
    // Execute, and parse result to object
    // marshall object to JSON and return
    
    
    // search for all places in a polygon
    
     // Validate input
    // Create connection
    // Create SQL
    // Execute, and parse result to object
    // marshall object to JSON and return
    
    
    
    
    private String escapeQueryCharacters(String input) {
        
        input = input.replaceAll("'" , "''");
        
        return input;
    }
}
