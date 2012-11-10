package amb.dev.geo.lookup.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.micromata.opengis.kml.v_2_2_0.Document;
import de.micromata.opengis.kml.v_2_2_0.Geometry;
import dev.amb.geo.data.output.GeoResult;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST Web Service
 *
 * @author AntB
 */
@Path("geolookup")
public class GeoLookupService {

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

            if (e.getStackTrace().length > 5) {
                for (int a = 0; a < 5; a++) {
                    System.out.println(e.getStackTrace()[a]);
                }
            }
        }
    }

    @GET
    @Path("/place")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlacenamesJson(@QueryParam("placename") ArrayList<String> places) {
        String sql = "";
        try {
            Statement statement = this.connection.createStatement();

            for (String placename : places) {

                placename = this.escapeQueryCharacters(placename);

                if (sql.equals("") == true) {
                    sql = placename;
                } else {
                    sql = sql + " OR " + placename;
                }
            }
            // TODO Review the SQL format
            sql = "SELECT name, country, latitude, longitude FROM geoname, country WHERE name = (" + sql + ") ORDER BY name ASC";
            ResultSet results = statement.executeQuery(sql);

            if (results != null && results.next() == true) {
                ArrayList<GeoResult> resultGeos = new ArrayList<GeoResult>();

                while (results.next()) {
                    GeoResult geo = new GeoResult();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    resultGeos.add(geo);
                }

                ObjectMapper mapper = new ObjectMapper();
                return Response.status(Response.Status.OK).entity(mapper.writeValueAsString(resultGeos)).build();

            } else {
                return Response.status(Response.Status.OK).entity("{}").build();
            }

        } catch (JsonProcessingException jpe) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity("JSON parsing exception : (" + jpe.getMessage() + ")").build());

        } catch (SQLException sqle) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity("SQL exception for query " + sql + " \n: Message = (" + sqle.getMessage() + ")").build());

        } catch (Exception e) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity("General exception : (" + e.getMessage() + ")").build());
        }
    }
    
    
    // search for places in a list of countries
    @GET
    @Path("/place/country")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlacesInCountriesJson(@QueryParam("places") ArrayList<String> places, @QueryParam("countries") ArrayList<String> countries) {
        String sql = "";
        try {

            Statement statement = this.connection.createStatement();

            for (String placename : places) {

                placename = this.escapeQueryCharacters(placename);

                if (sql.equals("") == true) {
                    sql = placename;
                } else {
                    sql = sql + " OR " + placename;
                }
            }
            // TODO Review the SQL format
            sql = "SELECT name, country, latitude, longitude FROM geoname, country WHERE name = (" + sql + ") ORDER BY name ASC";
            ResultSet results = statement.executeQuery(sql);

            if (results != null && results.next() == true) {
                ArrayList<GeoResult> resultGeos = new ArrayList<GeoResult>();

                while (results.next()) {
                    GeoResult geo = new GeoResult();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    resultGeos.add(geo);
                }

                ObjectMapper mapper = new ObjectMapper();
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(mapper.writeValueAsString(resultGeos)).build();

            } else {
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity("{}").build();
            }

        } catch (JsonProcessingException jpe) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("JSON parsing exception : (" + jpe.getMessage() + ")").build());

        } catch (SQLException sqle) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("SQL exception : (" + sqle.getMessage() + ")").build());

        } catch (Exception e) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("General exception : (" + e.getMessage() + ")").build());
        }
    }


    // Get all places inside a bounding polygon
    @POST
    @Path("/places/polygon")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlacesInPolygonJson(String polyCoordList) {

        if (polyCoordList.equals("") == false) {

            ArrayList<String> polygons = this.getValidPolygons(polyCoordList);
            String polySearchSQL = "";

            if (polygons.isEmpty() == false) {
                try {
                    polySearchSQL = "SELECT name, country, lat, lon FROM geoname, country WHERE ( (country=iso) AND polygon ";

                    String polyList = "";
                    for (String poly : polygons) {
                        if (polyList.equals("")) {
                            polyList = poly;
                        } else {
                            polyList = polyList + "," + poly;
                        }
                    }
                    polyList = " \' ( " + polyList + " ) \' ";

                    polySearchSQL = polySearchSQL + polyList + " @> point_location )";
                    System.out.println("SQL statement = \n" + polySearchSQL + "\n");

                    Statement polyPlace = connection.createStatement();
                    ResultSet results = polyPlace.executeQuery(polySearchSQL);

                    if (results != null && results.next() == true) {
                        ArrayList<GeoResult> resultGeos = new ArrayList<GeoResult>();

                        while (results.next()) {
                            GeoResult geo = new GeoResult();
                            geo.setPlacename(results.getString(1));
                            geo.setCountry(results.getString(2));
                            geo.setLatitude(results.getDouble(3));
                            geo.setLongitude(results.getDouble(4));
                            resultGeos.add(geo);
                        }

                        ObjectMapper mapper = new ObjectMapper();
                        return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(mapper.writeValueAsString(resultGeos)).build();

                    } else {
                        return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity("{}").build();
                    }

                } catch (Exception e) {
                    throw new WebApplicationException(
                            Response
                            .status(Response.Status.BAD_REQUEST)
                            .type(MediaType.APPLICATION_JSON)
                            .entity("Exception searching DB with input SQL :: " + polySearchSQL + " (" + e.getMessage() + ")")
                            .build());
                }
            } else {
                // TODO leave as default??
                return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("Could not parse any valid polygons from :: <<" + polyCoordList + ">>").build();
            }
        } else {
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("Input polygonCoords was blank :: <<" + polyCoordList + ">>").build();
        }
    }

    // TODO Add KML methods to return KML strings
    
    // utility methods to validate input
    private ArrayList<String> getValidPolygons(String polyCoordList) {
        // assume input is (#,#),(#,#), ... , (#,#)
        ArrayList<String> validPolygons = new ArrayList<String>();

        if (polyCoordList.equals("") == false || polyCoordList == null) {
            return null;
        } else {
            // maybe just one polygon
            String[] coords = polyCoordList.split("(\\Q),(\\E)");
            if ((coords.length < 1) == true) {
                return null;
            } else {

                String polyCoords = "";

                for (String coord : coords) {
                    String validCoord = this.validateCoordinate(coord);

                    if (validCoord != null) {
                        return null;
                    }
                    // concat valid coords to a single polygon string
                    if (polyCoords.equals("")) {
                        polyCoords = validCoord;
                    } else {
                        polyCoords = polyCoords + "," + validCoord;
                    }

                    validPolygons.add(polyCoords);

                }

                return validPolygons;

            }
        }
    }

    private String escapeQueryCharacters(String input) {
        input = input.replaceAll("'", "''");
        return input;
    }

    private String validateCoordinate(String input) {
        // assume lat/lon decimal degree, maximum 6 decimal places in form ( ## , ## )
        // Regex tested using http://www.regexplanet.com/advanced/java/index.html

        // trim brackets and spaces from input
        String inputValues = input.replace("(", "");
        inputValues = inputValues.replaceAll(")", "");
        inputValues = inputValues.replaceAll(" ", "");

        String lat = "";
        String lon = "";

        String[] coord = inputValues.split(",");
        if (coord.length == 2) {
            lat = coord[0];
            lon = coord[1];
        }

        Pattern latRegex = Pattern.compile("[\\Q-\\E]{0,1}[0-9]{1,2}([.]{1}[0-9]{0,6}){0,1}");
        Pattern lonRegex = Pattern.compile("[\\Q-\\E]{0,1}[0-9]{1,3}([.]{1}[0-9]{0,6}){0,1}");

        Matcher latMatch = latRegex.matcher(lat);
        Matcher lonMatch = lonRegex.matcher(lon);

        if (latMatch.matches() == true && lonMatch.matches() == true) {
            // add brackets for subsequent SQL statement
            return "(" + inputValues + ")";
        } else {
            return null;
        }
    }
}
