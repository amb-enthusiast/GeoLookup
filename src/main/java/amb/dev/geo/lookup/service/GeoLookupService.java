package amb.dev.geo.lookup.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.amb.geo.data.input.GeoArea;
import dev.amb.geo.data.input.SimpleCoord;
import dev.amb.geo.data.output.GeoLookup;
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

    public GeoLookupService() {

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
                    + "Below are the first 5 elements of the stack trace from the exception:\n");

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

        GeoResult geoResult = new GeoResult();

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
                ArrayList<GeoLookup> geoLookups = new ArrayList<GeoLookup>();

                while (results.next()) {
                    GeoLookup geo = new GeoLookup();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    geoLookups.add(geo);
                }

                geoResult.setGeos(geoLookups);

                ObjectMapper mapper = new ObjectMapper();
                return Response.status(Response.Status.OK).entity(mapper.writeValueAsString(geoResult)).build();

            } else {
                return Response.status(Response.Status.OK).entity("{}").build();
            }

        } catch (JsonProcessingException jpe) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("JSON parsing exception : (" + jpe.getMessage() + ")").build());

        } catch (SQLException sqle) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("SQL exception for query " + sql + " \n: Message = (" + sqle.getMessage() + ")").build());

        } catch (Exception e) {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("General exception : (" + e.getMessage() + ")").build());
        }
    }

    @GET
    @Path("/place")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_XML)
    public Response getPlacenamesKml(@QueryParam("placename") ArrayList<String> places) {

        GeoResult geoResult = new GeoResult();

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
                ArrayList<GeoLookup> geoLookups = new ArrayList<GeoLookup>();

                while (results.next()) {
                    GeoLookup geo = new GeoLookup();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    geoLookups.add(geo);
                }

                geoResult.setGeos(geoLookups);

                // get KML string
                String kml = GeoKmlUtil.createGeoLookupKml(geoLookups);

                return Response.status(Response.Status.OK).type(MediaType.TEXT_XML).entity(kml).build();

            } else {
                return Response.status(Response.Status.OK).type(MediaType.TEXT_XML).entity("</>").build();
            }

        } catch (Exception e) {
                throw new WebApplicationException(
                        Response
                        .status(Response.Status.BAD_REQUEST)
                        .type(MediaType.TEXT_XML)
                        .entity("<Exception searching DB :: " + " (" + e.getMessage() + ")/>")
                        .build());
            }
    }

    
    
    // search for places in a list of countries
    @GET
    @Path("/place/country")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlacesInCountriesJson(@QueryParam("places") ArrayList<String> places, @QueryParam("countries") ArrayList<String> countries) {
        GeoResult geoResult = new GeoResult();
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
                ArrayList<GeoLookup> geoLookups = new ArrayList<GeoLookup>();

                while (results.next()) {
                    GeoLookup geo = new GeoLookup();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    geoLookups.add(geo);
                }

                geoResult.setGeos(geoLookups);

                ObjectMapper mapper = new ObjectMapper();
                return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(mapper.writeValueAsString(geoResult)).build();

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

    @GET
    @Path("/place/country")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_XML)
    public Response getPlacesInCountriesKml(@QueryParam("places") ArrayList<String> places, @QueryParam("countries") ArrayList<String> countries) {
        GeoResult geoResult = new GeoResult();
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
                ArrayList<GeoLookup> geoLookups = new ArrayList<GeoLookup>();

                while (results.next()) {
                    GeoLookup geo = new GeoLookup();
                    geo.setPlacename(results.getString(1));
                    geo.setCountry(results.getString(2));
                    geo.setLatitude(results.getDouble(3));
                    geo.setLongitude(results.getDouble(4));
                    geoLookups.add(geo);
                }

                geoResult.setGeos(geoLookups);

                String kml = GeoKmlUtil.createGeoLookupKml(geoLookups);
                return Response.status(Response.Status.OK).type(MediaType.TEXT_XML).entity(kml).build();

            } else {
                return Response.status(Response.Status.OK).type(MediaType.TEXT_XML).entity("</>").build();
            }
        } catch (Exception e) {
                throw new WebApplicationException(
                        Response
                        .status(Response.Status.BAD_REQUEST)
                        .type(MediaType.TEXT_XML)
                        .entity("<Exception searching DB :: " + " (" + e.getMessage() + ")/>")
                        .build());
            }
    }

    
    
    // Get all places inside a list of bounding polygons
    @POST
    @Path("/places/polygon")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlacesInPolygonJson(ArrayList<String> polygonList) {

        if (polygonList.isEmpty() || polygonList == null) {
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("Input polygonCoords was blank").build();

        } else {
            try {
                GeoResult geoResult = new GeoResult();
                ArrayList<GeoResult> geoResults = new ArrayList<GeoResult>();

                // create a list of GeoResult objects, one for each polygon
                for (String polygon : polygonList) {
                    try {
                        GeoResult geo = this.areaGeoSearch(polygon);
                        if (geo != null) {
                            geoResults.add(geo);
                        }
                    } catch (Exception e) {
                        System.out.println("Skipping input::" + polygon + "Due to an excpetion querying the PostgresDB ::\n" + e.getStackTrace().toString());
                    }
                }

                // process results
                if (geoResults.isEmpty() == true) {
                    return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity("{}").build();
                } else {
                    // now return JSON string of results
                    ObjectMapper mapper = new ObjectMapper();
                    return Response.status(Response.Status.OK).type(MediaType.APPLICATION_JSON).entity(mapper.writeValueAsString(geoResults)).build();
                }

            } catch (Exception e) {
                throw new WebApplicationException(
                        Response
                        .status(Response.Status.BAD_REQUEST)
                        .type(MediaType.APPLICATION_JSON)
                        .entity("Exception searching DB :: " + " (" + e.getMessage() + ")")
                        .build());
            }
        }
    }

    @POST
    @Path("/places/polygon")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_XML)
    public Response getPlacesInPolygonKml(ArrayList<String> polygonList) {

        if (polygonList.isEmpty() || polygonList == null) {
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity("Input polygonCoords was blank").build();

        } else {
            try {
                GeoResult geoResult = new GeoResult();
                ArrayList<GeoResult> geoResults = new ArrayList<GeoResult>();

                // create a list of GeoResult objects, one for each polygon
                for (String polygon : polygonList) {
                    try {
                        GeoResult geo = this.areaGeoSearch(polygon);
                        if (geo != null) {
                            geoResults.add(geo);
                        }
                    } catch (Exception e) {
                        System.out.println("Skipping input::" + polygon + "Due to an excpetion querying the PostgresDB ::\n" + e.getStackTrace().toString());
                    }
                }

                String kml = GeoKmlUtil.createAreaSearchKml(geoResults);
                return Response.status(Response.Status.OK).type(MediaType.TEXT_XML).entity(kml).build();

            } catch (Exception e) {
                throw new WebApplicationException(
                        Response
                        .status(Response.Status.BAD_REQUEST)
                        .type(MediaType.TEXT_XML)
                        .entity("<Exception searching DB :: " + " (" + e.getMessage() + ")/>")
                        .build());
            }
        }
    }

    
    
    // utility methods to validate input
    private ArrayList<String> getValidPolyCoords(String polyCoordList) {
        // assume input is (#,#),(#,#), ... , (#,#) for a polygon
        ArrayList<String> validPolygonCoords = new ArrayList<String>();

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

                    validPolygonCoords.add(polyCoords);

                }

                return validPolygonCoords;

            }
        }
    }

    private GeoResult areaGeoSearch(String polygon) {

        GeoResult geoResult = new GeoResult();

        if (polygon.equals("") == true) {
            return null;

        } else {
            ArrayList<String> polyCoords = this.getValidPolyCoords(polygon);

            String polySearchSQL = "";

            polySearchSQL = "SELECT name, country, lat, lon FROM geoname, country WHERE ( (country=iso) AND polygon ";

            ArrayList<SimpleCoord> polyCoordinates = new ArrayList<SimpleCoord>();

            GeoArea poly = new GeoArea();

            String coordList = "";
            for (String coord : polyCoords) {

                polyCoordinates.add(new SimpleCoord(coord));

                if (coordList.equals("") == true) {
                    coordList = coord;
                } else {
                    coordList = coordList + " , " + coord;
                }
            }

            // create object to hold the polygon, and add to GeoResult object
            poly.setCoordinates(polyCoordinates);

            geoResult.setSearchPoly(poly);


            // process the SQL query
            coordList = " \' ( " + coordList + " ) \' ";
            polySearchSQL = polySearchSQL + coordList + " @> point_location )";

            System.out.println("SQL statement = \n" + polySearchSQL + "\n");

            try {

                ArrayList<GeoLookup> geoLookups = new ArrayList<GeoLookup>();

                Statement polyPlace = connection.createStatement();

                ResultSet results = polyPlace.executeQuery(polySearchSQL);

                if (results != null && results.next() == true) {

                    while (results.next()) {
                        GeoLookup geo = new GeoLookup();
                        geo.setPlacename(results.getString(1));
                        geo.setCountry(results.getString(2));
                        geo.setLatitude(results.getDouble(3));
                        geo.setLongitude(results.getDouble(4));
                        geoLookups.add(geo);
                    }
                }

                geoResult.setGeos(geoLookups);
                return geoResult;

            } catch (Exception e) {
                return null;
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
        inputValues = inputValues.replace(")", "");
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
