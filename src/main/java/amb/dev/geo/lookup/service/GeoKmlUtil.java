package amb.dev.geo.lookup.service;

import de.micromata.opengis.kml.v_2_2_0.Boundary;
import de.micromata.opengis.kml.v_2_2_0.ColorMode;
import de.micromata.opengis.kml.v_2_2_0.Coordinate;
import de.micromata.opengis.kml.v_2_2_0.Document;
import de.micromata.opengis.kml.v_2_2_0.Icon;
import de.micromata.opengis.kml.v_2_2_0.Kml;
import de.micromata.opengis.kml.v_2_2_0.LinearRing;
import de.micromata.opengis.kml.v_2_2_0.Style;
import dev.amb.geo.data.output.GeoLookup;
import dev.amb.geo.data.output.GeoResult;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;

/**
 *
 * @author AntB
 */
public class GeoKmlUtil {

    private final static String styleRef = "lookupStyle";
    // Geo methods

    public static String createAreaSearchKml(GeoResult geos) throws FileNotFoundException {

        Kml kml = new Kml();
        Document doc = kml.createAndSetDocument();
        doc = GeoKmlUtil.addDefaultStyle(doc);

        // we want a polygon, and corresponding placemarks
        Boundary bound = new Boundary();
        LinearRing lr = bound.createAndSetLinearRing();

        for (String kmlCoord : geos.getKmlPolygon()) {
            lr.getCoordinates().add(new Coordinate(kmlCoord));
        }

        for (GeoLookup geo : geos.getGeos()) {
            doc.createAndAddPlacemark().withStyleUrl(styleRef).withOpen(Boolean.TRUE).
                    withName(geo.getPlacename()).createAndSetPoint().addToCoordinates(geo.getKmlCoord());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);
        kml.marshal(ps);
        String kmlString = baos.toString();

        return kmlString;

    }

    public static String createAreaSearchKml(ArrayList<GeoResult> geosIn) throws FileNotFoundException {

        Kml kml = new Kml();
        Document doc = kml.createAndSetDocument();

        doc = GeoKmlUtil.addDefaultStyle(doc);

        for (GeoResult geos : geosIn) {
            // we want a polygon, and corresponding placemarks
            Boundary bound = new Boundary();
            LinearRing lr = bound.createAndSetLinearRing();

            for (String kmlCoord : geos.getKmlPolygon()) {
                lr.getCoordinates().add(new Coordinate(kmlCoord));
            }

            for (GeoLookup geo : geos.getGeos()) {
                doc.createAndAddPlacemark().withStyleUrl(styleRef).withOpen(Boolean.TRUE).
                        withName(geo.getPlacename()).createAndSetPoint().addToCoordinates(geo.getKmlCoord());
            }
        }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos, true);
            kml.marshal(ps);
            String kmlString = baos.toString();

            return kmlString;
    }

    public static String createGeoLookupKml(ArrayList<GeoLookup> geos) throws FileNotFoundException {
        Kml kml = new Kml();
        Document doc = kml.createAndSetDocument();
        doc = GeoKmlUtil.addDefaultStyle(doc);

        // placemark for each GeoLookup result
        for (GeoLookup geo : geos) {
            doc.createAndAddPlacemark().withStyleUrl(styleRef).withOpen(Boolean.TRUE).
                    withName(geo.getPlacename()).createAndSetPoint().addToCoordinates(geo.getKmlCoord());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);
        kml.marshal(ps);
        String kmlString = baos.toString();

        return kmlString;
    }    
    
    
    // private utils
    private static Document addDefaultStyle(Document doc) {
        Style style = doc.createAndAddStyle().withId(styleRef);
        style.createAndSetIconStyle().withColor("ff00fcfc").withScale(1.35).withIcon(new Icon().withHref("http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png"));
        style.createAndSetLabelStyle().withColor("ff0000ff").withScale(1.5);
        style.createAndSetLineStyle().withColor("ffff0000").withWidth(11);
        style.createAndSetPolyStyle().withColor("bfffbaa5").withColorMode(ColorMode.NORMAL);

        return doc;
    }
}
