/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package oxomi.es.bulk.util;

import sirius.kernel.Sirius;
import sirius.kernel.health.Exceptions;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Small helper POJO for storing the connection data of an Elasticsearch instance.
 */
public class ESInstance {

    private static final String URI_PATTERN = "%s:%s/%s/_bulk";

    private String host;

    private int port;

    private String index;

    private URL uri;

    public ESInstance() {
        this(Sirius.getConfig().getString("host"), Sirius.getConfig().getInt("port"), Sirius.getConfig().getString("index"));
    }

    public ESInstance(String host, int port, String index) {
        this.host = host;
        this.port = port;
        this.index = index;
        try {
            uri = new URL(String.format(URI_PATTERN, host, port, index));
        } catch (MalformedURLException e) {
            Exceptions.handle(e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getIndex() {
        return index;
    }

    public URL getUri() {
        return uri;
    }
}
