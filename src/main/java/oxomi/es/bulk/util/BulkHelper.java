/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package oxomi.es.bulk.util;

import oxomi.es.bulk.Main;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Provides methods for communicating the bulk REST service of an Elasticsearch instance.
 * Server parameters are read from the config.
 */
@Register(classes = BulkHelper.class)
public class BulkHelper {

    /**
     * Imports the data contained in the file into the given Elasticsearch instance using the bulk API.
     * <p>
     * Specifications for the provided data can be found at the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">ES
     * Docs Bulk API Chapter</a>.
     *
     * @param instance the Elasticsearch server instance
     * @param sourcePath the relative path to the JSON data file
     * @param batchsize the size of one import batch (default: 20000)
     * @return <b>true</b> if the data was successfully imported, <b>false</b> otherwise
     */
    public boolean importFile(ESInstance instance, String sourcePath, int batchsize) {
        try {
            File file = new File(sourcePath);
            long size = file.length();

            if (!file.isFile() || size <= 0) {
                Main.LOG.SEVERE("Please provide a valid file. The provided file did not exist or was empty.");
                return false;
            }

            Counter currentBatch = new Counter();
            long progress = 0;
            int lastPercent = -1;
            String line;
            StringBuilder batch = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                while ((line = br.readLine()) != null) {
                    batch.append(line).append("\n");
                    if (currentBatch.inc() >= batchsize) {
                        if (!importBatch(instance, batch.toString().getBytes())) {
                            return false;
                        }
                        progress += batch.length();
                        int percent = (int) (progress * 100 / size);
                        if (percent > lastPercent) {
                            Main.LOG.INFO("Progress: " + percent + "%");
                            lastPercent = percent;
                        }
                        batch.setLength(0);
                        currentBatch.reset();
                    }
                }
            }
            return true;
        } catch (IOException e) {
            Exceptions.handle(e);
        }
        return false;
    }

    /**
     * Uploads the data of one batch of the file to the provided Elasticsearch instance.
     *
     * @param instance the Elasticsearch server instance
     * @param data the binary data of the batch
     * @return <b>true</b> if the batch was imported successfully, <b>false</b> otherwise
     */
    private boolean importBatch(ESInstance instance, byte[] data) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) instance.getUri().openConnection();
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setUseCaches(false);
            conn.setAllowUserInteraction(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            conn.setRequestProperty("Content-Length", Integer.toString(data.length));

            try (DataOutputStream os = new DataOutputStream(conn.getOutputStream())) {
                os.write(data);
                os.flush();
                os.close();
            }
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                conn.disconnect();
                Main.LOG.SEVERE("Failed: HTTP error code: " + conn.getResponseCode() + " " + conn.getResponseMessage());
                return false;
            }
            return true;
        } catch (Exception e) {
            Exceptions.handle(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return false;
    }
}
