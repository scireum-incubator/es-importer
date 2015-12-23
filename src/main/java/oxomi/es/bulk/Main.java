/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package oxomi.es.bulk;

import com.typesafe.config.Config;
import org.apache.log4j.Level;
import oxomi.es.bulk.util.BulkHelper;
import oxomi.es.bulk.util.ESInstance;
import sirius.kernel.Setup;
import sirius.kernel.Sirius;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;

/**
 * Starting point of the OXOMI CLI importer.
 * Checks all provided and needed parameters and initializes the restore process.
 */
public class Main {

    public static final Log LOG = Log.get("es-importer");

    @Part
    private static BulkHelper bulk;

    @ConfigValue("source")
    private static String file;

    @ConfigValue("batchsize")
    private static int batchsize;

    private Main() {
    }

    public static void main(String[] args) {
        try {
            Setup setup = new Setup(Setup.Mode.PROD, Main.class.getClassLoader());
            setup.withDefaultLogLevel(Level.WARN).withConsoleLogFormat("%c - %m%n").withLogToConsole(true).withLogToFile(
                    false);
            Sirius.start(setup);
            LOG.INFO("started ESImporter");

            Config c = Sirius.getConfig();

            ESInstance instance = new ESInstance();

            if (Strings.isEmpty(instance.getHost())) {
                LOG.SEVERE("Please provide the host url of your Elasticsearch installation.");
                return;
            }

            if (Strings.isEmpty(instance.getPort())) {
                LOG.SEVERE("Please provide the port of your Elasticsearch installation.");
                return;
            }

            if (Strings.isEmpty(instance.getIndex())) {
                LOG.SEVERE("Please provide the name of the target-index for the bulk import.");
                return;
            }

            if (Strings.isEmpty(file)) {
                LOG.SEVERE("Please provide a source json file with your data.");
                return;
            }
            Watch w = Watch.start();
            if (bulk.importFile(instance, file, batchsize)) {
                LOG.INFO("Completed import in " + w.duration());
            } else {
                LOG.INFO("Import failed");
            }
        } catch (Throwable t) {
            Exceptions.handle(t);
        } finally {
            LOG.INFO("finished ESImporter");
            Sirius.stop();
        }
    }
}
