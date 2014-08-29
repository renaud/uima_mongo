package org.apache.uima.mongo;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Writes annotations to MongoDB
 * 
 * @author renaud.richardet@epfl.ch
 */
public class MongoWriter extends JCasAnnotator_ImplBase {

    @ConfigurationParameter(name = MongoCollectionReader.PARAM_DB_CONNECTION, mandatory = true, //
    description = "host, dbname, collectionname, user, pw")
    private String[] db_connection;

    public static final String PARAM_CONNECTION_SAFE_MODE = "safeMode";
    @ConfigurationParameter(name = PARAM_CONNECTION_SAFE_MODE, defaultValue = "true", //
    description = "Mongo's WriteConcern SAFE(true) or NORMAL(false)")
    private boolean safeMode;

    private DBCollection coll;

    private XmiCasSerializer xcs;

    @Override
    public void initialize(UimaContext context)
            throws ResourceInitializationException {
        super.initialize(context);
        xcs = new XmiCasSerializer(null);

        try {
            MongoConnection conn = new MongoConnection(db_connection, safeMode);
            coll = conn.coll;
        } catch (IOException e) {
            throw new ResourceInitializationException(e);
        }
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        try {

            StringWriter sw = new StringWriter();
            xcs.serializeJson(jCas.getCas(), sw);

            DBObject doc = (DBObject) JSON.parse(sw.toString());

            coll.insert(doc);

        } catch (Throwable t) {
            throw new AnalysisEngineProcessException(t);
        }
    }
}