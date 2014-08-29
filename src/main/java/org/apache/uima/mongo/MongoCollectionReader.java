package org.apache.uima.mongo;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.uima.UimaContext;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;

import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Reads CASes from Mongo
 * 
 * @author renaud.richardet@epfl.ch
 */
public class MongoCollectionReader extends JCasCollectionReader_ImplBase {

    public static final String PARAM_DB_CONNECTION = "mongo_connection";
    @ConfigurationParameter(name = PARAM_DB_CONNECTION, //
    description = "host, dbname, collectionname, user, pw")
    protected String[] db_connection;
    protected DBCursor cur;

    public static final String PARAM_QUERY = "mongo query";
    @ConfigurationParameter(name = PARAM_QUERY, mandatory = false, //
    description = "a mongo query, e.g. {my_db_field:{$exists:true}} or {ftr.ns:1} or {pmid: 17} "
            + "or {pmid:{$in:[12,17]}} or {pmid:{ $gt: 8, $lt: 11 }} ")
    private String query = null;

    @Override
    public void initialize(UimaContext context)
            throws ResourceInitializationException {
        super.initialize(context);
        try {
            MongoConnection conn = new MongoConnection(db_connection);
            initQuery(conn);
        } catch (IOException e) {
            throw new ResourceInitializationException(e);
        }
    }

    protected void initQuery(MongoConnection conn) throws IOException {
        if (query != null)
            cur = conn.coll.find((DBObject) JSON.parse(query));
        else
            cur = conn.coll.find();
        cur.addOption(Bytes.QUERYOPTION_NOTIMEOUT).batchSize(1000);
    }

    public boolean hasNext() throws IOException, CollectionException {
        return cur.hasNext();
    }

    @Override
    public void getNext(JCas jCas) throws IOException, CollectionException {

        try {

            DBObject doc = cur.next();
            String json = JSON.serialize(doc);
            XmiCasDeserializer.deserialize(
                    new ByteArrayInputStream(json.getBytes(UTF_8)),
                    jCas.getCas());
        } catch (Exception e) {
            throw new CollectionException(e);
        }
    }

    public Progress[] getProgress() {
        return null;// nope
    }
}
