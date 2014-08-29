package org.apache.uima.mongo;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;
import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.mongo.MongoCollectionReader.PARAM_DB_CONNECTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.JUnitExtension;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.test.Token;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class MongoTest {

    private static XMLParser parser = UIMAFramework.getXMLParser();

    static String[] getTestConn(String testName) {
        return new String[] { "localhost", "uima_testing",
                "test_" + testName + "_" + System.currentTimeMillis(), "", "" };
    }

    private static String[] conn;
    private static TypeSystemDescription tsd;
    private static CASImpl cas;

    @BeforeClass
    public static void before() throws Exception {

        conn = getTestConn("MongoTest");

        File tsdFile = JUnitExtension.getFile("Mongo/desc/allTypes.xml");
        tsd = parser.parseTypeSystemDescription(new XMLInputSource(tsdFile));
        cas = (CASImpl) CasCreationUtils.createCas(tsd, null, null);
    }

    @Test
    public void testWriteRead() throws Exception {

        JCas jCas = cas.getJCas();
        jCas.setDocumentText("this is just a test");
        Token t = new Token(jCas, 0, 4);
        t.addToIndexes();
        runPipeline(cas,
                createEngine(MongoWriter.class, PARAM_DB_CONNECTION, conn));

        Iterator<JCas> iterator = iterator(createReader(
                MongoCollectionReader.class, PARAM_DB_CONNECTION, conn));
        assertTrue(iterator.hasNext());
        JCas next = iterator.next();
        assertEquals("this is just a test", next.getDocumentText());
        Collection<Token> tokens = select(jCas, Token.class);
        assertEquals(1, tokens.size());
    }

    public static Iterator<JCas> iterator(final CollectionReader cr) {

        return new Iterator<JCas>() {

            public boolean hasNext() {
                try {
                    return cr.hasNext();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            public JCas next() {
                try {
                    CAS cas;
                    cas = CasCreationUtils.createCas(cr
                            .getProcessingResourceMetaData());
                    cr.getNext(cas);
                    return cas.getJCas();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            public void remove() {
                throw new NotImplementedException();
            }
        };
    }
}
