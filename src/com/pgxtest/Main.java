package com.pgxtest;

import oracle.pgx.api.*;
import oracle.pgx.config.Format;
import org.openscience.cdk.DefaultChemObjectBuilder;
import org.openscience.cdk.exception.InvalidSmilesException;
import org.openscience.cdk.interfaces.IAtomContainer;
import org.openscience.cdk.smiles.SmilesParser;
import shared.QueryUtils;

import java.io.FileNotFoundException;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) {
        shared.SDFParser parser = new shared.SDFParser("../chembl_test1.sdf");
        HashMap<String, IAtomContainer> db = null;

        try {
            db = parser.getDB("chembl_id");
        } catch (FileNotFoundException exc) {
            System.out.println("DB file not found");
        }

        IAtomContainer queryContainer = null;
        String query = QueryUtils.query4_1;

        try {
            SmilesParser sp  = new SmilesParser(DefaultChemObjectBuilder.getInstance());
            queryContainer = sp.parseSmiles(query);
        } catch (InvalidSmilesException e) {
            System.err.println(e.getMessage());
        }

        GraphDBHandler dbHandler = new GraphDBHandler("http://localhost:7007/pgx");

        try {
            PgxGraph graphDB = dbHandler.buildDb(db);
            PgqlResultSet result = dbHandler.executeQuery(queryContainer, graphDB);
            result.print();
            System.out.println(result.getNumResults());

        } catch(Exception e) {
            System.err.println(e.getMessage());
        }


        /*ServerInstance instance = Pgx.getInstance("http://localhost:7007/pgx");

        try {
            PgxSession session = instance.createSession("xyz");
            //PgxGraph graph = session.readGraphWithProperties("sample.adj.json");

            GraphBuilder<Integer> builder = session.createGraphBuilder();

            builder.addVertex(0).setProperty("label", 1).setProperty("id", 0);
            builder.addVertex(1).setProperty("label", 0).setProperty("id", 0);
            builder.addVertex(2).setProperty("label", 0).setProperty("id", 0);

            builder.addEdge(0, 1).setLabel("xx");
            builder.addEdge(1, 2).setLabel("yy");

            builder.addVertex(3).setProperty("label", 1).setProperty("id", 1);
            builder.addVertex(4).setProperty("label", 0).setProperty("id", 1);
            builder.addVertex(5).setProperty("label", 0).setProperty("id", 1);
            builder.addVertex(6).setProperty("label", 0).setProperty("id", 1);

            builder.addEdge(3, 4).setLabel("xx");
            builder.addEdge(4, 5).setLabel("yy");
            builder.addEdge(5, 6).setLabel("xx");
//            builder.addEdge(2, 1).setLabel("yy");
            //builder.addEdge(2, 0).setLabel("-");
            //builder.addEdge(3, 0).setLabel("-");

            PgxGraph graph = builder.build();

            //graph.store(Format.ADJ_LIST)

            PgqlResultSet result = graph.queryPgql("SELECT DISTINCT a.id MATCH (a)-[:yy]-(b)-[e:xxx|xx]-(c)");
            //PgqlResultSet result = graph.queryPgql("SELECT COUNT(a) MATCH (a)-[e:xxx|xx]->(b)-[:yy]->(c)  WHERE a.label = 1");

            result.print();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }*/
    }
}
