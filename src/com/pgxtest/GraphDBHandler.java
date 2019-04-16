package com.pgxtest;

import oracle.pgx.api.*;
import org.openscience.cdk.interfaces.IAtom;
import org.openscience.cdk.interfaces.IAtomContainer;
import org.openscience.cdk.interfaces.IBond;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GraphDBHandler {

    PgxSession session;

    public GraphDBHandler(String connectionString) {
        try {
            ServerInstance instance = Pgx.getInstance(connectionString);
            session = instance.createSession("test");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String getBondCharacter(IBond bond) throws IOException {
        if (bond.isAromatic()) {
            return "A";
        }
        if (bond.getOrder().equals(IBond.Order.SINGLE)){
            return "S";
        }
        if (bond.getOrder().equals(IBond.Order.DOUBLE)) {
            return "D";
        }
        if (bond.getOrder().equals(IBond.Order.TRIPLE)) {
            return "T";
        }
        if (bond.getOrder().equals(IBond.Order.QUADRUPLE)) {
            return "Q";
        }
        if (bond.getOrder().equals(IBond.Order.QUINTUPLE)) {
            return "QI";
        }
        if (bond.getOrder().equals(IBond.Order.SEXTUPLE)) {
            return "SE";
        }

        throw new IOException("Unknown type of bond");
    }

    public PgxGraph buildDb(HashMap<String, IAtomContainer> db) throws Exception {
        GraphBuilder<Integer> builder = session.createGraphBuilder();
        int atomId = 0;

        for (Map.Entry<String, IAtomContainer> entry : db.entrySet()) {
            String graphId = entry.getKey();
            IAtomContainer molecule = entry.getValue();

            for (IAtom atom : molecule.atoms()) {
                atom.setID(Integer.toString(atomId));
                if (atom.isAromatic()) {
                    atom.setSymbol(atom.getSymbol().toLowerCase());
                }

                builder.addVertex(atomId).addLabel(atom.getSymbol()).setProperty("graphId", graphId);
                atomId++;
            }

            for (IBond bond : molecule.bonds()) {
                int atom1Id = Integer.parseInt(bond.getAtom(0).getID());
                int atom2Id = Integer.parseInt(bond.getAtom(1).getID());
                builder.addEdge(atom1Id, atom2Id).setLabel(getBondCharacter(bond));
            }
        }

        return builder.build();

//        PgxGraph graph = builder.build();

        //PgqlResultSet result = graph.queryPgql("SELECT DISTINCT a1.graphId MATCH (a1:c)-[:X]-(a2:c)-[:X]-(a3:c)-[:X]-(a4:c)");
//        PgqlResultSet result = graph.queryPgql("SELECT DISTINCT a1.graphId MATCH (a1:c)-[:A]-(a2:c), (a2:c)-[:A]-(a3:c), (a3:c)-[:A]-(a4:c) WHERE a1 <> a2 AND a1 <> a3 AND a1 <> a4 AND a2 <> a3 AND a2 <> a4 AND a3 <> a4");
        //PgqlResultSet result = graph.queryPgql("SELECT COUNT(a) MATCH (a)-[e:xxx|xx]->(b)-[:yy]->(c)  WHERE a.label = 1");

//        result.print();
//        System.out.println(result.getNumResults());
    }

    public PgqlResultSet executeQuery(IAtomContainer queryGraph, PgxGraph db) throws Exception {

        //TODO
        StringBuilder query = new StringBuilder();
        int atomId = 0;
        query.append("SELECT DISTINCT v0.graphId MATCH ");

        for (IAtom atom : queryGraph.atoms()) {
            atom.setID(Integer.toString(atomId++));
            if (atom.isAromatic()) {
                atom.setSymbol(atom.getSymbol().toLowerCase());
            }
        }

        for (IBond bond : queryGraph.bonds()) {
            query.append("(v");
            query.append(bond.getAtom(0).getID());
            query.append(":");
            query.append(bond.getAtom(0).getSymbol());
            query.append(")-[:");
            query.append(getBondCharacter(bond));
            query.append("]-(v");
            query.append(bond.getAtom(1).getID());
            query.append(":");
            query.append(bond.getAtom(1).getSymbol());
            query.append("), ");
        }

        //We need to remove last comma
        query.setLength(query.length() - 2);
        query.append(" WHERE ");

        for (IAtom atom1 : queryGraph.atoms()) {
            for (IAtom atom2 : queryGraph.atoms()) {
                if (atom1.getID().compareTo(atom2.getID()) < 0) {
                    query.append("v");
                    query.append(atom1.getID());
                    query.append(" <> v");
                    query.append(atom2.getID());
                    query.append(" AND ");
                }
            }
        }

        //We need to remove last " AND "
        query.setLength(query.length() - 5);

        return db.queryPgql(query.toString());
    }
}
