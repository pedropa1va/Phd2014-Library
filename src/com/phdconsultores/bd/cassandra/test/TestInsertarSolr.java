package com.phdconsultores.bd.cassandra.test;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Stopwatch;


public class TestInsertarSolr {

	public static void main(String[] args) throws IOException,
			SolrServerException {
		String host = "192.168.0.100";
		String keyspace = "saren";
		String urlString = "http://192.168.0.100:8983/solr/test";
		Row row;
		int n=100;
		ResultSet rs = com.phdconsultores.bd.cassandra.phdconsultoresCassandra
				.Consultar(
						"select * from saren.publico where tipo_registro='PersonaComun' limit "+n+" ALLOW FILTERING",
						host, keyspace);
		Stopwatch timer = Stopwatch.createStarted();
		while(rs.isExhausted() == false){
			row=rs.one();			
					com.phdconsultores.bd.cassandra.phdconsultoresCassandra.insertarSolr(urlString,row);
		}
		System.out.println("Tiempo de ejecución del metodo insertarSolr para "+n+" de registros: " + timer.stop());	
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesion();
		System.exit(0);
	}

}