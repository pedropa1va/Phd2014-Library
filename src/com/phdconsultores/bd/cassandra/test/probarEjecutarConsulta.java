package com.phdconsultores.bd.cassandra.test;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class probarEjecutarConsulta {
	// private static modelRow registro = new modelRow();

	public static void main(String[] args) throws IOException,
			SolrServerException {
		// String consulta = "SELECT * FROM saren.mercantil limit 2;";
		String host = "192.168.0.100";
		String keyspace = "saren";
		String json = null;
		String urlString = "http://192.168.0.100:8983/solr/test";
		ResultSet rs = com.phdconsultores.bd.cassandra.phdconsultoresCassandra
				.Consultar(
						"select * from saren.publico where tipo_registro='PersonaComun' limit 1000000 ALLOW FILTERING",
						host, keyspace);

		for (Row row : rs) {
			if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
				rs.fetchMoreResults();			
					// com.phdconsultores.bd.cassandra.phdconsultoresCassandra.insertarSolr(urlString,row);
			/*com.phdconsultores.bd.cassandra.phdconsultoresCassandra
					.insertarSolr(urlString, row);*/
		}
		// String json =
		// com.phdconsultores.bd.cassandra.phdconsultoresCassandra.toJsonFull(rs);
		 //System.out.println(json);
		//com.phdconsultores.bd.cassandra.phdconsultoresCassandra.insertarMongodb();
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesion();
		
		System.exit(0);
	}

}