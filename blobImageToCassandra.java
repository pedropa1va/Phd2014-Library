package com.phdconsultores.bd.cassandra;


import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class phconsultoresCassandra {
	

	private static Session cassandraSession = null;
	private static String hostName = null;
	private static String keyspaceName = null;

	phconsultoresCassandra() {
	}

	private static Session obtenerSesion() {

		if (cassandraSession == null) {
			cassandraSession = crearSesion();
		}

		return cassandraSession;

	}

	private static Session crearSesion() {
		Cluster cluster = Cluster.builder()
				.addContactPoint(hostName)
				.withQueryOptions(new QueryOptions().setFetchSize(100))
				.withSocketOptions(new SocketOptions().setReadTimeoutMillis(999999999).setConnectTimeoutMillis(999999999))
                .withCredentials("etl", "etl2014#")	
				.build();	
		return cluster.connect(keyspaceName);
	}

	public static ResultSet Consultar(String consulta, String host,
			String keyspace) {
		
		hostName = host;
		keyspaceName = keyspace;
		ResultSet results = phconsultoresCassandra.obtenerSesion()
				.execute(consulta);
		
		return results;
	}
	
	public static ResultSetFuture ConsultarAsincrono(String consulta, String host,
			String keyspace) {
		
		hostName = host;
		keyspaceName = keyspace;
		ResultSetFuture results = phconsultoresCassandra.obtenerSesion()
				.executeAsync(consulta);
		
		return results;
	}

	public static ResultSet Modificar(String update, String host,
			String keyspace) {
		
		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phconsultoresCassandra.obtenerSesion()
				.prepare(update);
		BoundStatement boundStatement = preparedStatement.bind();
		return phconsultoresCassandra.obtenerSesion().execute(boundStatement);
	}
	
	public static ResultSetFuture ModificarAsincono(String update, String host,
			String keyspace) {
		
		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phconsultoresCassandra.obtenerSesion()
				.prepare(update);
		BoundStatement boundStatement = preparedStatement.bind();
		
		return phconsultoresCassandra.obtenerSesion().executeAsync(boundStatement);
	}
	
	public static ResultSet Insertar(String insert, String host,
			String keyspace) {
		
		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phconsultoresCassandra.obtenerSesion()
				.prepare(insert);
		BoundStatement boundStatement = preparedStatement.bind();
		return phconsultoresCassandra.obtenerSesion().execute(boundStatement);
	}

	public static ResultSetFuture InsertarAsincrono(String insert, String host,
			String keyspace) {
		
		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phconsultoresCassandra.obtenerSesion()
				.prepare(insert);
		BoundStatement boundStatement = preparedStatement.bind();
		return phconsultoresCassandra.obtenerSesion().executeAsync(boundStatement);
	}
	/**
	 * Funcion especial para insertar las imagenes en Cassandra
	 *
	 * */
	public static ResultSet guardarImagen(String host, 
			String keyspace, String columnfamily,String clave ,String oficina,String tipo_registro,
			byte[] imagen, String insert ) throws IOException, SQLException{
		
		hostName = host;
		keyspaceName = keyspace;

		//Se prepara la sentencia insert y se insertan todos los campos menos la imagen
		PreparedStatement preparedStatement = phconsultoresCassandra.obtenerSesion()
				.prepare(insert);
		BoundStatement boundStatement = preparedStatement.bind();
		phconsultoresCassandra.obtenerSesion().execute(boundStatement);
		
		//Se prepara la imagen para hacerle el update
		
		ByteBuffer byteBuffer = ByteBuffer.wrap(imagen); //cassandra solo acepta Bytebuffer para imagenes
		Statement statement = QueryBuilder.update(keyspace, columnfamily)
								.with(QueryBuilder.set("imagen", byteBuffer))
								.where(QueryBuilder.eq("clave", clave))
								.and(QueryBuilder.eq("tipo_registro", tipo_registro))
								.and(QueryBuilder.eq("oficina",oficina ));
		
		
		 return	obtenerSesion().execute(statement);
			
		
		
		
	}

	
				
}