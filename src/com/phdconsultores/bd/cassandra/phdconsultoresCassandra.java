package com.phdconsultores.bd.cassandra;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.bson.Document;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SocketOptions;
import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * @author Josemy Duarte
 *
 */
public class phdconsultoresCassandra{
	/* Casandra */
	private static Session cassandraSession = null;
	private static String hostName = null;
	private static String keyspaceName = null;
	/**/
	/* Mongo */
	private static MongoClient MongoCliente = null;
	private static MongoDatabase MongoDatabase = null;
	private static MongoCollection<Document> MongoCollection = null;
	private static String MongoIP = null;
	private static int MongoPuerto = 0;
	private static String MongoBD = null;
	private static String MongoColeccion = null;
	/**/
	/* Solr */
	private static SolrClient SolrCliente = null;
	private static String SolrURL = null;
	private static SimpleDateFormat FormatoSolr = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss'Z'");
	/**/
	/* General */
	private static SimpleDateFormat formato = new SimpleDateFormat("YYYY-MM-DD");
	private static Map<String, String> claves = new LinkedHashMap<String, String>();
	//private static Map<String, String> valores = new LinkedHashMap<String, String>();
	private static Gson gson = new Gson(); /*Considerar usar Jackson*/
	/**/
	private static Date fecha;
	
	public phdconsultoresCassandra() {
	}

	/**
	 * Verifica que no se haya iniciado previamente alguna sesion
	 */
	private static void obtenerSesionMongo() {

		if (MongoCliente == null) {
			crearSesionMongo();
		}

	}

	/**
	 * Cierra la conexion con el cliente de Mongo
	 */
	public static void cerrarSesionMongo() {

		if (MongoCliente != null) {
			MongoCliente.close();
			MongoCliente=null;
		}
	}
	/**
	 * Metodo llamado solo cuando no existe previamente una sesion.
	 * Se encarga de crear la conexion con el servidor de MongoDB
	 */
	private static void crearSesionMongo() {
		MongoCliente = new MongoClient(MongoIP, MongoPuerto);
		MongoDatabase = MongoCliente.getDatabase(MongoBD);
		MongoCollection = MongoDatabase.getCollection(MongoColeccion);
	}

	/**
	 * Accion para insertar un row de Cassandra dentro de MongoDB
	 * 
	 * @param ip
	 *            IP del hosting donde se encuentra MongoDB levantado
	 * @param puerto
	 *            Puerto en el que se encuentra MongoDB levantado
	 * @param BD
	 *            Nombre de la BD dentro de MongoDB a utilizar
	 * @param Coleccion
	 *            Nombre de la coleccion dentro de MongoDB a utilizar
	 * @param row
	 * 			  Registro de Cassandra a insertar en MongoDB
	 */
	public static void insertarMongodb(String ip, int puerto, String bd,
			String coleccion, Row row) {
		MongoIP = ip;
		MongoPuerto = puerto;
		MongoBD = bd;
		MongoColeccion = coleccion;
		obtenerSesionMongo();
		Document documento = new Document("id", row.getString("clave"))
				.append("clave", row.getString("clave"))
				.append("tipo_registro", row.getString("tipo_registro"))
				.append("creado_por", row.getString("creado_por"))
				.append("fecha_creacion",
						formato.format(row.getDate("fecha_creacion"))
								+ "T00:00:00.0Z")
				.append("modificado_por", row.getString("modificado_por"))
				.append("fecha_ultima_modificacion",
						formato.format(row.getDate("fecha_ultima_modificacion"))
								+ "T00:00:00.0Z")
				.append("oficina", row.getString("oficina"));

		claves = row.getMap("valores", String.class, String.class);

		for (Map.Entry<String, String> entry : claves.entrySet()) {
			documento.append(entry.getKey(), entry.getValue());
		}

		MongoCollection.insertOne(documento);
		

	}

	private static void obtenerSesionSolr() {

		if (SolrCliente == null) {
			crearSesionSolr();
		}

	}

	private static void crearSesionSolr() {
		SolrCliente = new HttpSolrClient(SolrURL);		
	}
	
	public static void cerrarSesionSolr() throws IOException {
		if (SolrCliente != null) {
		SolrCliente.close();
		SolrCliente=null;
		}
	}

	/**
	 * Inserta todo el Resultset devuelto por Cassandra
	 * en Solr
	 * @param url Direccion de Solr con su respectivo core/collection
	 * @param rs resultset de Cassandra
	 * @return
	 * @throws IOException
	 * @throws SolrServerException
	 */
	public static boolean insertarSolrFull(String url, ResultSet rs) throws IOException,
	SolrServerException {
		SolrURL = url;
		//int i=0;
		obtenerSesionSolr();
		for (Row row : rs) {
			if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
				rs.fetchMoreResults();
			if(!insertarSolr(row)){
				cerrarSesionSolr();
				return false;
			}/*else{
				i++;
				if(i==100){
					SolrCliente.commit();
					i=0;
				}
			}*/			
		}
		SolrCliente.commit();
		//cerrarSesionSolr();
		return true;
	}
	
	
	/**
	 * Funcion que se encarga de indexar en Solr el contenido del parametro row.
	 * 
	 * @param row
	 * @return True si la indexación en Solr se ejecuto sin problema. False, en
	 *         caso contrario.
	 * @throws IOException
	 * @throws SolrServerException
	 */
	private static boolean insertarSolr(Row row) throws IOException,
			SolrServerException {
		
	//	private static SimpleDateFormat FormatoSolr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		SolrInputDocument SolrDocumento = new SolrInputDocument();
		SolrDocumento.addField("id", row.getString("clave"));
		SolrDocumento.addField("clave", row.getString("clave"));
		SolrDocumento.addField("tipo_registro", row.getString("tipo_registro"));
		SolrDocumento.addField("creado_por", row.getString("creado_por"));
		
		fecha = row.getDate("fecha_creacion");
		
	/*	if (fecha==null){
			SolrDocumento.addField("fecha_creacion",Calendar.getInstance().getTime());
		}else{*/
			SolrDocumento.addField("fecha_creacion",fecha);
	//	}

		SolrDocumento.addField("modificado_por",
				row.getString("modificado_por"));
		
		fecha = row.getDate("fecha_ultima_modificacion");	
			
		/*if (fecha==null){
			SolrDocumento.addField("fecha_ultima_modificacion",Calendar.getInstance().getTime());
		}else{*/
			SolrDocumento.addField("fecha_ultima_modificacion",fecha);
	//}	

		SolrDocumento.addField("oficina", row.getString("oficina"));

		claves = row.getMap("valores", String.class, String.class);

		for (Map.Entry<String, String> entry : claves.entrySet()) {
			SolrDocumento.addField(entry.getKey(), entry.getValue());
			
		}
		
		UpdateResponse response = SolrCliente.add(SolrDocumento);
		claves=null;
		if (response.getStatus() == 0) {
			//SolrCliente.commit();
			//SolrCliente.close();
			return true;
		} else {
			//SolrCliente.close();
			return false;
		}
	}
	
	/**
	 * Funcion que se encarga de indexar en Solr el contenido del parametro row.
	 * 
	 * @param url
	 * @param row
	 * @return True si la indexación en Solr se ejecuto sin problema. False, en
	 *         caso contrario.
	 * @throws IOException
	 * @throws SolrServerException
	 */
	public static boolean insertarSolr(String url, Row row) throws IOException,
			SolrServerException {
		SolrURL = url;
		obtenerSesionSolr();
		SolrInputDocument SolrDocumento = new SolrInputDocument();
		SolrDocumento.addField("id", row.getString("clave"));
		SolrDocumento.addField("clave", row.getString("clave"));
		SolrDocumento.addField("tipo_registro", row.getString("tipo_registro"));
		SolrDocumento.addField("creado_por", row.getString("creado_por"));
		SolrDocumento.addField("fecha_creacion",
				FormatoSolr.format(row.getDate("fecha_creacion")));
		SolrDocumento.addField("modificado_por",
				row.getString("modificado_por"));
		SolrDocumento.addField("fecha_ultima_modificacion",
				FormatoSolr.format(row.getDate("fecha_ultima_modificacion")));
		SolrDocumento.addField("oficina", row.getString("oficina"));

		claves = row.getMap("valores", String.class, String.class);
		

		for (Map.Entry<String, String> entry : claves.entrySet()) {
			SolrDocumento.addField(entry.getKey(), entry.getValue());
		}
		UpdateResponse response = SolrCliente.add(SolrDocumento);
		claves=null;

		if (response.getStatus() == 0) {
			SolrCliente.commit();
			//SolrCliente.close();
			return true;
		} else {
			//SolrCliente.close();
			return false;
		}
	}

	/**
	 * Recibe un registro, crea un map con todos los valores y convierte el Map
	 * a un string con formato JSON. El orden de insercion en el Map es
	 * importante para el orden de los campos en el JSON final
	 * 
	 * @param row
	 * @return String con registro en formato JSON
	 * 
	 */
	public static String toJson(Row row) {
		Map<String, String> valores = new LinkedHashMap<String, String>();
		valores.put("id", row.getString("clave"));
		valores.put("clave", row.getString("clave"));
		valores.put("tipo_registro", row.getString("tipo_registro"));
		valores.put("creado_por", row.getString("creado_por"));
		valores.put("fecha_creacion",
				formato.format(row.getDate("fecha_creacion")) + "T00:00:00.0Z");
		valores.put("modificado_por", row.getString("modificado_por"));
		valores.put("fecha_ultima_modificacion",
				formato.format(row.getDate("fecha_ultima_modificacion"))
						+ "T00:00:00.0Z");
		valores.put("oficina", row.getString("oficina"));
		claves = row.getMap("valores", String.class, String.class);
		valores.putAll(claves);
		// String json=valores.toString();
		String json = gson.toJson(valores);
		valores=null;
		claves=null;
		return json;

	}

	/**
	 * Recibe el resulset devuelto por el Query de Cassandra para convertirlo en
	 * un arreglo de registros con formato JSON
	 * 
	 * @param rs
	 * @return String en formato JSON en forma de arreglo de registros.
	 */
	public static String toJsonFull(ResultSet rs) {
		String json = "";
		// int i=0;
		json += "[";
		for (Row row : rs) {
			if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()){
				rs.fetchMoreResults();
				//System.out.println("ENTRO");
			}
			json += toJson(row);
			json += ",";
			// i++;
		}
		json = json.substring(0, json.length() - 1);
		json += "]";
		// System.out.println(i);
		return json;

	}

	private static Session obtenerSesion() {

		if (cassandraSession == null) {
			cassandraSession = crearSesion();
		}

		return cassandraSession;

	}

	public static void cerrarSesion() {

		if (cassandraSession != null) {
			cassandraSession.close();
			cassandraSession=null;
		}
	}

	private static Session crearSesion() {
		Cluster cluster = Cluster
				.builder()
				.addContactPoint(hostName)
				.withQueryOptions(
						new QueryOptions().setFetchSize(100)
								.setConsistencyLevel(ConsistencyLevel.ONE))
				/* Solo un nodo */
				.withSocketOptions(
						new SocketOptions().setReadTimeoutMillis(999999999)
								.setConnectTimeoutMillis(999999999))
				.withCredentials("etl", "etl2014#").build();
		return cluster.connect(keyspaceName);
	}

	public static ResultSet Consultar(String consulta, String host,
			String keyspace) {

		hostName = host;
		keyspaceName = keyspace;
		ResultSet results = phdconsultoresCassandra.obtenerSesion().execute(
				consulta);

		return results;
	}

	public static ResultSetFuture ConsultarAsincrono(String consulta,
			String host, String keyspace) {

		hostName = host;
		keyspaceName = keyspace;
		ResultSetFuture results = phdconsultoresCassandra.obtenerSesion()
				.executeAsync(consulta);

		return results;
	}

	public static ResultSet Modificar(String update, String host,
			String keyspace) {

		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phdconsultoresCassandra
				.obtenerSesion().prepare(update);
		BoundStatement boundStatement = preparedStatement.bind();

		return phdconsultoresCassandra.obtenerSesion().execute(boundStatement);
	}

	public static ResultSetFuture ModificarAsincono(String update, String host,
			String keyspace) {

		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phdconsultoresCassandra
				.obtenerSesion().prepare(update);
		BoundStatement boundStatement = preparedStatement.bind();

		return phdconsultoresCassandra.obtenerSesion().executeAsync(
				boundStatement);
	}

	public static ResultSet Insertar(String insert, String host, String keyspace) {

		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phdconsultoresCassandra
				.obtenerSesion().prepare(insert);
		BoundStatement boundStatement = preparedStatement.bind();
		return phdconsultoresCassandra.obtenerSesion().execute(boundStatement);
	}

	public static ResultSetFuture InsertarAsincrono(String insert, String host,
			String keyspace) {

		hostName = host;
		keyspaceName = keyspace;
		PreparedStatement preparedStatement = phdconsultoresCassandra
				.obtenerSesion().prepare(insert);
		BoundStatement boundStatement = preparedStatement.bind();
		return phdconsultoresCassandra.obtenerSesion().executeAsync(
				boundStatement);
	}

}