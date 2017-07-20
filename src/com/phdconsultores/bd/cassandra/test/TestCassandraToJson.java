package com.phdconsultores.bd.cassandra.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.solr.client.solrj.SolrServerException;

import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;
import com.phdconsultores.bd.cassandra.phdconsultoresCassandra;


public class TestCassandraToJson {

/*	public static void main(String[] args) throws IOException,
		SolrServerException {
		String host = "192.168.0.100";
		String keyspace = "saren";
		int n=10;
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Calendar calIni = Calendar.getInstance();
		String ruta = "/home/phd2014/saren/eclipse/PruebasLog/json/json_"+dateFormat.format(calIni.getTime())+".txt";
		ResultSet rs = com.phdconsultores.bd.cassandra.phdconsultoresCassandra
				.Consultar(
						"select * from saren.publico where tipo_registro='PersonaComun' limit "+n+" ALLOW FILTERING",
						host, keyspace);
		Stopwatch timer = Stopwatch.createStarted();
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.toJsonFull(rs);
		System.out.println("Tiempo de ejecución del metodo toJsonFull para "+n+" de registros: " + timer.stop());
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesion();
				    
	        File archivo = new File(ruta);
	        BufferedWriter bw = null;	        
	            bw = new BufferedWriter(new FileWriter(archivo));
	            bw.write("Inicio: "+dateFormat.format(calIni.getTime())+"\n");
	            bw.write("Cantidad de registros: "+n+"\n");
	            bw.write("Duración: "+timer.toString()+"\n");	            
	        bw.close();
	        
		System.exit(0);
	}
	
	*/
	public void test(String host,String keyspace,int n) throws IOException,
	SolrServerException {

	/*String host = "192.168.0.100";
	String keyspace = "saren";
	int n=10;*/
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	Calendar calIni = Calendar.getInstance();
	String inicio="Hora de inicio para prueba de ToJson: " + dateFormat.format(calIni.getTime()) + "\n";
	String ruta = "/home/phd2014/saren/eclipse/PruebasLog/json/json_"+dateFormat.format(calIni.getTime())+".txt";
	ResultSet rs = com.phdconsultores.bd.cassandra.phdconsultoresCassandra
			.Consultar(
					"select * from saren.publico where tipo_registro='PersonaComun' limit "+n+" ALLOW FILTERING",
					host, keyspace);
	Stopwatch timer = Stopwatch.createStarted();
	com.phdconsultores.bd.cassandra.phdconsultoresCassandra.toJsonFull(rs);
	timer.stop();
	//System.out.println("Tiempo de ejecución del metodo toJsonFull para "+n+" de registros: " + timer.stop());
	//com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesion();
			    
	File archivo = new File(ruta);
	BufferedWriter bw = null;
	bw = new BufferedWriter(new FileWriter(archivo));
	System.out.print(inicio);
	bw.write(inicio);
	bw.write("Cantidad de registros: " + n + "\n");
	System.out.print("Cantidad de registros: " + n + "\n");
	bw.write("Duración: " + timer.toString() + "\n");
	System.out.print("Duración: " + timer.toString() + "\n");
	bw.close();
        
        /**/
        bw=null;
        rs=null;
        timer=null;
        archivo=null;
        /**/
        
		phdconsultoresCassandra.cerrarSesion();
		GC();

}
	
	public static void GC(){

	    Runtime garbage = Runtime.getRuntime();
	    garbage.gc();

	}

}