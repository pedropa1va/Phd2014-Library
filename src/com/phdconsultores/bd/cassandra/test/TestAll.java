package com.phdconsultores.bd.cassandra.test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.solr.client.solrj.SolrServerException;

import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;

public class TestAll {
	
	public static void main(String[] args) throws IOException,
	SolrServerException {
		String host = "192.168.0.100";
		String keyspace = "saren";
		/*Solr*/
		String urlString = "http://192.168.0.100:8983/solr/test";
		/**/
		/*Mongo*/
		String mongoDB = "test";
		String mongoColeccion = "post";
		String mongoIP="127.0.0.1";
		int mongoPuerto=27017;
		/**/
		//TestInsertarSolrFull SolrFull= new TestInsertarSolrFull();
		//TestInsertarMongodb Mongo = new TestInsertarMongodb();
		//TestCassandraToJson CassandraToJson = new TestCassandraToJson();
		int n = Integer.parseInt(args[0]);
		int prueba = Integer.parseInt(args[1]);
		
	/*	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Calendar calIni = Calendar.getInstance();	
		String CadenaPrueba = "";
		/*switch(prueba){
		case 1:
			CadenaPrueba="Solr";
			break;
		case 2:
			CadenaPrueba="MongoDB";
			break;
		case 3:
			CadenaPrueba="ToJson";
			break;
		}*/

		//System.out.println("Hora de inicio de prueba GENERAL: "+dateFormat.format(calIni.getTime()));
		//Stopwatch timer = Stopwatch.createStarted();
		//while(n<=1000){	
			//for(int i=0;i<5;i++){*/
				
				if(prueba==1){
				//System.out.println("Entrando a TestInsertarSolrFull");
				TestInsertarSolrFull SolrFull= new TestInsertarSolrFull();
				SolrFull.test(host,keyspace,urlString,n);
				SolrFull=null;
				GC();
				}
				if(prueba==2){
				//System.out.println("Entrando a TestInsertarMongodb");
				TestInsertarMongodb Mongo = new TestInsertarMongodb();
				Mongo.test(host,keyspace,mongoDB,mongoColeccion,mongoIP,mongoPuerto,n);
				Mongo=null;
				GC();
				}
				if(prueba==3){
				//System.out.println("Entrando a TestCassandraToJson");
				TestCassandraToJson CassandraToJson = new TestCassandraToJson();
				CassandraToJson.test(host,keyspace,n);
				CassandraToJson=null;
				GC();
				}
				//System.out.println("Salida");
				//System.out.println("");
		//	}
			//n*=10;
			//System.out.println("N = "+n);
	//	}
		
		//System.out.println("Duración: " + timer.stop());	
		//Calendar calFin = Calendar.getInstance();
		//System.out.println("Hora de fin de prueba para "+CadenaPrueba+": "+dateFormat.format(calFin.getTime()));
		/*com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesionSolr();
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesionMongo();
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesion();*/
		System.exit(0);
}

	public static void GC(){

	    Runtime garbage = Runtime.getRuntime();
	    garbage.gc();

	}
}
