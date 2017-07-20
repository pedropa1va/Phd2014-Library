package com.phdconsultores.bd.cassandra.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Stopwatch;
import com.phdconsultores.bd.cassandra.phdconsultoresCassandra;


public class TestInsertarMongodb {

/*	public static void main(String[] args) throws IOException
			 {
		String host = "192.168.0.100";
		String keyspace = "saren";
		String mongoDB = "test";
		String mongoColeccion = "post";
		String mongoIP="127.0.0.1";
		int mongoPuerto=27017;
		int n=100;
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Calendar calIni = Calendar.getInstance();
		String ruta = "/home/phd2014/saren/eclipse/PruebasLog/mongo/mongo_"+dateFormat.format(calIni.getTime())+".txt";
		//Row row;
		ResultSet rs = com.phdconsultores.bd.cassandra.phdconsultoresCassandra
				.Consultar(
						"select * from saren.publico where tipo_registro='PersonaComun' limit "+n+" ALLOW FILTERING",
						host, keyspace);
		Stopwatch timer = Stopwatch.createStarted();
		for (Row row : rs) {
			if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()){
				rs.fetchMoreResults();
				System.out.println("ent");
			}		
					 com.phdconsultores.bd.cassandra.phdconsultoresCassandra.insertarMongodb(mongoIP,mongoPuerto,mongoDB,mongoColeccion,row);
		}
		System.out.println("Tiempo de ejecución del metodo insertarMongoDB para "+n+" de registros: " + timer.stop());	
		com.phdconsultores.bd.cassandra.phdconsultoresCassandra.cerrarSesion();
		
		
        File archivo = new File(ruta);
        BufferedWriter bw = null;	        
            bw = new BufferedWriter(new FileWriter(archivo));
            bw.write("Inicio: "+dateFormat.format(calIni.getTime())+"\n");
            bw.write("Cantidad de registros: "+n+"\n");
            bw.write("Duración: "+timer.toString()+"\n");	            
        bw.close();
        
		System.exit(0);
	}*/
	
	public  void test(String host,String keyspace,String mongoDB,String mongoColeccion,String mongoIP,int mongoPuerto,int n) throws IOException
	 {
/*String host = "192.168.0.100";
String keyspace = "saren";
String mongoDB = "test";
String mongoColeccion = "post";
String mongoIP="127.0.0.1";
int mongoPuerto=27017;
int n=1000;*/

DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
Calendar calIni = Calendar.getInstance();
String inicio="Hora de inicio para prueba de MongoDB: " + dateFormat.format(calIni.getTime()) + "\n";
String ruta = "/home/phd2014/saren/eclipse/PruebasLog/mongo/mongo_"+dateFormat.format(calIni.getTime())+".txt";

//Row row;
ResultSet rs = com.phdconsultores.bd.cassandra.phdconsultoresCassandra
		.Consultar(
				"select * from saren.publico where tipo_registro='PersonaComun' limit "+n+" ALLOW FILTERING",
				host, keyspace);
Stopwatch timer = Stopwatch.createStarted();
for (Row row : rs) {
	if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()){
		rs.fetchMoreResults();
		//System.out.println("ent");
	}		
			 com.phdconsultores.bd.cassandra.phdconsultoresCassandra.insertarMongodb(mongoIP,mongoPuerto,mongoDB,mongoColeccion,row);
}
 timer.stop();	
//System.out.println("Tiempo de ejecución del metodo insertarMongoDB para "+n+" de registros: " + timer.stop());	
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

phdconsultoresCassandra.cerrarSesionMongo();
phdconsultoresCassandra.cerrarSesion();


}
	
	public static void GC(){

	    Runtime garbage = Runtime.getRuntime();
	    garbage.gc();

	}

}