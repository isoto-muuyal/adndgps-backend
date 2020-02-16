package Models;

import java.net.*;
import java.sql.*;
import java.io.*;
import java.time.LocalDateTime;;

public class ListenerALL extends Thread
{ 
	 protected static boolean serverContinue = true;
	 protected Socket clientSocket;

	 public static void main(String[] args) throws IOException 
	   { 
	    ServerSocket serverSocket = null; 

	    try { 
	         serverSocket = new ServerSocket(5050); 
	         System.out.println ("Connection Socket Created");
	         try { 
	              while (serverContinue)
	                 {
	                  System.out.println ("Waiting for Connection");
	                  new ListenerALL (serverSocket.accept()); 
	                 }
	             } 
	         catch (IOException e) 
	             { 
	              System.err.println("Accept failed."); 
	              System.exit(1); 
	             } 
	        } 
	    catch (IOException e) 
	        { 
	         System.err.println("Could not listen on port: 5050."); 
	         System.exit(1); 
	        } 
	    finally
	        {
	         try {
	              serverSocket.close(); 
	             }
	         catch (IOException e)
	             { 
	              System.err.println("Could not close port: 5050."); 
	              System.exit(1); 
	             } 
	        }
	   }

	 private ListenerALL (Socket clientSoc)
	   {
	    clientSocket = clientSoc;
	    start();
	   }

	 public void run()
	   {
	    System.out.println ("New Communication Thread Started");
	    
	    try { 
	    	 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), 
	                                      true); 
	         DataOutputStream dOut = new DataOutputStream(clientSocket.getOutputStream());
	    	 BufferedReader in = new BufferedReader( 
	                 new InputStreamReader( clientSocket.getInputStream())); 

	         String inputLine; 
	         try {
		         while ((inputLine = in.readLine()) != null) 
		         { 
		             //AQUI SE HACE LA CONEXION	A LA BASE DE DATOS
		             Connection cn = null;
		             Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		             cn=DriverManager.getConnection("jdbc:sqlserver://WIN-L5D6H0K72NT:1433;databaseName=PAvl2002","sa","Avlmexico$123");
		             Statement myStatement = cn.createStatement();
		             Statement deleteStatement = cn.createStatement();
		             Statement updateStatement = cn.createStatement();

		             //Aqui comienza a leer los registros del GPS
					  String[] arrayDatos = new String[35]; //Aqui se define un arreglo fijo de 27 elementos para estandarizar el Insert a la BD
		              System.out.println ("Server: " + inputLine); 
		              out.println(inputLine); 
		              String cadena=inputLine;
		              String[] arrayCadena = cadena.split(";");
					  for (int i=0; i<35; i++){
						  if (i < arrayCadena.length){
							arrayDatos[i] = arrayCadena[i];
					      }
						  else{
							arrayDatos[i] = "";
						  }
					  }
		              String dato0=arrayDatos[0]; //HDR
		              String dato1=arrayDatos[1]; //IMIE
		              String tipo_reporte=dato0;
					  String tipo_gps = dato0.substring(0,5);
	                  if (tipo_reporte.contains("CMD")){
			              dato1=arrayDatos[2]; //IMIE
	                  }
		              
		              //BUSCAMOS LOS DATOS DEL VEHICULO EN BASE AL IMEI
		              String instruccionSQL = "Select * from e_inventario_general where imei = '" + dato1 + "'" ;
	        		  ResultSet myResultSet = myStatement.executeQuery(instruccionSQL);
	        		  String usuario = "";
	                  String id_servidor = "";
	                  String nombre_vehiculo = "";
					  String tabla ="";
	                  String tblCampos = "(HDR,DEV_ID,MODEL,SW_VER,DATE,TIME,CELL,LAT,LON,SPD,CRS,SATT,FIX,DIST,PWR_VOLT,IO,EVT_ID,H_METER,BCK_VOLT,MSG_TYPE,ADC,ODOMETRO,TF,VS,DID,DID_REG,TEMPERATURE1,usuario,id_servidor,nombre_vehiculo)";
	                  boolean flag = false;
					  boolean envia_comando = false;
		                  
	                  while (myResultSet.next()){
	                	  System.out.println(myResultSet.getString("nombre_usuario"));
	                	  System.out.println(myResultSet.getString("id"));
	                	  System.out.println(myResultSet.getString("nombre_vehiculo"));
	                	  usuario = myResultSet.getString("nombre_usuario");
	                	  id_servidor = myResultSet.getString("id");
	                	  nombre_vehiculo= myResultSet.getString("nombre_vehiculo");
	                  }

			          //AQUI SE BUSCA POR SI HAY COMANDOS PENDIENTES DE ENVIAR
			          String comando ="";
			          String vacio =" ";
			          int contador = 0;
	                  String sqlComandos = "Select * from j_interacciones_listener where condicion = 'false' and contador > 0 and id_servidor ='" + id_servidor + "'" ;
			          ResultSet commResultSet = myStatement.executeQuery(sqlComandos);
			          while (commResultSet.next()){
			          	 contador = Integer.parseInt(commResultSet.getString("contador"));
			          	 comando = commResultSet.getString("comando");
			          	 if (contador >0){
				        	dOut.writeUTF(commResultSet.getString("comando"));
				            dOut.flush();
					        System.out.println ("Command Sent: " + commResultSet.getString("comando")); 
					        if(commResultSet.getString("comando").contains("StatusReq")){
			           		  //aqui borra el registro de la tabla de comandos
			           		  sqlComandos = "Delete from j_interacciones_listener where id = " + commResultSet.getString("id");
			           		  deleteStatement.execute(sqlComandos);
				           	}
				           	int cont = Integer.parseInt(commResultSet.getString("contador")) - 1;
				           	sqlComandos = "Update j_interacciones_listener set contador =" + Integer.toString(cont) + " where id = " + commResultSet.getString("id");
				           	updateStatement.execute(sqlComandos);
			          	 }
			          	 else{
		        			instruccionSQL = "Insert into k_interacciones_estatus values('" + comando + "','" + LocalDateTime.now() + "','" + id_servidor + "','FALLIDO')";
		        			myStatement.execute(instruccionSQL);
							sqlComandos = "Delete from j_interacciones_listener where  where id = " + commResultSet.getString("id");
							deleteStatement.execute(sqlComandos);
			          	 }
			          }

	                  if (tipo_reporte.contains("CMD")){
						  System.out.println("Respuesta de COMANDO Recibida");
	                      switch (tipo_gps){
							case "ST300":
								sqlComandos = "Delete from j_interacciones_listener where comando = 'ST300CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "'";
			        			instruccionSQL = "Insert into k_interacciones_estatus values('ST300CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "','" + LocalDateTime.now() + "','" + id_servidor + "','ENVIADO')";
								envia_comando = true;
								break;
							case "SA200":
								sqlComandos = "Delete from j_interacciones_listener where comando = 'SA200CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "'";
			        			instruccionSQL = "Insert into k_interacciones_estatus values('SA200CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "','" + LocalDateTime.now() + "','" + id_servidor + "','ENVIADO')";
								envia_comando = true;
								break;
							case "ST600":
								sqlComandos = "Delete from j_interacciones_listener where comando = 'ST600CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "'";
			        			instruccionSQL = "Insert into k_interacciones_estatus values('ST600CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "','" + LocalDateTime.now() + "','" + id_servidor + "','ENVIADO')";
								envia_comando = true;
								break;
							default:
								System.out.println("MODELO DE GPS NO CONFIGURADO PARA EL LISTENER......");
								envia_comando = false;
								break;
						  }
	   	              	  if (envia_comando){
							deleteStatement.execute(sqlComandos);
		        			myStatement.execute(instruccionSQL);
						  }
	        		  }
	        		  if (tipo_reporte.contains("ALV")){
	                      System.out.println("Insertando KEEPALIVE");
	        			  instruccionSQL = "Insert into zkeepalive values('" + dato0 + "','" + arrayDatos[1] + "','" + id_servidor + "','" + nombre_vehiculo + "','" + usuario + "','" + LocalDateTime.now() + "')";
	        			  myStatement.execute(instruccionSQL);
	        		  }
	        		  else {
		        		  if (tipo_reporte.contains("STT")){
		                      System.out.println("Insertando LECTURAS");
							  tabla = "zlecturas";
							  flag = true;
		        		  }
		        		  if (tipo_reporte.contains("EVT")){
		                      System.out.println("Insertando EVENTOS");
							  tabla = "zeventos";
							  flag = true;
		        		  }
		        		  if (tipo_reporte.contains("EMG")){
		                      System.out.println("Insertando EMERGENCIAS");
							  tabla = "zemergencias";
							  flag = true;
		        		  }
		        		  if (tipo_reporte.contains("ALT")){
		                      System.out.println("Insertando ALERTAS");
							  tabla = "zalertas";
							  flag = true;
		        		  }
						  if (flag){
							  switch (tipo_gps){
								case "ST300":  //GPS Modelos ST300 y ST340
								  instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[7] + "','" + arrayDatos[8] + "','" + arrayDatos[9] + "','" + arrayDatos[10] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[14] + "','" + arrayDatos[15] + "','" + arrayDatos[16] + "','" + arrayDatos[17] + "','" + arrayDatos[18] + "','" + arrayDatos[19] + "','" + arrayDatos[20] + "','" + arrayDatos[21] + "','" + arrayDatos[22] + "','" + arrayDatos[23] + "','" + arrayDatos[24] + "','" + arrayDatos[25] + "','" + arrayDatos[26] + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
								  myStatement.execute(instruccionSQL);
								  break;
								case "SA200":  //GPS Modelos ST215 y ST330
								  int test = arrayCadena.length - 2;
								  int test2 = test - 1;
								  //System.out.println("Test :" + test + " - " + arrayDatos[test]);
								  if (arrayCadena.length > 18) {
									instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + vacio + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[7] + "','" + arrayDatos[8] + "','" + arrayDatos[9] + "','" + arrayDatos[10] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[14] + "','" + arrayDatos[test] + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + vacio + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
								  }
								  else {
									instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + arrayDatos[26] + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[7] + "','" + arrayDatos[8] + "','" + arrayDatos[9] + "','" + arrayDatos[10] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[test2] + "','" + arrayDatos[test] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[14] + "','" + arrayDatos[21] + "','" + arrayDatos[22] + "','" + arrayDatos[23] + "','" + arrayDatos[24] + "','" + arrayDatos[25] + "','" + arrayDatos[26] + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
									//instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + arrayDatos[26] + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[7] + "','" + arrayDatos[8] + "','" + arrayDatos[9] + "','" + arrayDatos[10] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[15] + "','" + arrayDatos[16] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[14] + "','" + arrayDatos[21] + "','" + arrayDatos[22] + "','" + arrayDatos[23] + "','" + arrayDatos[24] + "','" + arrayDatos[25] + "','" + arrayDatos[26] + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
								  }
								  myStatement.execute(instruccionSQL);
								  break;
								case "ST600":  //GPS Modelos ST600 y STU600
								  if (tipo_reporte.contains("STT")){
									instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[14] + "','" + arrayDatos[15] + "','" + arrayDatos[16] + "','" + arrayDatos[17] + "','" + arrayDatos[18] + "','" + arrayDatos[19] + "','" + arrayDatos[20] + "','" + arrayDatos[21] + "','" + arrayDatos[22] + "','" + arrayDatos[24] + "','" + arrayDatos[25] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
								    myStatement.execute(instruccionSQL);
								  }
								  else{
									instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[14] + "','" + arrayDatos[15] + "','" + arrayDatos[16] + "','" + arrayDatos[17] + "','" + arrayDatos[18] + "','" + arrayDatos[19] + "','" + arrayDatos[20] + "','" + arrayDatos[21] + "','" + arrayDatos[21] + "','" + arrayDatos[23] + "','" + arrayDatos[24] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + arrayDatos[26] + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
								    myStatement.execute(instruccionSQL);
								  }
								  break;
								default:
								  System.out.println("MODELO DE GPS NO CONFIGURADO PARA EL LISTENER......");
								  break;
							  }
						  }
	        		  }
						  
		              if (inputLine.equals("Bye.")) 
		                  break; 
		
		              if (inputLine.equals("End Server.")) 
		                  serverContinue = false; 
		         } 
		   	 }catch (Exception e){
		   		 System.out.println("no conecta con la base de datos");
				 e.printStackTrace();
			 }
	         out.close(); 
	         in.close(); 
	         dOut.close();
	         clientSocket.close(); 
	        } 
	    catch (IOException e) 
	        { 
	         System.err.println("Problem with Communication Server");
	         //System.exit(1); 
	         return;
	        } 
	    }}