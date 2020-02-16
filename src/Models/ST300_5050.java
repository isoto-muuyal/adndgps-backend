package Models;



import java.net.*;
import java.sql.*;
import java.io.*;
import com.microsoft.sqlserver.jdbc.*;
import java.time.LocalDateTime;;

public class ST300_5050 extends Thread
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
	                  System.out.println ("Waiting for Connection with GPS Models ST300 and ST340");
	                  new ST300_5050 (serverSocket.accept()); 
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

	 private ST300_5050 (Socket clientSoc)
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

		             //AQUI SE BUSCA POR SI HAY COMANDOS PENDIENTES DE ENVIAR
		             String sqlComandos = "Select id,comando,condicion,contador,fecha_peticion,id_servidor,DATEDIFF(minute, fecha_peticion, getdate()) as minutos from j_interacciones_listener where condicion = 'false' and comando like 'ST300CMD%'";
		             ResultSet commResultSet = myStatement.executeQuery(sqlComandos);
		             while (commResultSet.next()){
		            	 dOut.writeUTF(commResultSet.getString("comando"));
		                 dOut.flush();
			             System.out.println ("Command Sent: " + commResultSet.getString("comando")); 
		            	 if(commResultSet.getString("comando").contains("StatusReq")){
		            		 //aqui borra el registro de la tabla de comandos
		            		 sqlComandos = "Delete from j_interacciones_listener where id = " + commResultSet.getString("id");
		   	              	 deleteStatement.execute(sqlComandos);
		            	 }
	   	              	 else{
			            	 if(Integer.parseInt(commResultSet.getString("minutos")) > 2){
			            		 //aqui borra el registro de la tabla de comandos
			            		 sqlComandos = "Delete from j_interacciones_listener where id = " + commResultSet.getString("id");
			   	              	 deleteStatement.execute(sqlComandos);
			            		 //aqui inserta el registro en la tabla de interacciones estatus
				            	 sqlComandos = "Insert into k_interacciones_estatus values('" + commResultSet.getString("comando") + "','" + LocalDateTime.now() + "','" + commResultSet.getString("id_servidor") + "','Fallido')";
				            	 myStatement.execute(sqlComandos);
			            	 }
		            	 }
		             }

		             //Aqui comienza a leer los registros del GPS
					  String[] arrayDatos = new String[27]; //Aqui se define un arreglo fijo de 27 elementos para estandarizar el Insert a la BD
		              System.out.println ("Server: " + inputLine); 
		              out.println(inputLine); 
		              String cadena=inputLine;
		              String[] arrayCadena = cadena.split(";");
					  for (int i=0; i<27; i++){
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
		              if (tipo_reporte.contains("CMD")){
		            	  dato1=arrayDatos[2];
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
		                  
	                  while (myResultSet.next()){
	                	  System.out.println(myResultSet.getString("nombre_usuario"));
	                	  System.out.println(myResultSet.getString("id"));
	                	  System.out.println(myResultSet.getString("nombre_vehiculo"));
	                	  usuario = myResultSet.getString("nombre_usuario");
	                	  id_servidor = myResultSet.getString("id");
	                	  nombre_vehiculo= myResultSet.getString("nombre_vehiculo");
	                  }
					  if (tipo_reporte.contains("CMD")){
						  System.out.println("Respuesta de COMANDO Recibida");
						  sqlComandos = "Delete from j_interacciones_listener where comando = 'ST300CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "'";
						  deleteStatement.execute(sqlComandos);
						  //aqui inserta el registro en la tabla de interacciones estatus
						  sqlComandos = "Insert into k_interacciones_estatus values('ST300CMD;" + arrayDatos[2] + ";02;" + arrayDatos[4] + "','" + LocalDateTime.now() + "','" + id_servidor + "','OK')";
						  myStatement.execute(sqlComandos);
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
							  instruccionSQL = "Insert into " + tabla + " "  + tblCampos + " values('" + arrayDatos[0] + "','" + arrayDatos[1] + "','" + arrayDatos[2] + "','" + arrayDatos[3] + "','" + arrayDatos[4] + "','" + arrayDatos[5] + "','" + arrayDatos[6] + "','" + arrayDatos[7] + "','" + arrayDatos[8] + "','" + arrayDatos[9] + "','" + arrayDatos[10] + "','" + arrayDatos[11] + "','" + arrayDatos[12] + "','" + arrayDatos[13] + "','" + arrayDatos[14] + "','" + arrayDatos[15] + "','" + arrayDatos[16] + "','" + arrayDatos[17] + "','" + arrayDatos[18] + "','" + arrayDatos[19] + "','" + arrayDatos[20] + "','" + arrayDatos[21] + "','" + arrayDatos[22] + "','" + arrayDatos[23] + "','" + arrayDatos[24] + "','" + arrayDatos[25] + "','" + arrayDatos[26] + "','" + usuario + "','" + id_servidor + "','" + nombre_vehiculo + "')";
							  myStatement.execute(instruccionSQL);
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