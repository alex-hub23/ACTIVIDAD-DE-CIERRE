package com.AV;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class AV {
	
	private static String nuevaCarpeta = "";
	private static String nombreArchivo = "";
	private static String contenido = "";
	private static int opcion = 0;
	private static int opcion1 = 0;
    private static int opcion2 = 0;
	private static String destino = "";
	private static String borrar = "";
	private static String user = "";
	

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		
		Configuration conf= new Configuration(true);
		conf.set("fs.defaultFS", "hdfs://192.168.100.144:8020/");
		
		System.setProperty("HADOOP_USER_NAME", "hdfs");
	    Scanner sc = new Scanner(System.in);
		
		do {
			
		System.out.println("__________________________________________________________________ ");	
		System.out.println("|     Ingrese el numero correspondiente a la accion a realizar    |");
		System.out.println("|                 Crear  un Archivo = 1                           |");
		System.out.println("|                 Borrar un Archivo = 2                           |");
		System.out.println("|_________________________________________________________________|");	
		opcion = sc.nextInt();
		
			
			switch(opcion) {
			case 1: 
				Scanner datos = new Scanner (System.in);
				try {
					
					
					FileSystem fs= FileSystem.get(conf);
					String home = fs.getHomeDirectory().toString();
					
					System.out.println(" _________________________________________________________________");	
					System.out.println("|                 Ingresa nombre de la carpeta                    |");
					System.out.println("|_________________________________________________________________|");	
					nuevaCarpeta = datos.nextLine();
					
					if (!fs.exists(new Path(home + "/" + nuevaCarpeta))) {
						fs.mkdirs(new Path(home + "/" + nuevaCarpeta));
					}
					
					System.out.println(" _________________________________________________________________");	
					System.out.println("|                 Ingresa nombre del archivo                      |");
					System.out.println("|_________________________________________________________________|");
					nombreArchivo = datos.nextLine();
					
					Path rutaArchivo = new Path(home + "/" + nuevaCarpeta + "/" + nombreArchivo);
					FSDataOutputStream outputStream = null;
					
					System.out.println(" _________________________________________________________________");	
					System.out.println("|                 Ingresa contenido del archivo                   |");
					System.out.println("|_________________________________________________________________|");
					contenido = datos.nextLine();
					
					if (!fs.exists(rutaArchivo)) {
						outputStream = fs.create(rutaArchivo);
						outputStream.writeBytes(contenido);
						outputStream.close();
						
						System.out.println(" Archivo creado en la ruta: "+ rutaArchivo);
						System.out.println(" _________________________________________________________________");	
						System.out.println("|                 ¿Deseas cambiar el dueño del archivo?           |");
						System.out.println("|                    Si = 1       NO= cualquier numero            |");
						System.out.println("|_________________________________________________________________|");
						opcion1 = datos.nextInt();
						if(opcion1 == 1) {
						Scanner sc1 = new Scanner (System.in);
					
						System.out.println("Ingresa el usuario dueño");
						user = sc1.nextLine();
						
						fs.setOwner(rutaArchivo, user,null);
					}
						System.out.println(" _________________________________________________________________");	
						System.out.println("|                 ¿Deseas cambiar permisos?                       |");
						System.out.println("|                    Si = 1       NO= cualquier numero            |");
						System.out.println("|_________________________________________________________________|");
						opcion1 = datos.nextInt();
						if(opcion1 == 1) {
							Scanner sc2 = new Scanner (System.in);
							System.out.println(" _________________________________________________________________");	
							System.out.println("|                 ¿Que permisos desea?                            |");
							System.out.println("|                Todos     = 1               Ninguno   = 2        |");
							System.out.println("|                Ejecutar  = 3               Lectura   = 4        |");
							System.out.println("|      Lectura y Ejecutar  = 5               Escritura = 6        |");
							System.out.println("|      Lectura y Escritura = 7   Escritura y Ejecutar  = 8        |");
							System.out.println("|_________________________________________________________________|");
							opcion2 = sc2.nextInt();
							switch(opcion2) {
							case 1:fs.setPermission(rutaArchivo, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL) );
								break;
							case 2:fs.setPermission(rutaArchivo, new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE) );
								break;
							case 3:fs.setPermission(rutaArchivo, new FsPermission(FsAction.EXECUTE, FsAction.EXECUTE, FsAction.EXECUTE) );
								break;
							case 4:fs.setPermission(rutaArchivo, new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ) );
								break;
							case 5:fs.setPermission(rutaArchivo, new FsPermission(FsAction.READ_EXECUTE, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE) );
								break;
							case 6:fs.setPermission(rutaArchivo, new FsPermission(FsAction.WRITE, FsAction.WRITE, FsAction.WRITE) );
								break;
							case 7:fs.setPermission(rutaArchivo, new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE) );
								break;
							case 8:fs.setPermission(rutaArchivo, new FsPermission(FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE) );
								break;
							default :System.out.println("Numero no valido");
							}
						}
						}

					fs.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
				
			case 2: 
				Scanner datos1 = new Scanner (System.in);
				
				
				try {
					System.out.println("Listado de directorio: /user/hdfs/");
					FileSystem fs= FileSystem.get(conf);
					String home = fs.getHomeDirectory().toString();

					FileStatus[] files = fs.listStatus(new Path(home + "/"));
					for (FileStatus file:files) {
						System.out.println(file.getPath().getName());
					}
					
					System.out.println("");
					System.out.println("Ingresa la carpeta que desea ingresar");
					destino = datos1.nextLine();
					
					
					if(fs.exists(new Path(home + "/" + destino))) {
						FileStatus[] files1 = fs.listStatus(new Path(home + "/" + destino));
						System.out.println("Archivos disponibles de ruta: "+ new Path(home + "/" + destino));
						for (FileStatus file:files1) {
							System.out.println(file.getPath().getName());
						}
						
					}
					System.out.println("");
					System.out.println("Ingresa el archivo que desea borrar");
					borrar = datos1.nextLine();
					if(fs.exists(new Path(home + "/" + destino + "/" + borrar ))) {
						fs.delete(new Path(home + "/" + destino + "/" + borrar ));
						System.out.println("Ha eliminado el archivo: " + new Path(home + "/" + destino + "/" + borrar ));
					}
					
					fs.close();
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					
				break;
				
			default: System.out.println("Numero invalido");
			}	
		}while(opcion != 1 & opcion != 2 );
	}

	
}
