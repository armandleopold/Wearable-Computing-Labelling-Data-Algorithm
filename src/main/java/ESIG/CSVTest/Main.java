/*
 * lancer Cassandra : net start DataStax_DDC_Server
 */

package ESIG.CSVTest;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.spark.connector.cql.CassandraConnector;

/**
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */


public class Main {
	
	/**
     * Find files in a directory
     * 
     * @param file 
     * the file directory
     * @param all
     * collection of file
     * @param extension
     * extension of file
     */
	private static void findFilesRecursively(File file, Collection<File> all, final String extension) {
	    //Liste des fichiers correspondant a l'extension souhaitee
	    final File[] children = file.listFiles(new FileFilter() {
	    	public boolean accept(File f) {
	                return f.getName().endsWith(extension) ;
	            }}
	    );
	    if (children != null) {
	        //Pour chaque fichier recupere, on appelle a nouveau la methode
	        for (File child : children) {
	            all.add(child);
	            findFilesRecursively(child, all, extension);
	        }
	    }
	}
	
	/**
     * CQL query function
     * 
     * @param session 
     * Spark session
     * @param query
     * query string in CQL
     * @param i
     * number of iteration for the query
     * @param limit
     * maximum of iterations
     * @param sc
     * SparkContext, if the query failed, the sc context is closed
     */
	public static void query(Session session, String query, int i, int limit, JavaSparkContext sc){
		try {
			session.execute(query);
		} catch (NoHostAvailableException e) {
            if (i==limit-1){
            	int j=0;
            	while(j<1000){
            		j++;
            	}
            }
			if (i<limit){
            	i++;
            	query(session, query, i,limit, sc);
            } else {
                e.printStackTrace();
                sc.close();
                System.exit(0);
            }
        } catch (AlreadyExistsException e) {
        }
	}
	
	/**
     * main
     * 
     */
    public static void main( String[] args )
    {
    	long T = System.currentTimeMillis();
    	//local spark cluster connection
    	SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		Session session = connector.openSession();
		
		query(session,"CREATE KEYSPACE wc_2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",0,100,sc);

    	/*Cluster cluster;
    	Session session;
    	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    	cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(2000);
    	session = cluster.connect("demo");*/

    	final List<File> all = new ArrayList<File>();
        findFilesRecursively(new File("src/main/resources/"), all, ".csv");
        //limit the number of thread (here 32)
        ExecutorService threadExecutor = Executors.newFixedThreadPool(32);
        
        for (int i=0;i<all.size();i++) {
        	try {
        	String[] id0 = all.get(i).getName().split("_");
	        String[] id = id0[1].split("-");
	    
	        Thread t = new Thread2Data(all.get(i),id[0],session,sc,i);
	        System.out.println(t.getName());
	        threadExecutor.execute(t);
        	} catch (IndexOutOfBoundsException e){
        		e.printStackTrace();
        	}	
	      }
        threadExecutor.shutdown();
        int min=0;
        int compteur=0;
    	while(Thread2Data.nbthread!=0){
    		try {
				Thread.sleep(10);
				compteur++;
				if (compteur == 6000){
					compteur=0;
					min++;
					System.out.println(min +" minute(s)");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	sc.close();
    	//cluster.close();
    	long Ttot= System.currentTimeMillis();
    	System.out.println("\nTemps total d'execution : " +(Ttot-T)+"ms\nPour "+Thread2Data.totline+" lignes");
    	System.exit(0);
    }
}
