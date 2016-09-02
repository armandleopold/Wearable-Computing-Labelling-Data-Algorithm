package ESIG.Labelling;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

//import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.spark.connector.cql.CassandraConnector;

import ESIG.DTO.DTOHeartrate;
import ESIG.DTO.DTOLocation;

/**
 * Labelling algorithm, working with Thread2Data.java
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */
public class Profil2 {
	
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
     * select CQL query function
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
	public static ResultSet selectQuery(Session session, String query, int i, int limit, JavaSparkContext sc){
		try {
			return session.execute(query);
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
		return null;
	}
	
	/**
     * main
     * 
     */
	public static void main(String[] args) {
		ResultSet rs=null;
		ResultSet rs2=null;
		long T = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		Session session = connector.openSession();
		
		query(session,"CREATE KEYSPACE wc_2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",0,100,sc);
		query(session, "CREATE TABLE wc_2.label(timestamps bigint primary key, profile text, start bigint, end bigint);", 0, 100, sc);
		
		Profil stand = new Profil("Stand or sit",1,80);
		Profil walk = new Profil("Walk",1,80);
		Profil run = new Profil("Run",1,120);
		List <Profil> listp = new ArrayList<Profil>();
		listp.add(stand);
		listp.add(walk);
		listp.add(run);
		
		List <Row> test = null;
		List <Row> test2 = null;
		
		
		
		//ColumnDefinitions collums = rs.getColumnDefinitions();
		//ColumnDefinitions collums2 = rs2.getColumnDefinitions();
		for (int a=0;a<listp.size();a++){
			if(listp.get(a).getName().equals("Stand or sit")){
				rs=selectQuery(session,"SELECT * FROM wc_2.location WHERE speed<"+listp.get(a).getSpeed()+" ALLOW FILTERING",0,100,sc); // WHERE timestamps<1454925599 AND timestamp>1454925559
				test=rs.all();
				
				rs2=selectQuery(session,"SELECT * FROM wc_2.heartrate WHERE rate<"+listp.get(a).getHeartrate()+" ALLOW FILTERING",0,100,sc);
				test2=rs2.all();
			} else {
				rs=selectQuery(session,"SELECT * FROM wc_2.location WHERE speed>"+listp.get(a).getSpeed()+" ALLOW FILTERING",0,100,sc); // WHERE timestamps<1454925599 AND timestamp>1454925559
				test=rs.all();
				
				rs2=selectQuery(session,"SELECT * FROM wc_2.heartrate WHERE rate>"+listp.get(a).getHeartrate()+" ALLOW FILTERING",0,100,sc);
				test2=rs2.all();
			}
			
			List<DTOHeartrate> l1 = new ArrayList<DTOHeartrate>();
			List<DTOLocation> l2 = new ArrayList<DTOLocation>();
			
			for(int i=0;i<test.size();i++){
				DTOLocation loc = new DTOLocation(test.get(i).getLong("timestamps"), test.get(i).getDouble("speed"));
				//System.out.println(loc.toString());
				l2.add(loc);
			}
			
			for(int l=0;l<test2.size();l++){
				DTOHeartrate hea = new DTOHeartrate(test2.get(l).getLong("timestamps"), test2.get(l).getDouble("rate"));
				//System.out.println(hea.toString());
				l1.add(hea);
			}
			
			System.out.println("rate :" +l1.size() +" loc :" +l2.size());
			Collections.sort(l1);
			Collections.sort(l2);
			int j=0;
			System.out.println("**********************************************************");
			System.out.println();
			System.out.println("                         "+listp.get(a).getName()+"                         ");
			System.out.println("          speed : "+listp.get(a).getSpeed() +"              rate : "+listp.get(a).getHeartrate());
			System.out.println();
			System.out.println("**********************************************************");
			for (int i=0;i<l2.size();i++){
				//System.out.println(l2.get(i).getTimestamps());
				long t1=0L;
				long t2=0L;
				while (l1.get(j).getTimestamps()<l2.get(i).getTimestamps()-60000 && j<l1.size()-1){
					j++;
				}
				t1 = l1.get(j).getTimestamps();
				Date d1 = new Date(t1);
				while (l1.get(j).getTimestamps()<=l2.get(i).getTimestamps()+60000 && j<l1.size()-1){
					j++;
				}
				if (t1!=l1.get(j).getTimestamps()){
					t2 = l1.get(j-1).getTimestamps();
				} else {
					t2 = l1.get(j).getTimestamps();
				}
				Date d2 = new Date(t2);
				
				if(t2-t1!=0){
					System.out.println("Start : "+d1);
					System.out.println("End : "+d2);
					System.out.println("Lenght : "+(t2-t1) +"ms\n----------------------------------------------");
					query(session,"INSERT INTO wc_2.label (timestamps,profile,start,end) VALUES ("+System.currentTimeMillis() +",'" +listp.get(a).getName() +"'," +t1 +"," +t2 +")",0,100,sc);
				}
				
				/*for (int j=0;j<l2.size();j++){
					if (l1.get(i).getTimestamps()>l2.get(j).getTimestamps() && l1.get(i).getTimestamps()<l2.get(j+1).getTimestamps()){
						c++;
					}
				}*/
			}
			
			
			//System.out.println(l1.size()+" et "+l2.size() + " et "+j);
		}
			
		sc.close();
		
		long Ttot= System.currentTimeMillis();
    	System.out.println("\nTotal execution time : " +(Ttot-T)+"ms");
		System.exit(0);
	}

}
