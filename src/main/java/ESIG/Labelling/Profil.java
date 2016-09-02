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
import com.datastax.spark.connector.cql.CassandraConnector;

import ESIG.DTO.DTOGyroscope;
import ESIG.DTO.DTOHeartrate;
import ESIG.DTO.DTOLocation;

/**
 * Labelling algorithm, working with ThreadData.java not with Thread2Data.java
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.1
 */
public class Profil {
	private String name;
	private int speed;
	private int heartrate;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getSpeed() {
		return speed;
	}

	public void setSpeed(int speed) {
		this.speed = speed;
	}

	public int getHeartrate() {
		return heartrate;
	}

	public void setHeartrate(int heartrate) {
		this.heartrate = heartrate;
	}

	public Profil(String name,int speed,int heartrate){
		this.setName(name);
		this.setSpeed(speed);
		this.setHeartrate(heartrate);
	}
	
	/**
     * main for the labelling algorithm
     * 
     */
	public static void main(String[] args) {
		ResultSet rs=null;
		ResultSet rs2=null;
		ResultSet rs3=null;
		long T = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		Session session = connector.openSession();
		
		Profil sit = new Profil("Sit",0,70);
		Profil stand = new Profil("Stand",0,80);
		Profil walk = new Profil("Walk",1,80);
		Profil run = new Profil("Run",1,120);
		List <Profil> listp = new ArrayList<Profil>();
		listp.add(sit);
		listp.add(stand);
		listp.add(walk);
		listp.add(run);
		
		List <Row> test = null;
		List <Row> test2 = null;
		List <Row> test3 = null;
		
		rs=session.execute("SELECT * FROM wc_2.location"); // WHERE timestamps<1454925599 AND timestamp>1454925559
		test=rs.all();
		
		rs2=session.execute("SELECT * FROM wc_2.heartrate");
		test2=rs2.all();
		
		rs3=session.execute("SELECT * FROM wc_2.gyroscope LIMIT 500000");
		test3=rs3.all();
		
		//ColumnDefinitions collums = rs.getColumnDefinitions();
		//ColumnDefinitions collums2 = rs2.getColumnDefinitions();
		for (int a=0;a<listp.size();a++){
			List<DTOHeartrate> l1 = new ArrayList<DTOHeartrate>();
			List<DTOLocation> l2 = new ArrayList<DTOLocation>();
			List<DTOGyroscope> list = new ArrayList<DTOGyroscope>();
			
			for(int i=0;i<test.size();i++){
				if(Double.parseDouble(test.get(i).getString("speed"))>listp.get(a).getSpeed()){
					/*for(int j=0;j<collums.size();j++){
							System.out.print(" | "+collums.getName(j)+" : "+test.get(i).getString(collums.getName(j)));
					}
					c++;
					System.out.println();*/
					DTOLocation loc = new DTOLocation(Long.parseLong(test.get(i).getString("timestamps")), Double.parseDouble(test.get(i).getString("speed")));
					l2.add(loc);
				}
			}
			
			for(int l=0;l<test2.size();l++){
				if(Double.parseDouble(test2.get(l).getString("rate"))>listp.get(a).getHeartrate()){
					/*for(int k=0;k<collums2.size();k++){
						System.out.print(" | "+collums2.getName(k)+" : "+test2.get(l).getString(collums2.getName(k)));
					}
					c2++;
					System.out.println();*/
					DTOHeartrate hea = new DTOHeartrate(Long.parseLong(test2.get(l).getString("timestamps")), Double.parseDouble(test2.get(l).getString("rate")));
					l1.add(hea);
				}
			}
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
				while (l1.get(j).getTimestamps()<l2.get(i).getTimestamps()-50000){
					j++;
				}
				t1 = l1.get(j).getTimestamps();
				Date d1 = new Date(t1);
				while (l1.get(j).getTimestamps()<=l2.get(i).getTimestamps()+50000){
					j++;
				}
				if (t1!=l1.get(j).getTimestamps()){
					t2 = l1.get(j-1).getTimestamps();
				} else {
					t2 = l1.get(j).getTimestamps();
				}
				Date d2 = new Date(t2);
				for (int b=0;b<test3.size();b++){
					if (Long.parseLong(test3.get(b).getString("timestamps"))>t1 && Long.parseLong(test3.get(b).getString("timestamps"))<t2){
						//System.out.println("x : "+test3.get(b).getString("x") +" | y : "+test3.get(b).getString("y") + " | z : "+test3.get(b).getString("z"));
						DTOGyroscope gyr = new DTOGyroscope(Long.parseLong(test3.get(b).getString("timestamps")), Double.parseDouble(test3.get(b).getString("x")), Double.parseDouble(test3.get(b).getString("y")), Double.parseDouble(test3.get(b).getString("z")));
						list.add(gyr);
					}
				}
				
				double x=0;
				double y=0;
				double z=0;
				for (int c=0;c<list.size();c++){
					x=x+list.get(c).getX();
					y=y+list.get(c).getY();
					z=z+list.get(c).getZ();			
				}
				x=x/list.size();
				y=y/list.size();
				z=z/list.size();
				if(t2-t1!=0){
					System.out.println("Debut : "+d1);
					System.out.println("Fin : "+d2);
					System.out.println("Durée : "+(t2-t1) +"\n----------------------------------------------");
					System.out.println("x : "+x +" | y : " +y +" | z : " +z +"\n----------------------------------------------");
				}
				
				/*for (int j=0;j<l2.size();j++){
					if (l1.get(i).getTimestamps()>l2.get(j).getTimestamps() && l1.get(i).getTimestamps()<l2.get(j+1).getTimestamps()){
						c++;
					}
				}*/
			}
			
			
			System.out.println(l1.size()+" et "+l2.size() + " et "+j);
		}
			
		sc.close();
		
		long Ttot= System.currentTimeMillis();
    	System.out.println("\nTemps total d'execution : " +(Ttot-T)+"ms");
		System.exit(0);
	}

}
