/*
 * lancer Cassandra : net start DataStax_DDC_Server
 */

package ESIG.CSVTest;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

import au.com.bytecode.opencsv.CSVReader;

public class App 
{
	
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
	
	private static void createClass(String id, String val){
		try{
		Cluster cluster;
    	Session session;
    	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    	session = cluster.connect("demo");
    	session.execute("CREATE TABLE demo." +id +" (" +val +");");
    	cluster.close();
		} catch (AlreadyExistsException e){
			 e.printStackTrace();
		}
	}
	
    public static void main( String[] args )
    {
    	long T = System.currentTimeMillis();
    	String tab[] = {"accelerometer","battery","calories","eventlabel","GSR","gyroscope","light"};
    	Cluster cluster;
    	Session session;
    	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    	session = cluster.connect("demo");
    	
    	for(String id : tab){
	    	final Collection<File> all = new ArrayList<File>();
	        findFilesRecursively(new File("src/main/resources/"+id), all, ".csv");

	        for (File file : all) {
	        	System.out.println(file.getName());
	        	try{
	            	long T1 = System.currentTimeMillis();
	            	System.out.println(file.getAbsolutePath());
	            	File file2 = new File(file.getAbsolutePath());
	                FileReader fr = new FileReader(file2);
	
	                CSVReader csvReader = new CSVReader(fr, ',');
	                
	                List<String[] > data = new ArrayList<String[] >();
	
	                String[] nextLine = null;
	                while ((nextLine = csvReader.readNext()) != null) {
	                    int size = nextLine.length;
	
	                    // ligne vide
	                    if (size == 0) {
	                        continue;
	                    }
	                    String debut = nextLine[0].trim();
	                    if (debut.length() == 0 && size == 1) {
	                        continue;
	                    }
	
	                    // ligne de commentaire
	                    if (debut.startsWith("#")) {
	                        continue;
	                    }
	                    data.add(nextLine);
	                }
	                
	                csvReader.close();
	                // compteur
	                //int i=0;
	                String req ="";
	                String[] ligne = data.get(0);
	                String val="";												//A RECUPERER DANS UNE NEW CLASS
	                for (int k=0;k<ligne.length;k++){								//
	                	if(ligne[k].equals("timestamps")){							//
	                		val=val+ligne[k]+" text primary key";					//
	                	} else {													//
	                		val=val+ligne[k]+" text";								//
	                	}															//
	                	if(k!=ligne.length-1){										//
                			val=val+",";											//
                		}															//
	                }																//
	                System.out.println(val);										//
                	
	                //session.execute("CREATE TABLE demo." +id +" (" +val +");");	//
	                for (String[] oneData : data) {
	                	//i++;
	                	
	                	String table ="";
	                	req ="";
	                	for(int j=0;j<oneData.length;j++){
	                		if(j==oneData.length-1){
	                			req = req+"'"+oneData[j]+"'";
	                			table = table+ligne[j];
	                		}else{
		                		req = req+"'"+oneData[j]+"',";
		                		table = table+ligne[j]+",";
	                		}
	                		//System.out.print(oneData[j]+" ");
	                	}
	                	//System.out.println(req);
	                	if(!oneData[0].equals(ligne[0])){
	                		session.execute("INSERT INTO demo."+id +"("+table+")" +" VALUES ("+req+")");
	                	} else {
	                		createClass(id, val);
	                	}
	                	//System.out.println("");
	                	//System.out.println(i+" " +oneData[0]+" " +oneData[1]+" "+oneData[2]+" "+oneData[3]+" "+oneData[4]+" "+oneData[5]+" "+oneData[6]+" "+oneData[7]+" "+oneData[8]+" "+oneData[9]+" "+oneData[10]+" "+oneData[11]);
	                }
	                long T2 = System.currentTimeMillis();
	                
	                System.out.println("\nTemps d'execution :" +(T2-T1)+"ms");
	            	
	            	/*} catch (FileNotFoundException e) {
	                    e.printStackTrace();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }*/
	        } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
	      }
    	}
    	cluster.close();
    	long Ttot= System.currentTimeMillis();
    	System.out.println("\nTemps total d'execution :" +(Ttot-T)+"ms");
    	
    }
}
