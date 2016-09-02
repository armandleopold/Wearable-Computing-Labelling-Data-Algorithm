package ESIG.CSVTest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Thread for input data (no javadoc here)
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.1
 */
public class ThreadData extends Thread {
	
	public static int nbthread = 0;
	public static int totline=0;
	private File file;
	private String id;
	private Session session;
	private JavaSparkContext sc;
	
	public ThreadData(File file, String id,Session session){
		this.file=file;
		this.id=id;
		this.session=session;
	}
	
	private void createClass(String id, String val, int i, int limit){
		try{
			//System.out.println(val);
			this.session.execute("CREATE TABLE wc_2." +id +" (" +val +");");
		} catch (NoHostAvailableException e){
			if (i<limit){
            	i++;
            	createClass(id,val,i,limit);
            } else {
                e.printStackTrace();
                sc.close();
                System.exit(0);
            }
		} catch (AlreadyExistsException e){
			 //e.printStackTrace();
		}
	}
	
	public void run() {
		ThreadData.nbthread++;
		BufferedReader br = null;
		String line = "";
		String req="";
		String table ="";
		String val="";
		String[] ligne = null;
		int compteur=0;
		long T1 = System.currentTimeMillis();
        String cvsSplitBy = ",";
		try {
			int verif=0;
			int test=0;

            br = new BufferedReader(new FileReader(file.getAbsolutePath()));
            while ((line = br.readLine()) != null) {
            	totline++;
                // use comma as separator
            	req="";
            	table="";
                String[] data = line.split(cvsSplitBy);
                
                if (compteur==0){
					
                	ligne=data;
                													//A RECUPERER DANS UNE NEW CLASS
					for (int k=0;k<ligne.length;k++){
							if(ligne[k].equals(" timestamps")){
								val=val+ligne[k]+" text primary key";
								verif=1;//					//					//
							} else {
								if(!ligne[k].equals(" ")){								//
									val=val+ligne[k]+" text";
									test=0;
								} else {
									test=1;
								}
							}//						
							//System.out.println(test);//
							if(k!=ligne.length-1 && test==0){				//
								val=val+",";											//
							} 
							if (test==1) {
								val=val.substring(0,val.length()-1); 	//
							}
					}
					if (verif==0){
						val=val+",timestamps text primary key";
						//System.out.println(val);
					}
					this.createClass(id, val,0,100);
					//System.out.println(val);
					compteur=1;
                } else {
                	for(int j=0;j<data.length;j++){
                		data[j]=data[j].replace("'", " ");
                		if(j==data.length-1){
                			if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])>9999999999999L){
                				req = req+"'"+String.valueOf(Long.parseLong(data[j])/1000000)+"'";
                			} else {
	                			req = req+"'"+data[j]+"'";
                			}
	                		table = table+ligne[j];
                		}else{
                			if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])>9999999999999L){
                				req = req+"'"+String.valueOf(Long.parseLong(data[j])/1000000)+"',";
                			} else {
	                			req = req+"'"+data[j]+"',";
                			}
                    		table = table+ligne[j]+",";
                		}
                		//System.out.print(oneData[j]+" ");
                	}
                	if(verif==0){
                		table=table+",timestamps";
                		req=req+",'"+Long.toString(System.currentTimeMillis())+"'";
                		//System.out.println("INSERT INTO demo."+id +"("+table+")" +" VALUES ("+req+")");
                	}
                	//System.out.println(table);
                	//System.out.println("INSERT INTO demo."+id +"("+table+")" +" VALUES ("+req+")");
                	this.session.execute("INSERT INTO wc_2."+id +"("+table+")" +" VALUES ("+req+")");
                }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoHostAvailableException e) {
            e.printStackTrace();
            sc.close();
            System.exit(0);
        }
		finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
		long T2 = System.currentTimeMillis();
		System.out.println("\nTemps d'execution : " +(T2-T1)+"ms");
		ThreadData.nbthread--;
	}
	
}
