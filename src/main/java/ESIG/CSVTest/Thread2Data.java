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
 * Thread for inputing data, better than the first version because it include the data type (string/int/long)
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */

public class Thread2Data extends Thread {
	
	public static int nbthread = 0;
	public static int totline=0;
	private File file;
	private String id;
	private Session session;
	private JavaSparkContext sc;
	private int numFile;
	
	public Thread2Data(File file, String id, Session session, JavaSparkContext sc, int numFile){
		this.file=file;
		this.id=id;
		this.session=session;
		this.sc=sc;
		this.numFile=numFile;
		nbthread++;
	}
	
	/**
     * create class function
     * 
     * @param id
     * id of the table
     * @param val
     * header of the table
     */
	private void createClass(String id, String val){
		try{
			//System.out.println(val);
			this.session.execute("CREATE TABLE wc_2." +id +" (" +val +");");
		} catch (AlreadyExistsException e){
			 //e.printStackTrace();
		}
	}
	
	/**
     * data type function (int, string, long)
     * 
     * @param data
     * string containing the data
     */
	private String dataType(String data){
		String type="text";
		int verif=0;
		try {
			Long.parseLong(data);
			type="bigint";
			verif=1;
		} catch (Exception e) {	
		}
		if(verif==0){
			try {
				Double.parseDouble(data);
				type="double";
			} catch (Exception e) {	
			}
		}
		return type;
	}
	
	/**
     * retry function for query
     * 
     * @param i 
     * number of iteration
     * @param limit
     * maximum of iterations
     */
	public void retry(int i, int limit){
		BufferedReader br = null;
		String line = "";
		String req="";
		String table ="";
		String val="";
		String[] ligne = null;
		String[] type = null;
		int compteur=0;
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
                String[] data2 = line.split(cvsSplitBy);
                
                if (compteur==0){
					
                	ligne=data;
                	type=data2;												//A RECUPERER DANS UNE NEW CLASS
					compteur=1;
                } else {
                	if(compteur==1){
                		
                		for (int k=0;k<ligne.length;k++){
							if(ligne[k].equals(" timestamps")){
								val=val+ligne[k]+" bigint primary key";
								type[k]=dataType(data[k]);
								verif=1;//					//					//
							} else {
								if(!ligne[k].equals(" ")){								//
									val=val+ligne[k]+" "+dataType(data[k]);
									type[k]=dataType(data[k]);
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
							val=val+", timestamps bigint primary key";
							type[ligne.length+1]=dataType(data[ligne.length+1]);
							//System.out.println(val);
						}
							//System.out.println(val);
							compteur=2;
							this.createClass(id, val);
							for(int j=0;j<data.length;j++){
		                		data[j]=data[j].replace("'", " ");
		                		if(j==data.length-1){
		                			//if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])<1000000000000000000L){
		                			if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])>9999999999999L){
		                				req = req+String.valueOf(Long.parseLong(data[j])/1000000);
		                				//System.out.println(data[j] +" " +dataType(data[j]));
		                			} else {
		                				if (type[j].equals("text")){
		                					req = req+"'"+data[j]+"'";
		                				}
		                				else {
		                					req = req+data[j];
		                				}
			                			//System.out.println(data[j] +" " +dataType(data[j]));
		                			}
			                		table = table+ligne[j];
		                		}else{
		                			//if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])<1000000000000000000L){
		                			if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])>9999999999999L){
		                				req = req+String.valueOf(Long.parseLong(data[j])/1000000)+",";
		                				//System.out.println(data[j] +" " +dataType(data[j]));
		                			} else {
		                				if (type[j].equals("text")){
		                					req = req+"'"+data[j]+"',";
		                				}
		                				else {
		                					req = req+data[j]+",";
		                				}
			                			//System.out.println(data[j] +" " +dataType(data[j]));
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
		                	//System.out.println("INSERT INTO demo."+id +" ("+table+")" +" VALUES ("+req+")");
                	} else {
                		for(int j=0;j<data.length;j++){
	                		data[j]=data[j].replace("'", " ");
	                		if(j==data.length-1){
	                			//if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])<1000000000000000000L){
	                			if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])>9999999999999L){
	                				req = req+String.valueOf(Long.parseLong(data[j])/1000000);
	                				//System.out.println(data[j] +" " +dataType(data[j]));
	                			} else {
	                				if (type[j].equals("text")){
	                					req = req+"'"+data[j]+"'";
	                				}
	                				else {
	                					req = req+data[j];
	                				}
		                			//System.out.println(data[j] +" " +dataType(data[j]));
	                			}
		                		table = table+ligne[j];
	                		}else{
	                			//if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])<1000000000000000000L){
	                			if(ligne[j].equals(" timestamps") && Long.parseLong(data[j])>9999999999999L){
	                				req = req+String.valueOf(Long.parseLong(data[j])/1000000)+",";
	                				//System.out.println(data[j] +" " +dataType(data[j]));
	                			} else {
	                				if (type[j].equals("text")){
	                					req = req+"'"+data[j]+"',";
	                				}
	                				else {
	                					req = req+data[j]+",";
	                				}
		                			//System.out.println(data[j] +" " +dataType(data[j]));
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

            }

        } catch (FileNotFoundException e) {
            if (i<limit){
            	i++;
            	retry(i,limit);
            } else {
                e.printStackTrace();	
            }
        } catch (IOException e) {
            if (i<limit){
            	i++;
            	retry(i,limit);
            } else {
                e.printStackTrace();	
            }  
        } catch (NoHostAvailableException e) {
        	if (i==limit-1){
            	int j=0;
            	while(j<1000){
            		j++;
            	}
            }
        	if (i<limit){
            	i++;
            	retry(i,limit);
            } else {
                e.printStackTrace();	
            }
        }
		finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (i>=limit){
				sc.close();
				System.exit(0);
			}
        }
	}
	
	/**
     * run function
     * 
     */
	public void run() {
		long T1 = System.currentTimeMillis();
		int i=0;
		retry(i,100);
		long T2 = System.currentTimeMillis();
		System.out.println("\nTemps d'execution pour le fichier n°" +(numFile+1) +": " +(T2-T1)+"ms");
		nbthread--;
	}
	
}
