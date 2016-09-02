package ESIG.CreateData;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.DecimalFormat;

/**
 * Thread for creating data
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */

public class CreateDataThread extends Thread {
	public static int nbthread = 0;
	private long id;
	private int num;
	
	public CreateDataThread(long id,int num){
		this.id=id;
		this.num=num;
		nbthread++;
	}
	
	
	/**
     * run function
     * 
     */
	public void run() {
		long T1=System.currentTimeMillis();
		DecimalFormat df = new DecimalFormat("#######0.000");
		try{
			PrintStream l_out = new PrintStream(new FileOutputStream("src/main/resources2/"+id+"/0_Accelerometer-"+id+"_"+num+".csv", true));
			l_out.println("sensor_type, device_type, timestamps, x, y, z, ");
			for (long i=0;i<id;i++){
				l_out.println("accelerometer,smartphone,"+System.nanoTime()+","+df.format(Math.random()*10).replace(",", ".")+","+df.format(Math.random()*10).replace(",", ".")+","+df.format(Math.random()*10).replace(",", "."));
			}
			l_out.flush(); 
			l_out.close();
		} catch(FileNotFoundException e){
		}
		long T2=System.currentTimeMillis();
		System.out.println("Execution time for " +id +" data n°"+(num+1)+" : "+(T2-T1)+" ms");
		nbthread--;
	}

}
