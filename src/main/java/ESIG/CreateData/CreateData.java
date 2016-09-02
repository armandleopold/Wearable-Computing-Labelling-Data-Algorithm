package ESIG.CreateData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * main for creating new data (only create accelerometer data)
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */

public class CreateData {

	public static void main(String[] args) {
		long T=System.currentTimeMillis();
		ExecutorService threadExecutor = Executors.newFixedThreadPool(100);
		long[] tab={1000,10000,100000,1000000,10000000};
		for(long id : tab)
			for(int i=0;i<10000000/id;i++){
				Thread t = new CreateDataThread(id, i);
				threadExecutor.execute(t);
			}
		while(CreateDataThread.nbthread!=0){
    		try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
		long Ttot=System.currentTimeMillis();
		System.out.println("Total execution time : "+(Ttot-T)+" ms");
		System.exit(0);
	}
}
