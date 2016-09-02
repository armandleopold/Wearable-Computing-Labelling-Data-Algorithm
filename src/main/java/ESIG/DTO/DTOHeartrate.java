package ESIG.DTO;

/**
 * Heartrate local class
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */
public class DTOHeartrate implements Comparable<DTOHeartrate>{
	private long timestamps;
	private double rate;
	
	public double getRate() {
		return rate;
	}
	public void setRate(double rate) {
		this.rate = rate;
	}
	public long getTimestamps() {
		return timestamps;
	}
	public void setTimestamps(long timestamps) {
		this.timestamps = timestamps;
	}
	
	public DTOHeartrate (long timestamps,double rate){
		this.timestamps=timestamps;
		this.rate=rate;
	}
	public String toString (){
		return "Timestamps : " +this.getTimestamps() +" | Rate : " +this.getRate();
	}
	public int compareTo(DTOHeartrate o) {
		int resultat=0;
	      if (this.getTimestamps() > o.getTimestamps())
	         resultat = 1;
	      if (this.getTimestamps() < o.getTimestamps())
	         resultat = -1;
	      if (this.getTimestamps() == o.getTimestamps())
	         resultat = 0;
	      return resultat;
	}

}
