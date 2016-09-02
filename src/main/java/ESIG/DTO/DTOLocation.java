package ESIG.DTO;

/**
 * Location local class
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */
public class DTOLocation implements Comparable<DTOLocation> {
	private long timestamps;
	private double speed;
	
	public long getTimestamps() {
		return timestamps;
	}
	public void setTimestamps(long timestamps) {
		this.timestamps = timestamps;
	}
	public double getSpeed() {
		return speed;
	}
	public void setSpeed(double speed) {
		this.speed = speed;
	}
	
	public DTOLocation(long timestamps,double speed){
		this.timestamps=timestamps;
		this.speed=speed;
	}
	public String toString(){
		return "Timestamps : " +this.getTimestamps() +" | Speed : " +this.getSpeed();
	}
	public int compareTo(DTOLocation o) {
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
