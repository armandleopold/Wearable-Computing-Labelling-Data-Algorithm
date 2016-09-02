package ESIG.DTO;

/**
 * Gyroscope local class
 * @author Matthieu Munhoven, Armand Leopold
 * @version 1.2
 */
public class DTOGyroscope {
	private long timestamps;
	private double x;
	private double y;
	private double z;
	
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public long getTimestamps() {
		return timestamps;
	}
	public void setTimestamps(long timestamps) {
		this.timestamps = timestamps;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	public double getZ() {
		return z;
	}
	public void setZ(double z) {
		this.z = z;
	}
	
	public DTOGyroscope(long timestamps, double x, double y, double z){
		this.timestamps=timestamps;
		this.x=x;
		this.y=y;
		this.z=z;
	}

}
