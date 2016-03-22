package hiepnt;

import java.io.Serializable;

public class Coord implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private double x;
	private double y;
	
	public Coord(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}
	
	public double distance(Coord o){
		double dx = x - o.x;
		double dy = y - o.y;
		return Math.sqrt(dx * dx + dy * dy);
	}
	
	public boolean equals(Object obj){
		if (obj == null) {
	        return false;
	    }
		
		final Coord other = (Coord) obj;
		
		if(this.x == other.x && this.y == other.y)
			return true;
		
		return false;
		
	}
	
	public int hashCode(){
		return 0;
		
	}
	
	public String toString(){
		return "(" + x + " , " + y + ")";
	}
	
}
