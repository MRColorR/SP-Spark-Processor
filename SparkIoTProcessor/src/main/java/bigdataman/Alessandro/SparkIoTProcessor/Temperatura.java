package bigdataman.Alessandro.SparkIoTProcessor;

import java.io.Serializable;

public class Temperatura implements Serializable{
	private String id;
	private float avg_temperature;
	private String country;
	
	
	public Temperatura() {}


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}


	public float getAvg_temperature() {
		return avg_temperature;
	}


	public void setAvg_temperature(float avg_temperature) {
		this.avg_temperature = avg_temperature;
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


	public Temperatura(String id, float avg_temperature, String country) {
		super();
		this.id = id;
		this.avg_temperature = avg_temperature;
		this.country = country;
	}
	

	
	
}

