package org.codefilarete.jumper.schema.difference;

public class Country {
	
	private final long id;
	
	private String name;
	
	public Country(long id) {
		this.id = id;
	}
	
	public long getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}