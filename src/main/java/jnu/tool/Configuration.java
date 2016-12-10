package jnu.tool;
/*
 *Use Configuration class rather than xml file is because we are building a 
 *data transform tool which mean that the information of database maybe change frequently.
 *It is better to give a setting interface for user. 
 */
public class Configuration {
	private String driver;
	private String url;
	private String user;
	private String password;
/*
 * the default information of my test databases;
 */
	public Configuration() {
		this.driver = "com.mysql.jdbc.Driver";
		this.url = "jdbc:mysql://localhost:3306/test";
		this.user = "root";
		this.password = "123456";
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriver() {
		return driver;
	}

	public String getUrl() {
		return url;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}
}
