package com.qst.hive211;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {

		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection conn = DriverManager.getConnection("jdbc:hive2://s201:10000/myhive");
			Statement st = conn.createStatement();
			st.execute("create table t3(id int,name string, age int)");
			st.close();
			System.out.println("ok");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testFind() {

		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection conn = DriverManager.getConnection("jdbc:hive2://s201:10000/");
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery("select * from user1");
			while(rs.next()){
				int id = rs.getInt("id");
				String name = rs.getString("name");
				int age = rs.getInt("age");
				String province = rs.getString("province");
				String city = rs.getString("city");
				
				System.out.println("id="+id+";name="+name+";age="+age+";province="+province+";city="+city);
			}
			
			st.close();
			System.out.println("ok");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
