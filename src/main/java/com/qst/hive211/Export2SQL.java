package com.qst.hive211;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Export2SQL {

	public static void main(String[] args) throws Exception {
		int count = 0;
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/qst","root","root");
		conn.setAutoCommit(false);
		PreparedStatement ppst = conn.prepareStatement("insert into duowan(id,name,pass,email,nickname) values(?,?,?,?,?) ");
		BufferedReader br = new BufferedReader(new FileReader("D:/wc/duowan_user2.txt"));
		String line = null;
		while((line = br.readLine()) != null){
			System.out.println(count++);
			try {
				String[] arr = line.split("\t");
			
				ppst.setInt(1, Integer.parseInt(arr[0]));
				ppst.setString(2, arr.length>0 && arr[1] != null ? arr[1] : null);
				ppst.setString(3, arr.length>0 && arr[2] != null ? arr[2] : null);
				ppst.setString(4, arr.length>0 && arr[3] != null ? arr[3] : null);
				ppst.setString(5, arr.length>0 && arr[4] != null ? arr[4] : null);
				ppst.executeUpdate();
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		conn.commit();
	}
}
