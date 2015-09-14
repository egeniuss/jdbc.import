package org.matrix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import opensource.jpinyin.PinyinFormat;
import opensource.jpinyin.PinyinHelper;

public class ImportExp {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Connection con = DatabaseUtil.getConnection();
		PreparedStatement ps = con.prepareStatement("select * from p_user");
		ResultSet rs = ps.executeQuery();
		int i = 0;
		while (rs.next()) {
			i++;
			String realName = rs.getString("real_name");
			String userName = rs.getString("user_name");
			System.out.println(".....code:" + realName);
			PreparedStatement ps1 = con.prepareStatement("update p_dept set attribute2 = ? where user_name = ?");
			String namePinying = PinyinHelper.convertToPinyinString(realName, "", PinyinFormat.WITHOUT_TONE);
			ps1.setString(1, namePinying);
			ps1.setString(2, userName);
			ps1.executeUpdate();
			ps1.close();
			System.out.println("....." + i + "....over!");
		}
		rs.close();
		ps.close();
		con.close();
	}

}
