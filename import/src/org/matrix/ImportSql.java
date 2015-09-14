/**
 * 
 */
package org.matrix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * <p>
 * Title: ImportSql.java
 * </p>
 * Description:
 * <p>
 * Modify histoty:
 * 
 * @author LinHua
 * @version 1.0
 * @created 2014-10-20 上午9:57:09
 **/
public class ImportSql {

	public static void main(String[] args) throws Exception {
		List<String> sqlList = SqlFileExecutor.loadSql("C:\\Users\\3wushan\\Desktop\\p_user.sql");
		Connection con = DatabaseUtil.getConnection();
		int errorCount = 0;
		PreparedStatement ps = null;
		for (String sql : sqlList) {
			try {
				ps = con.prepareStatement(sql);
				ps.executeUpdate();
			} catch (Exception e) {
				System.err.println("["+(++errorCount)+"]执行SQL语句错误：" + e.getMessage());
				System.err.println(sql);
			} finally {
				if (ps != null) {
					ps.close();
				}
			}
		}
		con.close();
	}

}
