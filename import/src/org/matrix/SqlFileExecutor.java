/**
 * 
 */
package org.matrix;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * Title: SqlFileUtil.java
 * </p>
 * Description:
 * <p>
 * Modify histoty:
 * 
 * @author LinHua
 * @version 1.0
 * @created 2014-10-20 上午9:47:52
 **/
public class SqlFileExecutor {

	/**
	 * 读取 SQL 文件，获取 SQL 语句
	 * 
	 * @param sqlFile
	 *            SQL 脚本文件
	 * @return List<sql> 返回所有 SQL 语句的 List
	 * @throws Exception
	 */
	public static List<String> loadSql(String sqlFile) throws Exception {
		List<String> sqlList = new ArrayList<String>();
		try {
			InputStream sqlFileIn = new FileInputStream(sqlFile);
			StringBuffer sqlSb = new StringBuffer();
			byte[] buff = new byte[1024];
			int byteRead = 0;
			while ((byteRead = sqlFileIn.read(buff)) != -1) {
				sqlSb.append(new String(buff, 0, byteRead, "utf-8"));
			}
			// Windows 下换行是 \r\n, Linux 下是 \n
			String[] sqlArr = sqlSb.toString().split("(;\\s*\\r\\n)|(;\\s*\\n)");
			for (int i = 0; i < sqlArr.length; i++) {
				String sql = sqlArr[i].replaceAll("--.*", "").trim();
				if (!sql.equals("")) {
					sqlList.add(sql);
				}
			}
			return sqlList;
		} catch (Exception ex) {
			throw new Exception(ex.getMessage());
		}
	}

	/**
	 * 传入连接来执行 SQL 脚本文件，这样可与其外的数据库操作同处一个事物中
	 * 
	 * @param conn
	 *            传入数据库连接
	 * @param sqlFile
	 *            SQL 脚本文件 可选参数，为空字符串或为null时 默认路径为
	 *            src/test/resources/config/script.sql
	 * @throws Exception
	 */
	public static void execute(Connection conn, String sqlFile) throws Exception {
		Statement stmt = null;
		if (sqlFile == null || "".equals(sqlFile)) {
			return;
		}
		List<String> sqlList = loadSql(sqlFile);
		stmt = conn.createStatement();
		for (String sql : sqlList) {
			stmt.addBatch(sql);
		}
		int[] rows = stmt.executeBatch();
		System.out.println("Row count:" + Arrays.toString(rows));
	}
}
