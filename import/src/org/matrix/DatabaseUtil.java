package org.matrix;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;


/**
 * 操作数据库工具类
 * 
 * @author 3zhangwei
 * 
 */
public class DatabaseUtil {

	private static Logger log = Logger.getLogger(DatabaseUtil.class);

	private static final String PROPERTIESFILE = "database.properties";

	private static final String DATABASEFORNAME = "database_forName";
	private static final String DATABASEPASSWORD = "database_password";
	private static final String DATABASEUSERNAME = "database_userName";
	private static final String DATABASEURL = "database_url";
	
	
	/**
	 * 获取conn
	 * 
	 * @return
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static Connection getConnection() throws SQLException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException, IOException {
		Properties pros = getProperty();
		Class.forName(pros.getProperty(DATABASEFORNAME)).newInstance();
		String url = pros.getProperty(DATABASEURL); // orcl为数据库的SID
		String user = pros.getProperty(DATABASEUSERNAME);
		String password = pros.getProperty(DATABASEPASSWORD);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}
	
	/**
	 * 获取配置信息
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Properties getProperty() throws IOException {
		Properties props = new Properties();
		String url = "/"
				+ DatabaseUtil.class.getClassLoader().getResource(
						PROPERTIESFILE).toString().substring(6);
		String empUrl = url.replace("%20", " ");// 如果你的文件路径中包含空格，是必定会报错的
		// log.info(empUrl);
		InputStream in = null;
		in = new BufferedInputStream(new FileInputStream(empUrl));
		props.load(in);
		in.close();
		return props;
	}

	/**
	 * 关闭
	 * 
	 * @param obj
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public static void close(Object obj) throws SecurityException,
			NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		if (obj != null) {
			Method method = obj.getClass().getMethod("close");
			method.setAccessible(true);
			method.invoke(obj);
		}
	}

	/**
	 * 执行一批sql语句
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws SecurityException
	 * @throws IllegalArgumentException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 */
	public static boolean executeUpdate(StringBuffer[] sql)
			throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			SecurityException, IllegalArgumentException, NoSuchMethodException,
			InvocationTargetException {
		int[] upDB = null;
		boolean b = true;
		Connection conn = getConnection();
		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();

		for (int i = 0; i < sql.length; i++) {
			if (sql[i] != null) {
				stmt.addBatch(sql[i].toString());
				log.info("executeUpdate " + sql[i].toString());
			}
		}

		try {
			upDB = stmt.executeBatch();
			// 是否所有sql全部执行成功。如果全部执行成功提交否则回滚
			for (int i = 0; i < upDB.length; i++) {
				if (upDB[i] == 0) {
					b = false;
					conn.rollback();
					return b;
				}
			}

			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			b = false;
		} finally {
			close(stmt);
			close(conn);
		}
		return b;
	}

	/**
	 * 查询一个ID
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws SecurityException
	 */
	public static String getDatabaseId(StringBuffer sql) throws SQLException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException, IOException, SecurityException,
			IllegalArgumentException, NoSuchMethodException,
			InvocationTargetException {
		String str = null;
		Connection conn = getConnection();
		PreparedStatement ps = conn.prepareStatement(sql.toString());

		try {
			// ps.setString(1, "123");
			ResultSet rs = ps.executeQuery();
			log.info("getDatabaseId " + sql.toString());

			while (rs.next()) {
				str = rs.getString(1);
			}

		} catch (Exception e) {
			conn.rollback();
		} finally {
			close(ps);
			close(conn);
		}
		return str;
	}

	/**
	 * 一个sql语句执行多次
	 * 
	 * @param sql
	 * @param str
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws SecurityException
	 * @throws IllegalArgumentException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 */
	public static void executeUpdate(String sql, List<String[]> str,Class ...cls)
			throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException,
			SecurityException, IllegalArgumentException, NoSuchMethodException,
			InvocationTargetException {
		if (sql != null && !"".equals(sql)) {
			Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql);
			
			for (String[] strings : str) {
				
				// 添加参数
				for (int i = 0; i < strings.length; i++) {
					ps.setString(i + 1, strings[i]);
					//log.info(strings[i]);
				}

				try {
					ps.executeUpdate();
					// log.info("executeUpdate " + sql.toString());
				} catch (Exception e) {
					log.info("executeUpdate Exception");
					e.printStackTrace();
				}
			}
			close(ps);
			close(conn);
			log.info("executeUpdate " + sql.toString());
			log.info("Update is size" + str.size());
		}
	}
	
	public static String getDatabaseId(StringBuffer sql, String... orgs) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, SecurityException, IllegalArgumentException, NoSuchMethodException, InvocationTargetException
	{

		String str = null;
		Connection conn = getConnection();
		PreparedStatement ps = conn.prepareStatement(sql.toString());

		try
		{    
			// ps.setString(1, "123");
			 //log.info("getDatabaseId :" + sql.toString());
			for (int i = 0; i < orgs.length; i++)
			{
				if (orgs[i] != null)
				{ //  log.info(orgs[i]);
					ps.setString(i + 1, orgs[i]);
				}
			}
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				str = rs.getObject(1).toString();
				// log.info("DatabaseId is " + str);
			}

		}
		catch (Exception e)
		{
			log.info("getDatabaseId Exception");
			log.info(e.getMessage());
		}
		finally
		{
			close(ps);
			close(conn);
		}
		return str;
	}
	
	/**
	 * 查询数据 结果放在itme里面 注意：cls可以自定义实体类。属性名称和数据库字段名称一样
	 * 
	 * @param sql
	 * @param itme
	 * @param cls
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 * @throws SecurityException
	 * @throws IllegalArgumentException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 */
	public static void executeUpdate(String sql, List itme, Class cls)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, IOException,
			SecurityException, IllegalArgumentException, NoSuchMethodException,
			InvocationTargetException {

		if (sql != null && !"".equals(sql)) {

			if (itme == null) {
				itme = new ArrayList();
			}

			Connection conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			log.info("executeUpdate " + sql.toString());

			try {

				while (rs.next()) {
					Object obj = cls.newInstance();
					// 获取类的所有属性
					Field[] fields = cls.getDeclaredFields();

					// 查询出来的数据全部放到类里面
					for (int i = 0; i < fields.length; i++) {
						fields[i].setAccessible(true);
						fields[i].set(obj, rs.getString(fields[i].getName()));
					}

					itme.add(obj);
				}

			} catch (Exception e) {
				log.info("executeUpdate Exception");
				log.info(e.getMessage());

			} finally {
				close(rs);
				close(ps);
				close(conn);
			}
		}
	}
}
