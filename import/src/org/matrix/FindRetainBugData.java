package org.matrix;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class FindRetainBugData {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		Connection con =DatabaseUtil.getConnection();
		PreparedStatement ps = con.prepareStatement("select a.id,b.code,a.material_id,a.sub_inv_id,a.location_id,a.batch_code from scm_out_store_order_del a left join scm_out_store_order b on a.out_store_order_id = b.id where b.status <> '5' and b.status <> '0' and b.order_type='14' and b.creation_date > to_date('2011-11-11 00:00:00','yyyy-MM-dd hh24:mi:ss')");
		ResultSet rs = ps.executeQuery();
		
		int i=0;
		while (rs.next()){
			i++;
			String did = rs.getString("id");
			String mid = rs.getString("material_id");
			String siid = rs.getString("sub_inv_id");
			String code = rs.getString("code");
			String lo_id =  rs.getString("location_id");
			String bc = rs.getString("batch_code");
			
			String sql = "select id,usable_quantity from scm_inventory_del where material_id = ? and sub_inv_id = ?";
			if(lo_id != null) sql = "select id,usable_quantity from scm_inventory_del where material_id = ? and sub_inv_id = ? and inv_location_id=?";
			if(bc != null) sql = "select id,usable_quantity from scm_inventory_del where material_id = ? and sub_inv_id = ? and inv_location_id=? and batch_code=?";

			PreparedStatement ps1 = con.prepareStatement(sql);
			ps1.setString(1, mid);
			ps1.setString(2, siid);
			if(lo_id != null) ps1.setString(3, lo_id);
			if(bc != null) ps1.setString(4, bc);
			
			ResultSet rs1 = ps1.executeQuery();
			
			if (rs1 != null){
				if(rs1.next()){
					String idid = rs1.getString("id");
					String uq = rs1.getString("usable_quantity");
					
					PreparedStatement ps2 = con.prepareStatement("select * from scm_retained_log sil where sil.retained_bussiness_id=? and sil.inventory_del_id=? and sil.retained_bussiness_code=?");
					ps2.setString(1, did);
					ps2.setString(2, idid);
					ps2.setString(3, code);
					ResultSet rs2 = ps2.executeQuery();
					
					if(rs2 == null || !rs2.next()){
						if(!"0".equals(uq))
							System.out.println("..."+i+"...:"+code+" retain error! id is: "+did);
					}
					
					ps2.close();
					rs2.close();
				}
			}
			ps1.close();
			rs1.close();
			
			if(i%500 == 0)System.out.println("....."+i+"....over!");
			
			//if (i == 1) break;
		}
		
		rs.close();
		ps.close();
		con.close();
	}

}






 