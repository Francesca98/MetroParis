package it.polito.tdp.metroparis.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.javadocmd.simplelatlng.LatLng;

import it.polito.tdp.metroparis.model.CoppieFermate;
import it.polito.tdp.metroparis.model.Fermata;
import it.polito.tdp.metroparis.model.Linea;

public class MetroDAO {

	public List<Fermata> getAllFermate() {

		final String sql = "SELECT id_fermata, nome, coordx, coordy FROM fermata ORDER BY nome ASC";
		List<Fermata> fermate = new ArrayList<Fermata>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Fermata f = new Fermata(rs.getInt("id_Fermata"), rs.getString("nome"),
						new LatLng(rs.getDouble("coordx"), rs.getDouble("coordy")));
				fermate.add(f);
			}

			st.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Errore di connessione al Database.");
		}

		return fermate;
	}

	public List<Linea> getAllLinee() {
		final String sql = "SELECT id_linea, nome, velocita, intervallo FROM linea ORDER BY nome ASC";

		List<Linea> linee = new ArrayList<Linea>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Linea f = new Linea(rs.getInt("id_linea"), rs.getString("nome"), rs.getDouble("velocita"),
						rs.getDouble("intervallo"));
				linee.add(f);
			}

			st.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Errore di connessione al Database.");
		}

		return linee;
	}
	
	
	public boolean fermateConnesse(Fermata fp, Fermata fa)
	{
		String sql="SELECT COUNT(*) AS C " + 
				"from connessione " + 
				"where `id_stazA`=? and `id_stazP`=? ";
		try {
		Connection conn= DBConnect.getConnection();
		PreparedStatement st= conn.prepareStatement(sql);
		
		st.setInt(1, fp.getIdFermata());
		st.setInt(2, fa.getIdFermata());
		
		ResultSet res = st.executeQuery();
		res.first(); //vuol dire vai sulla prima riga
		int linee= res.getInt("C");
		st.close();
		conn.close();
		return linee>=1;
	
		} catch(SQLException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	//data la fermata di partenza mi dici quali sono quelle di arrivo
	public List<Fermata> fermateAdiacenti(Fermata fp, Map<Integer, Fermata> map)
	{
		String sql="SELECT DISTINCT `id_stazA` " + 
				"from connessione " + 
				"where `id_stazP`=? ";
		List<Fermata> fermate = new LinkedList<>();
		try {
		Connection conn= DBConnect.getConnection();
		PreparedStatement st= conn.prepareStatement(sql);
		
		st.setInt(1, fp.getIdFermata());
		
		
		ResultSet res = st.executeQuery();
		while(res.next())
		{
			int id= res.getInt("id_stazA");
			Fermata f = map.get(id);
			fermate.add(f);
		}
	conn.close();
	st.close();
	return fermate;
		
	
		} catch(SQLException e)
		{
			e.printStackTrace();
		}
		return fermate;
	}

	public List<CoppieFermate> coppieFermate( Map<Integer, Fermata> map) {
	
		
		String sql = "SELECT DISTINCT `id_stazA` , `id_stazP` " + 
				"from connessione ";
		List<CoppieFermate> result = new LinkedList<CoppieFermate>();
	try {
		
		Connection conn= DBConnect.getConnection();
		PreparedStatement st = conn.prepareStatement(sql);
		ResultSet res = st.executeQuery();
		while(res.next())
		{
			int idA= res.getInt("id_stazA");
			int idP= res.getInt("id_stazP");
			Fermata a= map.get(idA);
			Fermata p= map.get(idP);
		 CoppieFermate c = new CoppieFermate(a, p);
		 result.add(c);
			
		}
		conn.close();
	} catch(SQLException e)
	{
		e.printStackTrace();
	}
		
		
		return result;
	}


}
