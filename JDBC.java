
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.cj.util.Util;

public class JDBC {
	private Connection mySQLConexion;
	private Connection mySQLiteConexion;
	private PreparedStatement ps = null; // atributo de instancia

	public static void main(String[] args) throws SQLException {
		JDBC prueJdbc = new JDBC();
		long timeb4;
		long timeAfter;
		String query;

		// prueJdbc.getInfo("add");

		// Ejercicio 1
		// prueJdbc.ejercicio1("a");

		// // Ejercicio 2-1

		// prueJdbc.darDeAltaAlumnos("Paco","Paquito", 180, 21);
		// // Ejercicio 2-2
		// prueJdbc.darDeAltaAsignaturas("ArtesOscuras");

		// // Ejercicio 3-1
		// prueJdbc.darDeBajaAlumno(15);

		// // Ejercicio 3-2
		// prueJdbc.darDeBajaAsignaturas(11);

		// // Ejercicio 4-1
		// prueJdbc.modificarAlumno(1, "Paco","Paquito",185, 20);

		// // Ejercicio 4-2
		// prueJdbc.modificarAula("Cosa", 6);

		// // Ejercicio 5-1
		// prueJdbc.aulasConAlumnos();

		// // Ejercicio 5-2

		// prueJdbc.alumnosAprobados();

		// // Ejercicio 5-3

		// prueJdbc.asignaturaSinAlumnos();

		// // Ejercicio 6

		// prueJdbc.pintarConsultas(prueJdbc.alumnoSinPreparada("a", 180));
		// prueJdbc.pintarConsultas(prueJdbc.alumnoConPreparada("a", 180));

		// // Ejercicio 7
		// timeb4 = (System.currentTimeMillis());
		// for (int i = 0; i < 1; i++) {
		// prueJdbc.alumnoSinPreparada("A", 150);
		// }
		// timeAfter = (System.currentTimeMillis());

		// System.out.printf("Sin ha tardado %d\n", timeAfter - timeb4);

		// timeb4 = System.currentTimeMillis();
		// for (int i = 0; i < 1; i++) {
		// prueJdbc.alumnoConPreparada("A", 150);
		// }
		// timeAfter = System.currentTimeMillis();
		// System.out.printf("Con ha tardado %d\n", timeAfter - timeb4);

		// // Ejercicio 8
		// prueJdbc.añadirColumna("alumnos", "mana", "varchar(20)", "DEFAULT NULL");

		// Ejercicio 9
		// prueJdbc.getInfoBD("add");

		// Ejercicio 10
		// prueJdbc.getDatosFromQuery();
		prueJdbc.abrirConexion("add", "localhost", "root", null);
		prueJdbc.abrirConexion2("SQLiteDataBase.sqlite");

		// prueJdbc.Migration(prueJdbc.getCreationQuery());

		//prueJdbc.OverInsert(32, "AulaSinPatatas", 0);
		prueJdbc.DoubleInsert(30, "Marco", "Polo",190 ,30);


		// Ejercicio 12 //TODO:

		// Alumno[] alumnosBien = {
		// new Alumno("Paco3", "Paquito3", 175, 5),
		// new Alumno("Pepe", "Pepito", 185, 5) };
		// prueJdbc.insertarAlumnos(alumnosBien);

		// Peta por que no existe el aula 100
		// Alumno[] alumnosMal = {
		// new Alumno("Maria", "Perez", 190, 5),
		// new Alumno("Juan","Gimenez", 155, 100)};
		// prueJdbc.insertarAlumnos(alumnosMal);

		// prueJdbc.conexion.setAutoCommit(true);

		// Ejercicio 13a //TODO:

		// prueJdbc.obtenerImagen();

		// Ejercicio 13b //TODO:

		// prueJdbc.guardarImagen(new File("c:\\Users\\Alejandro\\Desktop\\toys2.png"));

		// Ejercicio 15

		// prueJdbc.getAulasAndSum();

		// prueJdbc.buscaCad("add", "a");

		prueJdbc.cerrarConexion();

		// prueJdbc.abrirConexion2("hola3");

	}

	public ArrayList<String> getCreationQuery() {
		DatabaseMetaData dbmt;
		ResultSet tablas;
		Statement sta = null;
		String cadenaAModificar = "";
		ArrayList<String> querys = new ArrayList<>();

		try {
			dbmt = this.mySQLConexion.getMetaData();
			tablas = dbmt.getTables("add", null, null, null);

			while (tablas.next()) {
				sta = this.mySQLConexion.createStatement();

				ResultSet result = sta
						.executeQuery(String.format("SHOW CREATE TABLE %s", tablas.getString("TABLE_NAME")));
				result.next();
				cadenaAModificar = result.getString(2);
				cadenaAModificar = cadenaAModificar.replace("KEY `aula` (`aula`),", "");
				cadenaAModificar = cadenaAModificar.replace("AUTO_INCREMENT", "");
				cadenaAModificar = cadenaAModificar
						.replace("ENGINE=InnoDB =30 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci", "");
				cadenaAModificar = cadenaAModificar
						.replace("ENGINE=InnoDB =12 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci", " ");
				cadenaAModificar = cadenaAModificar
						.replace("ENGINE=InnoDB =32 DEFAULT CHARSET=utf8 COLLATE=utf8_spanish_ci", " ");
				cadenaAModificar = cadenaAModificar
						.replace("ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci", " ");
				cadenaAModificar = cadenaAModificar.replace("KEY `alumno` (`alumno`),", "");
				cadenaAModificar = cadenaAModificar.replace("KEY `asignatura` (`asignatura`),", "");
				cadenaAModificar = cadenaAModificar.replace("DEFAULT current_timestamp()", "");
				cadenaAModificar = cadenaAModificar.replace("DEFINER=`root`@`localhost` SQL SECURITY DEFINER", "");
				cadenaAModificar = cadenaAModificar.replaceAll("ALGORITHM\\s*=\\s*\\w+", "");
				System.out.println(cadenaAModificar);
				querys.add(cadenaAModificar);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return querys;
	}

	public void Migration(ArrayList<String> querys) throws SQLException {
		for (int i = 0; i < querys.size(); i++) {
			this.mySQLiteConexion.createStatement().executeUpdate(querys.get(i));
		}
	}

	// Ejercicio 2 sqlite

	// En la consola, una vez creada la base de datos usamos el comando .read que
	// dara error en la funcion de suma, tmb si ya estan definidas las tablas como
	// en nuestro caso por el ejercicio 1 dara error en las tablas ya creadas pero
	// insertara los datos sin problemas

	// Ejercicio 3 sqlite

	// select * from aulas order by puestos desc limit 1,2;

	// Ejercicio 4 sqlite

	public void MinPuestoSin(int numMin) {
		try {
			Statement sta = this.mySQLiteConexion.createStatement();
			ResultSet result = sta.executeQuery(String.format("SELECT * FROM aulas where puestos >=" + numMin));
			while (result.next()) {
				System.out.printf("%4d|%12s|%4d\n", result.getInt("numero"), result.getString("nombreAula"),
						result.getInt("puestos"));
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public void MinPuestoCon(int numMin) {
		PreparedStatement ps = null;
		String query = "SELECT * FROM aulas where puestos >= ?";
		try {
			if (this.ps == null) {
				this.ps = this.mySQLiteConexion.prepareStatement(query);
			}

			this.ps.setInt(1, numMin);

			ResultSet rs = this.ps.executeQuery();
			while (rs.next()) {
				System.out.printf("%4d|%12s|%4d\n", rs.getInt("numero"), rs.getString("nombreAula"),
						rs.getInt("puestos"));
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}


	// Ejercicio 5 sqlite

	public void InsertarDatosAulas(int numero, String nombreAula, int puestos) {
		try {
			Statement sta = this.mySQLiteConexion.createStatement();
			sta.executeUpdate(String.format("INSERT INTO aulas (numero, nombreAula, puestos) VALUES (%d,'%s',%d);", numero, nombreAula, puestos));
			System.out.println("Se ha insertado correctamente");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	// Ejercicio 6 sqlite

	public void OverInsert(int numero, String nombreAula, int puestos){
		try {
			Statement sta = this.mySQLiteConexion.createStatement();
			sta.executeUpdate(String.format("INSERT OR REPLACE INTO aulas (numero, nombreAula, puestos) VALUES (%d,'%s',%d);", numero, nombreAula, puestos));
			System.out.println("Se ha insertado correctamente");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	// Ejercicio 7 sqlite

	public void DoubleInsert(int codigo, String nombre, String apellidos, int altura, int aula){
		try {
			Statement staSQL = this.mySQLConexion.createStatement();
			Statement staLite = this.mySQLiteConexion.createStatement();
			staSQL.executeUpdate(String.format("INSERT OR REPLACE INTO alumnos (codigo,nombre,apellidos,altura,aula) VALUES (%d,'%s','%s',%d,%d);", codigo, nombre, apellidos, altura, aula));
			staLite.executeUpdate(String.format("INSERT OR REPLACE INTO alumnos (codigo,nombre,apellidos,altura,aula) VALUES (%d,'%s','%s',%d,%d);", codigo, nombre, apellidos, altura, aula));
			System.out.println("Se han insertado correctamente");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}



	public void getInfo(String bd) {
		DatabaseMetaData dbmt;
		ResultSet tablas, columnas;
		try {
			dbmt = this.mySQLConexion.getMetaData();
			tablas = dbmt.getTables(bd, null, null, null);
			while (tablas.next()) {
				System.out.println(
						String.format("%s %s", tablas.getString("TABLE_NAME"), tablas.getString("TABLE_TYPE")));
				columnas = dbmt.getColumns(bd, null, tablas.getString("TABLE_NAME"), null);
				while (columnas.next()) {
					System.out.println(String.format(" %s %s %d %s %s", columnas.getString("COLUMN_NAME"),
							columnas.getString("TYPE_NAME"), columnas.getInt("COLUMN_SIZE"),
							columnas.getString("IS_NULLABLE"), columnas.getString("IS_AUTOINCREMENT")));
				}
			}
		} catch (SQLException e) {
			System.out.println("Error obteniendo datos " + e.getLocalizedMessage());
		}
	}

	public void cerrarConexion() {
		try {
			this.mySQLConexion.close();
			System.out.println("Conexion cerrada");
		} catch (SQLException e) {
			System.out.println("Error al cerrar la conexión: " + e.getLocalizedMessage());
		}
	}

	public void abrirConexion(String bd, String servidor, String usuario, String password) {
		try {
			String url = String.format("jdbc:mysql://%s:3306/%s?useServerPrepStmts=true", servidor, bd);
			this.mySQLConexion = DriverManager.getConnection(url, usuario, password);
			if (this.mySQLConexion != null) {
				System.out.println("Conectado a " + bd + " en " + servidor);
			} else {
				System.out.println("No conectado a " + bd + " en " + servidor);
			}
		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getLocalizedMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("Código error: " + e.getErrorCode());
		}
	}

	public void abrirConexion2(String bd) {
		try {
			String url = "jdbc:sqlite:" + bd;
			this.mySQLiteConexion = DriverManager.getConnection(url);
			if (this.mySQLiteConexion != null) {
				System.out.println("Conectado a " + bd);
			} else {
				System.out.println("No conectado a " + bd);
			}
		} catch (SQLException e) {
			System.out.println("SQLiteException: " + e.getLocalizedMessage());
			System.out.println("SQLiteState: " + e.getSQLState());
			System.out.println("Código error: " + e.getErrorCode());
		}
	}

	// ----------------------------------------------------------------------------------------------------------------------------------//

	public void ejemlpos7() throws SQLException {
		CallableStatement cs = this.mySQLConexion.prepareCall("CALL getAulas(?,?)");
		// Se proporcionan valores de entrada al procedimiento
		cs.setInt(1, 10);
		cs.setString(2, "o");
		ResultSet resultado = cs.executeQuery();
		while (resultado.next()) {
			System.out.println(resultado.getInt(1) + "\t" + resultado.getString("nombreAula") + "\t"
					+ resultado.getInt("puestos"));
		}
	}

	// Ejercicio 12
	public void insertarAlumnos(Alumno[] alumnos) throws SQLException {

		try {
			mySQLConexion.setAutoCommit(false);
			String query = "INSERT INTO alumnos(nombre,apellidos,altura,aula) VALUES (?,?,?,?)";
			if (this.ps == null)
				this.ps = this.mySQLConexion.prepareStatement(query);

			for (Alumno alumno : alumnos) {
				ps.setString(1, alumno.nombre);
				ps.setString(2, alumno.apellidos);
				ps.setInt(3, alumno.altura);
				ps.setInt(4, alumno.aula);
				ps.executeUpdate();
			}
			mySQLConexion.commit();

		} catch (SQLException e) {
			try {
				System.out.println("Error en los datos: " + e.getMessage());
				mySQLConexion.rollback();
			} catch (SQLException ex) {
				System.out.println("No se como hemos llegado aqui: " + ex.getMessage());
			}
		}
	}

	// Ejercicio 13a

	public void obtenerImagen() throws SQLException {
		Statement sta = null;
		sta = this.mySQLConexion.createStatement();
		String query = String.format("SELECT imagen FROM imagenes WHERE nombre='imagen2.png';");
		ResultSet result = sta.executeQuery(query);
		result.next();
		InputStream imagen = result.getBinaryStream(1);
		try (FileOutputStream out = new FileOutputStream(new File("C:\\Users\\Alejandro\\Desktop\\imagen2.png"))) {
			int c;
			while ((c = imagen.read()) != -1) {
				out.write(c);
			}
		} catch (Exception e) {

		}
	}

	// Ejercicio 13b

	public void guardarImagen(File imagen) throws SQLException {

		String query = "INSERT INTO imagenes(nombre,imagen) VALUES (?,?)";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);

		ps.setString(1, imagen.getName());

		try (FileInputStream in = new FileInputStream(imagen)) {
			ps.setBinaryStream(2, in, imagen.length());
			ps.executeUpdate();
		} catch (Exception e) {

		}
	}

	// Ejercicio 15

	public void getAulasAndSum() throws SQLException {
		Statement sta = null;
		sta = this.mySQLConexion.createStatement();
		String query = ("SELECT SUMA();");
		ResultSet result = sta.executeQuery(query);
		System.out.println("SUMA:");
		while (result.next()) {
			System.out.println("La suma es de: " + result.getInt(1));
		}
		System.out.println();
		System.out.println("Getaulas:");
		String query2 = ("CALL getaulas(32,'a');");
		ResultSet result2 = sta.executeQuery(query2);
		while (result2.next()) {
			System.out.println(String.format("%d %s %d", result2.getInt(1), result2.getString(2), result2.getInt(3)));
		}
	}

	// Ejercicio 16

	public void buscaCad(String bd, String cad) throws SQLException {
		abrirConexion(bd, "localhost", "root", "");
		Statement sta = mySQLConexion.createStatement();
		ResultSet tablas = mySQLConexion.getMetaData().getTables(bd, null, null, new String[] { "TABLE" });
		while (tablas.next()) {
			ResultSet columnas = mySQLConexion.getMetaData().getColumns(bd, null, tablas.getString("TABLE_NAME"), null);
			while (columnas.next()) {
				if (columnas.getString("TYPE_NAME").equals("VARCHAR")
						|| columnas.getString("TYPE_NAME").equals("CHAR")) {
					String query = String.format("SELECT %s FROM %s WHERE %s LIKE '%s';",
							columnas.getString("COLUMN_NAME"), columnas.getString("TABLE_NAME"),
							columnas.getString("COLUMN_NAME"), "%" + cad + "%");
					ResultSet result = sta.executeQuery(query);
					while (result.next()) {
						System.out.println(String.format("%10s%15s%15s%30s", bd, tablas.getString("TABLE_NAME"),
								columnas.getString("COLUMN_NAME"), result.getString(1)));
					}
				}
			}
		}
	}

	// Ejercicio 1
	public void ejercicio1(String cadena) throws SQLException {
		abrirConexion("add", "localhost", "root", null);

		int cont = 0;
		String query = "SELECT * FROM alumnos WHERE nombre LIKE ?";

		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);

		ps.setString(1, "%" + cadena + "%");

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			System.out.println(rs.getString("nombre"));
			cont++;
		}
		System.out.println("Hay " + cont + " coincidencias");
	}

	// Ejercicio 2

	public void darDeAltaAlumnos(String nombre, String apellidos, int altura, int aula) throws SQLException {

		String query = "INSERT INTO alumnos VALUES (Default, ? , ? , ?, ?, Default)";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setString(1, nombre);
		ps.setString(2, apellidos);
		ps.setInt(3, altura);
		ps.setInt(4, aula);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);

	}

	public void darDeAltaAsignaturas(String asignatura) throws SQLException {

		String query = "INSERT INTO asignaturas VALUES (Default, ?)";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setString(1, asignatura);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);
	}

	// Ejercicio 3

	public void darDeBajaAlumno(int cod) throws SQLException {
		String query = "DELETE FROM alumnos WHERE codigo = ?";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setInt(1, cod);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);
	}

	public void darDeBajaAsignaturas(int cod) throws SQLException {
		String query = "DELETE FROM asignaturas WHERE COD = ?";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setInt(1, cod);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);
	}

	// Ejercicio 4

	public void modificarAlumno(int codigo, String nombre, String apellido, int altura, int aula) throws SQLException {
		String query = "Update alumnos set nombre = ?, apellidos = ?, altura = ?, aula = ?  where codigo = ?";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setString(1, nombre);
		ps.setString(2, apellido);
		ps.setInt(3, altura);
		ps.setInt(4, aula);
		ps.setInt(5, codigo);
		ps.executeUpdate();
	}

	public void modificarAula(String nombre, int codigo) throws SQLException {
		String query = "Update asignaturas set nombre = ?  where COD = ?";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setString(1, nombre);
		ps.setInt(2, codigo);
		ps.executeUpdate();
	}

	// Ejercicio 5

	public void aulasConAlumnos() throws SQLException {
		int cont = 0;

		String query = "SELECT DISTINCT numero FROM aulas JOIN alumnos ON alumnos.aula = aulas.numero";

		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			System.out.println(rs.getString("numero"));
			cont++;
		}
		System.out.println("Hay " + cont + " coincidencias");
	}

	public void alumnosAprobados() throws SQLException { // TODO: dejar bonito
		int cont = 0;

		String query = "SELECT asignaturas.NOMBRE,alumnos.nombre, notas.NOTA FROM asignaturas JOIN notas ON notas.asignatura = asignaturas.COD JOIN alumnos on alumnos.codigo = notas.alumno where notas.NOTA >=5";

		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			System.out.print(rs.getString("asignaturas.NOMBRE") + "\t");
			System.out.print(rs.getString("alumnos.nombre") + "\t");
			System.out.println(rs.getString("notas.NOTA"));
			cont++;
		}
		System.out.println("Hay " + cont + " coincidencias");
	}

	public void asignaturaSinAlumnos() throws SQLException {
		int cont = 0;
		String query = "SELECT nombre from asignaturas where cod not IN(SELECT DISTINCT asignatura FROM notas)";

		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			System.out.println(rs.getString("nombre"));
			cont++;
		}
		System.out.println("Hay " + cont + " coincidencias");
	}

	// Ejercicio 6

	public ResultSet alumnoConPreparada(String patron, int altura) throws SQLException {
		String query = "SELECT nombre from alumnos where nombre LIKE ? and altura > ?";

		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);

		ps.setString(1, "%" + patron + "%");
		ps.setInt(2, altura);

		ResultSet rs = ps.executeQuery();
		return rs;
	}

	public ResultSet alumnoSinPreparada(String patron, int altura) throws SQLException {
		String query = String.format("SELECT nombre from alumnos where nombre LIKE %s and altura > %d",
				"\"%" + patron + "%\"", altura);

		Statement st = mySQLConexion.createStatement();
		ResultSet rs = st.executeQuery(query);
		return rs;
	}

	public void pintarConsultas(ResultSet rs) throws SQLException {
		int cont = 0;
		while (rs.next()) {
			System.out.println(rs.getString("nombre"));
			cont++;
		}
		System.out.println("Hay " + cont + " coincidencias");
	}

	// Ejercicio 8
	public void añadirColumna(String tabla, String campo, String tipoDeDato, String propiedades) {
		Statement sta = null;
		try {
			sta = this.mySQLConexion.createStatement();
			String query = String.format("ALTER TABLE %s ADD %s %s %s", tabla, campo, tipoDeDato, propiedades);

			int filasAfectadas = sta.executeUpdate(query);
			System.out.println("Filas afectadas: " + filasAfectadas);
		} catch (SQLException e) {
			System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
		} finally {
			if (sta != null) {
				try {
					sta.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Ejercicio 9

	public void getInfoBD(String bd) {
		DatabaseMetaData dbmt;
		ResultSet nombresBd, tablas, vistas, procedimienntos, columnas;
		try {

			System.out.println("A");
			System.out.println("------------------------------------------------------------------------");
			dbmt = this.mySQLConexion.getMetaData();
			System.out.println("El nombre del driver es: " + dbmt.getDriverName());
			System.out.println("El nombre de la version es: " + dbmt.getDriverMajorVersion());
			System.out.println("El nombre de la url es: " + dbmt.getURL());
			System.out.println("El nombre del usuario es: " + dbmt.getUserName());
			System.out.println("El nombre del SGBD: " + dbmt.getDatabaseProductName());
			System.out.println("La version del SGBD: " + dbmt.getDatabaseProductVersion());
			System.out.println("Las palabras reservadas del SGBD: " + dbmt.getSQLKeywords());
			System.out.println();
			System.out.println("B");
			System.out.println("------------------------------------------------------------------------");
			nombresBd = dbmt.getCatalogs();
			while (nombresBd.next()) {
				System.out.println(nombresBd.getString("TABLE_CAT"));
			}
			System.out.println();
			System.out.println("C");
			System.out.println("------------------------------------------------------------------------");
			tablas = dbmt.getTables("add", null, null, null);
			while (tablas.next()) {
				System.out.println(String.format("Nombre de la tabla: %s , tipo de tabla: %s",
						tablas.getString("TABLE_NAME"), tablas.getString("TABLE_TYPE")));
			}
			System.out.println();
			System.out.println("D");
			System.out.println("------------------------------------------------------------------------");
			vistas = dbmt.getTables("add", null, null, new String[] { "VIEW" });
			while (vistas.next()) {
				System.out.println(String.format("Nombre de la vista: %s , tipo de tabla: %s",
						vistas.getString("TABLE_NAME"), vistas.getString("TABLE_TYPE")));
			}
			System.out.println();
			System.out.println("E");
			System.out.println("------------------------------------------------------------------------");
			nombresBd = dbmt.getCatalogs();
			while (nombresBd.next()) {
				String tabla = nombresBd.getString("TABLE_CAT");
				System.out.println(tabla);
				tablas = dbmt.getTables(tabla, null, null, null);
				while (tablas.next()) {
					System.out.println(String.format("Nombre de la tabla: %s , tipo de tabla: %s",
							tablas.getString("TABLE_NAME"), tablas.getString("TABLE_TYPE")));
				}
			}
			System.out.println();
			System.out.println("F");
			System.out.println("------------------------------------------------------------------------");
			dbmt = this.mySQLConexion.getMetaData();
			procedimienntos = dbmt.getProcedures("add", null, null);
			while (procedimienntos.next()) {
				System.out.println(String.format("Nombre del procedimiento es : %s ",
						procedimienntos.getString("PROCEDURE_NAME")));
			}
			System.out.println();
			System.out.println("G");
			System.out.println("------------------------------------------------------------------------");

			apartadoG(bd);

			System.out.println();
			System.out.println("H");
			System.out.println("------------------------------------------------------------------------");

			tablas = dbmt.getTables(bd, null, null, new String[] { "TABLE" });
			while (tablas.next()) {
				ResultSet cvPrimarias = dbmt.getPrimaryKeys("add", null, tablas.getString(3));
				ResultSet cvForaneas = dbmt.getExportedKeys("add", null, tablas.getString(3));

				System.out.println(String.format("Nombre de la tabla: %s , tipo de tabla: %s",
						tablas.getString("TABLE_NAME"), tablas.getString("TABLE_TYPE")));

				System.out.print("Claves primarias: ");
				while (cvPrimarias.next()) {
					System.out.println(cvPrimarias.getString("COLUMN_NAME"));
				}
				System.out.print("Claves foranea: ");
				while (cvForaneas.next()) {
					System.out.println(
							cvForaneas.getString("FKCOLUMN_NAME") != null ? cvForaneas.getString("FKCOLUMN_NAME")
									: "No hay");
				}
				System.out.println();
			}

		} catch (

		SQLException e) {
			System.out.println("Error obteniendo datos " + e.getLocalizedMessage());
		}
	}

	// ejercicio 10
	public void getDatosFromQuery() {
		String query = "select *, nombre as non from alumnos";
		try (Statement stmt = mySQLConexion.createStatement(); ResultSet rs = stmt.executeQuery(query)) {

			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();

			for (int i = 1; i <= columnCount; i++) {
				System.out.println("Nombre de la columna: " + rsmd.getColumnName(i));
				System.out.println("Alias de la columna: " + rsmd.getColumnLabel(i));
				System.out.println("Nombre del tipo de dato: " + rsmd.getColumnTypeName(i));
				System.out.println("Es autoincrementado: " + (rsmd.isAutoIncrement(i) ? "Si" : "No"));
				System.out.println("Permite nulos: " + (rsmd.isNullable(i) == 0 ? "No" : "Si"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	// ejercicio 11

	public void apartadoG(String bd) {
		DatabaseMetaData dbmt;
		ResultSet tablas, columnas;

		try {
			dbmt = this.mySQLConexion.getMetaData();
			System.out.println("El nombre de la base de  es: " + dbmt.getDriverName());

			tablas = dbmt.getTables("add", null, "a%", null);

			while (tablas.next()) {
				System.out.println(String.format("Nombre de la tabla: %s", tablas.getString("TABLE_NAME")));

				columnas = dbmt.getColumns("add", null, tablas.getString("TABLE_NAME"), null);

				while (columnas.next()) {

					System.out.println(String.format(
							"Nombre de la columna: %s \nTipo de dato: %s\nTamaño de la columna: %d\nPuede ser nulo? %s\nPuede ser autoincrementada? %s",
							columnas.getString("COLUMN_NAME"), columnas.getString("TYPE_NAME"),
							columnas.getInt("COLUMN_SIZE"), columnas.getString("IS_NULLABLE"),
							columnas.getString("IS_AUTOINCREMENT")));

				}
			}
		} catch (Exception e) {
			e.getMessage();
		}
	}

	// Lo demas

	public void consultaJugadores(String bd) {

		abrirConexion("furbo", "localhost", "root", null);
		try (Statement stmt = this.mySQLConexion.createStatement()) {
			String query = "select * from jugadores_celta";
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				System.out.println(rs.getInt(1) + "\t" + rs.getString("nombre") + "\t" + rs.getInt("goles"));
			}
		} catch (SQLException e) {
			System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
		} finally {
			cerrarConexion();
		}
	}

	public void consultaJugadoresPS(String nombre, int peso) throws SQLException {
		String query = "Update jugadores_celta set peso = ? where nombre = ?";
		if (this.ps == null)
			this.ps = this.mySQLConexion.prepareStatement(query);
		ps.setString(2, nombre);
		ps.setInt(1, peso);
		ps.executeUpdate();
	}

	public void insertarFila() {
		try (Statement sta = this.mySQLConexion.createStatement()) {
			String query = "INSERT INTO jugadores_celta VALUES (10, 'Alex', 'delantero', 24, 'España', 88, 89, 28, 1500)";
			int filasAfectadas = sta.executeUpdate(query);
			System.out.println("Filas insertadas: " + filasAfectadas);
		} catch (SQLException e) {
			System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
		}
	}

	public void addColumna() {
		Statement sta = null;
		try {
			sta = this.mySQLConexion.createStatement();
			String query = "ALTER TABLE jugadores_celta ADD altura VARCHAR(5) DEFAULT NULL, peso VARCHAR(5) DEFAULT NULL";
			int filasAfectadas = sta.executeUpdate(query);
			System.out.println("Filas afectadas: " + filasAfectadas);
		} catch (SQLException e) {
			System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
		} finally {
			if (sta != null) {
				try {
					sta.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

}