
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBC {
	private Connection conexion;
	private PreparedStatement ps = null; // atributo de instancia

	public static void main(String[] args) throws SQLException {
		JDBC prueJdbc = new JDBC();
		long timeb4;
		long timeAfter;

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

		
		prueJdbc.abrirConexion("add", "localhost", "root", null);
		
		// Ejercicio 9
		//prueJdbc.getInfoBD("add");

		// Ejercicio 10
		prueJdbc.getDatosFromQuery();

		prueJdbc.cerrarConexion();

	}

	public void getInfo(String bd) {
		DatabaseMetaData dbmt;
		ResultSet tablas, columnas;
		try {
			dbmt = this.conexion.getMetaData();
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

	public void ejemlpos7() throws SQLException {
		CallableStatement cs = this.conexion.prepareCall("CALL getAulas(?,?)");
		// Se proporcionan valores de entrada al procedimiento
		cs.setInt(1, 10);
		cs.setString(2, "o");
		ResultSet resultado = cs.executeQuery();
		while (resultado.next()) {
			System.out.println(resultado.getInt(1) + "\t" + resultado.getString("nombreAula") + "\t"
					+ resultado.getInt("puestos"));
		}
	}

	public void cerrarConexion() {
		try {
			this.conexion.close();
			System.out.println("Conexion cerrada");
		} catch (SQLException e) {
			System.out.println("Error al cerrar la conexión: " + e.getLocalizedMessage());
		}
	}

	public void abrirConexion(String bd, String servidor, String usuario, String password) {
		try {
			String url = String.format("jdbc:mysql://%s:3306/%s?useServerPrepStmts=true", servidor, bd);
			this.conexion = DriverManager.getConnection(url, usuario, password);
			if (this.conexion != null) {
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

	// ----------------------------------------------------------------------------------------------------------------------------------//

	// Ejercicio 1
	public void ejercicio1(String cadena) throws SQLException {
		abrirConexion("add", "localhost", "root", null);

		int cont = 0;
		String query = "SELECT * FROM alumnos WHERE nombre LIKE ?";

		if (this.ps == null)
			this.ps = this.conexion.prepareStatement(query);

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
			this.ps = this.conexion.prepareStatement(query);
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
			this.ps = this.conexion.prepareStatement(query);
		ps.setString(1, asignatura);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);
	}

	// Ejercicio 3

	public void darDeBajaAlumno(int cod) throws SQLException {
		String query = "DELETE FROM alumnos WHERE codigo = ?";
		if (this.ps == null)
			this.ps = this.conexion.prepareStatement(query);
		ps.setInt(1, cod);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);
	}

	public void darDeBajaAsignaturas(int cod) throws SQLException {
		String query = "DELETE FROM asignaturas WHERE COD = ?";
		if (this.ps == null)
			this.ps = this.conexion.prepareStatement(query);
		ps.setInt(1, cod);

		int filasAfectadas = ps.executeUpdate();
		System.out.println("Filas insertadas: " + filasAfectadas);
	}

	// Ejercicio 4

	public void modificarAlumno(int codigo, String nombre, String apellido, int altura, int aula) throws SQLException {
		String query = "Update alumnos set nombre = ?, apellidos = ?, altura = ?, aula = ?  where codigo = ?";
		if (this.ps == null)
			this.ps = this.conexion.prepareStatement(query);
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
			this.ps = this.conexion.prepareStatement(query);
		ps.setString(1, nombre);
		ps.setInt(2, codigo);
		ps.executeUpdate();
	}

	// Ejercicio 5

	public void aulasConAlumnos() throws SQLException {
		int cont = 0;

		String query = "SELECT DISTINCT numero FROM aulas JOIN alumnos ON alumnos.aula = aulas.numero";

		if (this.ps == null)
			this.ps = this.conexion.prepareStatement(query);

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
			this.ps = this.conexion.prepareStatement(query);

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
			this.ps = this.conexion.prepareStatement(query);

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
			this.ps = this.conexion.prepareStatement(query);

		ps.setString(1, "%" + patron + "%");
		ps.setInt(2, altura);

		ResultSet rs = ps.executeQuery();
		return rs;
	}

	public ResultSet alumnoSinPreparada(String patron, int altura) throws SQLException {
		String query = String.format("SELECT nombre from alumnos where nombre LIKE %s and altura > %d",
				"\"%" + patron + "%\"", altura);

		Statement st = conexion.createStatement();
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
			sta = this.conexion.createStatement();
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
			dbmt = this.conexion.getMetaData();
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
			dbmt = this.conexion.getMetaData();
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
							cvForaneas.getString("FK_NAME") != null ? cvForaneas.getString("FK_NAME") : "No hay");
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
        try (Statement stmt = conexion.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

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

	public void apartadoG(String bd) {
		DatabaseMetaData dbmt;
		ResultSet tablas, columnas;

		try {
			dbmt = this.conexion.getMetaData();
			System.out.println("El nombre de la base de  es: " + dbmt.getDriverName());

			tablas = dbmt.getTables("add", null, "a%", null);
			while (tablas.next()) {
				System.out.println(String.format("Nombre de la tabla: %s", tablas.getString("TABLE_NAME")));

				columnas = dbmt.getColumns(bd, null, tablas.getString("TABLE_NAME"), null);
				while (columnas.next()) {
					System.out.println(String.format(
							"Nombre de la columna: %s \nTipo de dato: %s\nTamaño de la columna: %d\nPuede ser nulo? %s\nPuede ser autoincrementada? %s",
							columnas.getString("COLUMN_POSITION"), columnas.getString("TYPE_NAME"),
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
		try (Statement stmt = this.conexion.createStatement()) {
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
			this.ps = this.conexion.prepareStatement(query);
		ps.setString(2, nombre);
		ps.setInt(1, peso);
		ps.executeUpdate();
	}

	public void insertarFila() {
		try (Statement sta = this.conexion.createStatement()) {
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
			sta = this.conexion.createStatement();
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